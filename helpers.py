# Standard Libraries
import requests
import csv
import os
import glob
import zipfile
import io
import math
import pickle
import yaml
import sys
from datetime import datetime as dt
from dateutil.relativedelta import relativedelta

# Third Party Libraries
import pandas as pd
import MySQLdb

class Database(object):
    def __init__(self, db_local):
        self.db_local = db_local
        self.db_conn = None
        self.db_cursor = None

    def __enter__(self):
        # This ensure, whenever an object is created using "with"
        # this magic method is called, where you can create the connection.
        self.db_conn = MySQLdb.connect(**self.db_local)
        self.db_cursor = self.db_conn.cursor()

    def __exit__(self, exception_type, exception_val, trace):
        # once the with block is over, the __exit__ method would be called
        # with that, you close the connnection
        try:
           self.db_cursor.close()
           self.db_conn.close()
        except AttributeError: # isn't closable
           print('Not closable.')
           return True # exception handled successfully

    def get_row(self, sql, data = None):
        self.db_cursor.execute(sql)
        self.resultset = self.db_cursor.fetchall()
        return self.resultset

# NOTE: Logging has been changed to print statements specifically for sharing on Github.

def load_yaml_config(filename: str="config.yaml") -> dict:
    with open(filename, 'r') as stream:
        config = yaml.load(stream)
    return config

        
def utc_to_local(x, timezone) -> pd.Timestamp:
    '''
    :param timestamp object (pandas or datetime.datetime):
    :param timezone (pandas accepted timezone eg. Us/Pacific):
    :return: timestamp object
    '''
    return pd.Timestamp(x).tz_localize('utc').tz_convert(timezone).replace(tzinfo=None)


def timestamp_string(ts, formatstr='%Y-%m-%dT%H:%M') -> pd.Timestamp:
    '''
    :param ts: utc timestamp string
    :param formatstr: Converts a UTC YYYY-MM-DD or other into local time
    :return: timestamp
    '''
    try:
        return utc_to_local(pd.to_datetime(ts, errors='coerce', format=formatstr), 'US/Pacific')
    except Exception as e:
        print(e); print(ts)
        return None


def determine_timestamps(df: pd.DataFrame, start: pd.Timestamp, end: pd.Timestamp) -> (pd.Timestamp, pd.Timestamp):
    start = start.date()
    end = end.date()
    data_start = df.index.min()
    data_end = df.index.max()
    if pd.Timestamp(start).date() < pd.Timestamp(data_start).date():
        print(start)
        print(data_start)
    else:
        start = pd.Timestamp(data_end).date() + relativedelta(days=1)
    return start, end


def daily_peaks(df: pd.DataFrame, col: bool=None, hb: int=7, he: int=22) -> pd.DataFrame:
    if col is None:
        col = df.columns[0]
    start = df.index.min().date()
    end = df.index.max().date()
    df['Hour'] = [x.hour for x in df.index]
    results = pd.DataFrame()

    while start <= end:
        subdf = pd.DataFrame(df[col].loc[(df.index >= start) & (df.index < end)])
        onp = subdf.loc[(subdf['Hour'] >= hb) & (subdf['Hour'] <= he)]
        offp = subdf.loc[(subdf['Hour'] < hb) | (subdf['Hour'] > he)]
        results.append(pd.DataFrame([[onp.mean(), offp.mean()]], index=[start]))
        start = start + relativedelta(days=1)

    results.columns = [col + pktype for pktype in ['On Peak', 'Off Peak']]
    return results


def split_dates_by_limit(start_date: str, end_date: str, limit: int, date_format: str="%m/%d/%y") -> dict:
    '''

    APIs commonly have date limits for historical data. This function takes a desired start and end date, and returns
    a dictionary of start and end dates that are generated based on the limit of the API.

    :param start_date: str
    :param end_date: str
    :param limit: int
    :param date_format: str
    :return: dictionary if splits, or None if no split required.

    For example, YES API has a 90 day period limit when calling their API. CAISO OASIS has a 30 day period.

    For a YES call, an example call would look like:
    split_dates_by_limit(start_date='01/01/17', end_date='12/31/18', limit=90)
    expected return:
    {'start_dates': ['01/01/17', '04/01/17', '06/30/17', '09/28/17', '12/27/17', '03/27/18', '06/25/18', '09/23/18',
    '12/22/18'],
    'end_dates': ['03/31/17', '06/29/17', '09/27/17', '12/26/17', '03/26/18', '06/24/18', '09/22/18', '12/21/18',
    '12/31/18']}
    '''

    # Convert limit to pd.Timedelta object for easy comparison
    limit = pd.Timedelta(days=limit)

    # Convert str date to given date_format and calculate delta between start and end date
    dt_start_date = pd.to_datetime(start_date, format=date_format)
    dt_end_date = pd.to_datetime(end_date, format=date_format)
    dt_delta = dt_end_date - dt_start_date

    # if the delta is less than limit, return 1 to indicate only 1 call is needed to API
    if limit > dt_delta:
        return None
    else:
        # Else, set up necessary split start and end dates
        new_start_date = dt_start_date + limit
        new_end_date = dt_start_date + limit - pd.Timedelta(1)

        # Lists as containers for new start and end dates to iterate through, initializing with original start_date and
        # end date is equal to original start date + limit
        start_dates = [dt_start_date]
        end_dates = [new_end_date]

        # Number of calls needed by dividing the delta over given limit, and rounding up
        splits = math.ceil(dt_delta / limit)

        # Number of start dates must be equal to number of splits
        try:
            while len(start_dates) < splits:
                # Append new start date, then add limit for next iteration
                start_dates.append(new_start_date)
                new_start_date += limit

                # if calculated end date is less than original end date & original end date - calc end date > limit, we
                # keep appending to end dates list.
                if new_end_date < dt_end_date and (dt_end_date - new_end_date > limit):
                    end_dates.append(new_end_date + limit)
                    new_end_date += limit
                # Else the last end date appended will be last calced end date + remaining delta
                else:
                    end_dates.append(new_end_date + (dt_end_date - new_end_date))
        except Exception as e:
            print('Critical', 'Failed to split start and end dates for API calls.')

    # List comprehension to convert datetime objects back to string types within same list containers, then return lists
    start_dates = [x.strftime(date_format) for x in start_dates]
    end_dates = [x.strftime(date_format) for x in end_dates]

    # Return a dictionary of start_dates and end_dates list
    return {'start_dates': start_dates, 'end_dates': end_dates}


def read_pickle(name: str, sub_folder: str='pickles'):
    if sub_folder is not None:
        with open(f'{sub_folder}/{name}.pkl', 'rb') as file:
            df = pickle.load(file)
    else:
        with open(f'{name}.pkl', 'rb') as file:
            df = pickle.load(file)
    return df


def write_pickle(df: pd.DataFrame, name: str, sub_folder: str='pickles'):
    if sub_folder is not None:
        mkdir(sub_folder)
        with open(f'{sub_folder}/{name}.pkl', 'wb') as output_file:
            pickle.dump(df, output_file)
        return f'Wrote {sub_folder}/{name}.pkl'
    else:
        with open(f'{name}.pkl', 'wb') as output_file:
            pickle.dump(df, output_file)
        return f'Wrote {name}.pkl'


def mkdir(directory):
    if not os.path.exists(directory):
        os.makedirs(directory)


def rmdir(directory):
    if os.path.exists(directory):
        try:
            os.removedirs(directory)
        except Exception as e:
            print(e)


def get_latest_csv(filepath: str, delete=False) -> str:
    list_of_files = glob.glob('%s/*.csv' % filepath)
    try:
        latest_file = max(list_of_files, key=os.path.getctime)
        df = pd.read_csv(latest_file)
        if delete:
            try:
                os.remove(latest_file)
            except Exception as e:
                print(e)
    except ValueError as e:
        print(e)
        if e == 'max() arg is an empty sequence':
            return print('no csv found')
    return df


def merge_dataframes(df1: pd.DataFrame, df2: pd.DataFrame) -> pd.DataFrame:
    if df1.empty:
        result = df2
    elif df2.empty:
        result = df1
    else:
        result = pd.merge(df1, df2, how='outer', left_index=True, right_index=True)
    return result

def add_days_to_date(date: str, add: int, format: str="%Y%m%d") -> str:
    dt_date = pd.Timestamp(date)
    dt_date += pd.Timedelta(days=add)
    new_str_date = dt.strftime(dt_date, format)
    return new_str_date

class Web(object):
    def __init__(self, url=None, payload=None):
        self.session = requests.Session()
        try:
            # Proxy obfuscated for security reasons; Normally stored in ecrypted configuration file.
            self.proxy = {"http": "placeholder",
                          "https": "placeholder"}
            self.session.proxies.update(self.proxy)
        except Exception as e:
            print('Backup proxy invoked from pullback')
            #TODO find a better backup
            # self.proxy = {"http": backup_http_proxy,
            #               "https": backup_https_proxy}

        if url is not None:
            self.url = url
            if payload is not None:
                try:
                    self.payload = payload
                    self.r = self.session.get(url, params=self.payload)
                except Exception as e:
                    print(e)
            else:
                self.r = self.session.get(url)

    def unzip(self, sub_folder_name=None):

        mkdir("unzipped")
        self.sub_folder_name = sub_folder_name
        if self.sub_folder_name is not None:
            try:
                mkdir(f"unzipped/{self.sub_folder_name}")
                with zipfile.ZipFile(io.BytesIO(self.r.content), "r") as zip_ref:
                    zip_ref.extractall("unzipped/%s" % self.sub_folder_name)
            except Exception as e:
                print(e)
        else:
            try:
                with zipfile.ZipFile(io.BytesIO(self.r.content), "r") as zip_ref:
                    zip_ref.extractall("unzipped")
            except Exception as e:
                print(e)

    def get_text(self):
        return self.r.text

    def get_content(self):
        return self.r.content

    def get_json(self):
        return self.r.json()
