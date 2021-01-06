# Standard Library
import time
import pickle

# Third Party Libraries
import pandas as pd
import sqlalchemy

# Local libraries
import helpers
from logging import log
#functions.load_configuration(description=True)
log.log.name = 'Oasis'

transmission_lines = ['NOB_ITC', 'ADLANTOVICTVL-SP_ITC', 'ELDORADO_ITC', 'MALIN500', 'MCCLMKTPC_ITC', 'MEAD_ITC',
                      'PATH15_BG', 'PALOVRDE_ITC', 'PATH26_BG', 'VICTVL_ITC']


class TransmissionData:

    def __init__(self, ti_id: str, start_date: str, end_date: str, ti_direction: str = 'ALL', sub_folder: str = None):
        self.ti_id = ti_id
        self.start_date = start_date
        self.end_date = end_date
        self.ti_direction = ti_direction
        self.sub_folder = sub_folder

    def get_current_transmission(self, insert=False):
        """

        Downloads a zip file from CAISO OASIS Current Transmission Usage, and unzips file to CSV

        example oasis url: http://oasis.caiso.com/oasisapi/SingleZip?resultformat=6&queryname=TRNS_CURR_USAGE&version=1&
        ti_id=PALOVRDE_ITC&ti_direction=ALL&startdatetime=20181105T07:00-0000&enddatetime=20181106T07:00-0000

        """
        base_url = r'http://oasis.caiso.com/oasisapi/SingleZip?resultformat=6&queryname=TRNS_CURR_USAGE&version=1'
        timestamp_adder = r'T08:00-0000'

        payload = {'ti_id': self.ti_id, 'ti_direction': self.ti_direction, 'startdatetime': self.start_date + timestamp_adder,
                   'enddatetime': self.end_date + timestamp_adder}
        w = helpers.Web(base_url, payload=payload)

        if self.sub_folder is not None:
            try:
                w.unzip(sub_folder_name=self.sub_folder)
                print(f'successful unzip in sub_folder_name check in oasis.get_current_transmission for ti_id: {self.ti_id} self.ti_id')
            except Exception as e:
                print(f'failed unzip in sub_folder_name check in oasis.get_current_transmission for ti_id: {self.ti_id}')
        else:
            try:
                w.unzip()
                print(f'successful unzip in else check in oasis.get_current_transmission for ti_id: {self.ti_id}')
            except Exception as e:
                print('failed unzip in sub_folder_name check in oasis.get_current_transmission for ti_id:'
                          + ' %s' % self.ti_id)

        df = helpers.get_latest_csv('unzipped\%s' % self.sub_folder)
        if insert:
            self.insert_to_mysql(df)

        return df

    @staticmethod
    def remove_transmission_types(df, col='LABEL', condition1='Constraint', condition2='Hourly TTC'):
        '''
        Specific function created to filter to particular transmission types that was needed for use case.

        :param df: pd.DataFrame of data with condition types.
        :param col: str, column name.
        :param condition1: str, first condition.
        :param condition2: str, second condition.
        :return: pd.DataFrame with unnecessary transmission types removed.
        '''

        if condition2 is None:
            df = df[df[col] == condition1]
        else:
            df = df[(df[col] == condition1) | (df[col] == condition2)]
        return df

    @staticmethod
    def insert_to_mysql(df):
        # credentials below are made up; std practice would store encrypted credentials, and pass via reading a config
        local_mysql_server, local_mysql_username, local_mysql_password, local_mysql_database = \
            'localhost', 'root', 'root', 'duck'

        col_data_types = {'OPR_DT': sqlalchemy.types.DATE, 'OPR_HR': sqlalchemy.types.INT,
                          'INTERVALSTARTTIME_GMT': sqlalchemy.types.CHAR(255),
                          'INTERVALENDTIME_GMT': sqlalchemy.types.CHAR(255),
                          'TI_ID': sqlalchemy.types.NVARCHAR(255),
                          'TI_DIRECTION': sqlalchemy.types.NVARCHAR(255),
                          'MARKET_RUN_ID': sqlalchemy.types.NVARCHAR(255),
                          'TI_CONSTRAINT_ID': sqlalchemy.types.NVARCHAR(255),
                          'TR_TYPE': sqlalchemy.types.NVARCHAR(255),
                          'XML_DATA_ITEM': sqlalchemy.types.NVARCHAR(255),
                          'LABEL': sqlalchemy.types.NVARCHAR(255),
                          'POS': sqlalchemy.types.INT, 'OPR_INTERVAL': sqlalchemy.types.INT,
                          'MW': sqlalchemy.types.FLOAT,
                          'MARKET_RUN_ID': sqlalchemy.types.NVARCHAR(255),
                          'GROUP': sqlalchemy.types.INT}

        with helpers.Database('local_mysql') as db:
            table_name = 'db_test_trans'
            try:
                df.to_sql(name=table_name, con=db.engine, if_exists='append', schema=local_mysql_database,
                          dtype=col_data_types)
                print('SQL written to table %s' % table_name)
            except Exception as e:
                print('failed to insert to table %s' % table_name)


class Oasis:
    oasis_url = r'http://oasis.caiso.com/oasisapi/SingleZip?resultformat=6&queryname=%s&version=1'

    def __init__(self, start_date: str, end_date: str):
        self.start_date = start_date
        self.end_date = end_date

    def get(self, call_type: str='Demand'):
        """
        Downloads a zip file from CAISO OASIS System Demand for all or renewable demand, and unzips file to CSV

        example oasis url: http://oasis.caiso.com/oasisapi/SingleZip?resultformat=6&queryname=SLD_REN_FCST&version=1
        &startdatetime=20181101T07:00-0000&enddatetime=20181110T08:00-0000

        Example call:
        x = Oasis("20180101", "20180201").get_system_demand(renewable=True)
        x.get_system_demand() or x.get_system_demand(renewable=True)
        """
        #TODO add more types of calls here other than demand and renewables; add exception handling on invalid types instead of else
        if call_type == 'Renewable':
            base_url = self.oasis_url % 'SLD_REN_FCST'
        else:
            base_url = self.oasis_url % 'SLD_FCST'

        timestamp_adder = r'T08:00-0000'
        dates = helpers.split_dates_by_limit(start_date=self.start_date, end_date=self.end_date, limit=30,
                                             date_format='%Y%m%d')
        df = pd.DataFrame()

        if dates is None:
            payload = {'startdatetime': self.start_date + timestamp_adder,
                       'enddatetime': self.end_date + timestamp_adder}
            print(f'Attempting to call OASIS for {call_type} for {self.start_date}-{self.end_date}. '
                  f'This can take awhile.')
            w = helpers.Web(base_url, payload=payload)

            try:
                w.unzip()
                print('successful unzip in oasis.get_system_demand date range'
                          + ' %s-%s' % (self.start_date, self.end_date))
            except Exception as e:
                print('failed unzip in oasis.get_system_demand for date range'
                          + ' %s-%s' % (self.start_date, self.end_date))

            df = helpers.get_latest_csv('unzipped', delete=True)
        else:
            start_dates = dates.get('start_dates', 'start_dates not found.')
            end_dates = dates.get('end_dates', 'end_dates not found.')

            if isinstance(start_dates, (list,)) and isinstance(end_dates, (list,)):
                for s_date, e_date in zip(start_dates, end_dates):
                    payload = {'startdatetime': s_date + timestamp_adder,
                               'enddatetime': e_date + timestamp_adder}
                    try:
                        print(f'Attempting to call OASIS for {call_type} for {self.start_date}-{self.end_date}.'
                              f'This can take awhile.')
                        w = helpers.Web(base_url, payload=payload)
                        w.unzip()
                        print('successful unzip in oasis.get_system_demand date range'
                              + ' %s-%s' % (s_date, e_date))
                        if len(df) == 0:
                            df = helpers.get_latest_csv('unzipped', delete=True)
                            print( f'Found most recent CSV unzipped from CAISO Oasis for {s_date}-{e_date}.'
                                   f'Creating new DF.')
                        else:
                            temp_df = helpers.get_latest_csv('unzipped', delete=True)
                            df = df.append(temp_df)
                            print( f'successfully appended {s_date} - {e_date} to existing DF.')
                    except Exception as e:
                        print('failed unzip in oasis.get_system_demand for date range'
                              + ' %s-%s' % (s_date, e_date))
        try:
            helpers.rmdir('unzipped')
        except Exception as e:
            print('Unable to remove unzipped directory.')
        return df


# if __name__ == '__main__':
#     ti_id = 'PALOVRDE_ITC'
#     start_date = '20190129'
#     end_date = '20190130'
#     ti_direction = 'ALL'
#     print(TransmissionData(ti_id, start_date, end_date, sub_folder=ti_id).get_current_transmission())
#
#     x = Oasis("20180101", "20180301").get_system_demand(renewable=True)
#     print(x)


#
# while i <= weeks:
#     for x in transmission_lines:
#         a = TransmissionData(x, start_date, end_date, sub_folder=x)
#         a.get_current_transmission()
#         print("%s has attempted to be unzipped" % x)
#         time.sleep(5)
#     start_date = functions.add_days_to_date(start_date, 7)
#     end_date = functions.add_days_to_date(end_date, 7)
#     i += 1

# while i <= weeks:
#     for x in transmission_lines:
#         a = TransmissionData(x, start_date, end_date, sub_folder=x)
#         a.get_current_transmission()
#         print("%s has attempted to be unzipped" % x)
#         time.sleep(5)
#     start_date = functions.add_days_to_date(start_date, 7)
#     end_date = functions.add_days_to_date(end_date, 7)
#     i += 1
