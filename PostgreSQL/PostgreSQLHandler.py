import sys
sys.path.append('..')
from Slack.SlackHandler import SlackHandler
from psycopg2.extras import execute_values
from datetime import datetime
import json
import psycopg2

class PostgreSQLHandler(SlackHandler):
    '''
    purpose               : used for interacting with PostgreSQL
    param host            : server for connecting
    param database        : database for connecting
    param connect_timeout : timeout setting for connection with default = 15 seconds
    param slack_proxy     : proxy setting for slack chosen from ('AWS' / 'GCP' / 'CORP' / 'LOCAL')
    param slack_channel   : slack channel for recording log
     
    
    '''
    def __init__(self, server = 'LDCDataTest', database = 'Data_ETL', connect_timeout = 15, slack_proxy = 'CORP', slack_channel = 'log-test'):
        '''
        param host            : name of server
        param database        : name of database with default = 'Data_ETL'
        param connect_timeout : timeout setting for connection with default = 15 seconds
        param slack_proxy     : proxy setting for slack chosen from 'AWS' / 'GCP' / 'CORP' / 'LOCAL' (use local settnig) with 'CORP' as default
        param slack_channel   : slack channel for recording log with default = 'log-test'
         
        '''
        starting_time = datetime.now()
        
        self.is_connect_succeed = False
        try:
            super().__init__(slack_proxy, slack_channel)
            self.connection=psycopg2.connect(database = database, 
                                            user            = self.key_dict['postgresql'][server]['user'], 
                                            password        = self.key_dict['postgresql'][server]['password'], 
                                            host            = self.key_dict['postgresql'][server]['host'], 
                                            port            = self.key_dict['postgresql'][server]['port'],
                                            connect_timeout = connect_timeout)
            
            self.cur = self.connection.cursor()
            self.is_connect_succeed = True
        
        except Exception as E:
            error_log = str(E)
        
        ending_time = datetime.now()
        
        message = '{} to connect to PostgreSQL at Server: {} with Database: {} using time: {}{}'.format('Succeeded' if self.is_connect_succeed else 'Failed',
                                                                     server, database, ending_time - starting_time, 
                                                                     '' if self.is_connect_succeed else ' due to {}'.format(error_log))
        print(message + '\n\n')
        
        self.PostMessage(self.slack_channel, message, '{}_Log_PostgreSQL'.format('Correct' if self.is_connect_succeed else 'Error'))
    
    def InsertInto(self, data, table, data_type = 'dataframe', schema = 'public', is_neeed_process = True):
        
        '''
        purpose                : insert data into table
        param data             : data for inserting into table 
        param table            : table name of inserted table (case sensitive)
        param data_type        : type of data inserted into table (type chosen from dataframe / list with default = dataframe)
        param schema           : schama name of inserted table with default = 'public' (case sensitive)
        param is_neeed_process : check if need to procees inserted data with default = True
        return is_insert_succeed (boolean)
        '''     
        insert_starting_time = datetime.now()
        is_insert_succeed = False
        
        try:
            # acquire columns of inserted table
            self.cur.execute(''' select column_name from information_schema.columns  where table_schema = '{}' and table_name='{}' order by ordinal_position ASC  '''.format(schema, table))
            columns_list = [item[0] for item in self.cur.fetchall()]
            
            # commit for avoiding idle in transaction
            self.connection.commit()
            
            # process data for inserting
            if is_neeed_process:
                data = self.DataProcess(data, data_type)
            
            # execute batch inserting process
            execute_values(self.cur, '''INSERT INTO {}.{} ({}) VALUES %s '''.format(schema, table, ','.join(['"{}"'.format(col) for col in columns_list])), data)               
            self.connection.commit()
            
            is_insert_succeed = True
        
        except Exception as E:
            self.connection.rollback()
            error_log = str(E)
        insert_ending_time = datetime.now()
        insert_message = '{} to insert data with row number : {} into {}.{} using time: {}{}'.format('Succeeded' if is_insert_succeed else 'Failed', 
                                                                                    len(data), schema, table, 
                                                                                    insert_ending_time - insert_starting_time, 
                                                                                    '' if is_insert_succeed else ' with error log: {}'.format(error_log))
        
        print(insert_message+'\n\n')
        self.PostMessage(self.slack_channel, insert_message, 'Correct_Log_PostgreSQL' if is_insert_succeed else 'Error_Log_PostgreSQL')
        return is_insert_succeed
    
    def Truncate(self, table, schema = 'public'):
        '''
        purpose         : truncate table
        param table     : table for truncating
        param schema    : schama for truncating
        return is_truncate_succeed (boolean)
        '''     
        truncate_starting_time = datetime.now()
        is_truncate_succeed = False
        try:
            # truncate assigned table
            self.cur.execute(''' TRUNCATE {}.{} '''.format(schema, table))
            self.connection.commit()
            is_truncate_succeed = True
        except Exception as E:
            self.connection.rollback()
            error_log = str(E)
        
        truncate_ending_time = datetime.now()
        truncate_message = '{} to truncate {}.{} using time: {}{}'.format('Succeeded' if is_truncate_succeed else 'Failed', 
                                                                                    schema, table, 
                                                                                    truncate_ending_time - truncate_starting_time, 
                                                                                    '' if is_truncate_succeed else ' with error log: {}'.format(error_log))
        
        print(truncate_message+'\n\n')
        self.PostMessage(self.slack_channel, truncate_message, 'Correct_Log_PostgreSQL' if is_truncate_succeed else 'Error_Log_PostgreSQL')
        return is_truncate_succeed
        
    def DataProcess(self, data, data_type):
        '''
        purpose                : process data for inserting into table
        param data             : data for processing
        param data_type        : data type of processed data chosen from (dataframe / list)  
        reutrn                 : nested list
        '''     

        def SingleItemProcesser(item):
            '''
            purpose  : function for dealing with each item
            '''
            import numpy as np
            import pandas as pd

            if type(item) == pd._libs.tslibs.timestamps.Timestamp:
                return str(item)
            
            elif type(item) in [dict, list]:
                return json.dumps(item)
            
            elif (type(item) == float and str(item) == 'nan') or (type(item) == pd._libs.tslibs.nattype.NaTType and str(item) == 'NaT'):
                return None
           
            elif type(item) == np.int64:
                return int(item)     
            
            else: return item
        
        if data_type == 'dataframe':
            unformatted_list = data.values.tolist()
        
        elif data_type == 'list':
            unformatted_list = data
    
        return list(map(lambda lst: list(map(lambda item : SingleItemProcesser(item),lst)),unformatted_list))
   
    def ReadQueryAsDataFrame(self, query):
        '''
        purpose            : read sql query result as DataFrame
        param query        : sql query
        reutrn read_dict   : is_read_succeed => if succeed to read sql query as DataFrame
                             read_df => DataFrame of query result (only exists if is_read_succeed == True)
        '''

        read_starting_time = datetime.now()
        read_dict = {}
        is_read_succeed = False

        try:
            import pandas as pd
            read_df = pd.read_sql_query(query, self.connection)
            self.connection.commit()
            is_read_succeed = True         
            read_dict['read_df'] = read_df
        except Exception as E:
            error_log = str(E)
            self.connection.rollback()
        read_dict['is_read_succeed'] = is_read_succeed
        read_ending_time = datetime.now()
        
        message = '{} to read sql query result as DataFrame{} using time: {}'.format('Succeeded' if is_read_succeed else 'Failed',  
                                                                           '' if is_read_succeed else ' due to {}'.format(error_log), 
                                                                           read_ending_time - read_starting_time)
        print(message +'\n\n')
        
        return read_dict
    
    
    def CloseWork(self):
        '''
        purpose    : close connection to database
        return is_close_succeed (boolean)
        '''
        close_starting_time = datetime.now()
        is_close_succeed = False
        
        try:
            self.connection.close()
            is_close_succeed = True
        
        except Exception as E:
            error_log = str(E)
        
        close_ending_time = datetime.now()
        close_message = '{} to close connection using time: {}{}'.format('Succeeded' if is_close_succeed else 'Failed',  
                                                                                    close_ending_time - close_starting_time, 
                                                                                    '' if is_close_succeed else ' with error log: {}'.format(error_log))
        
        print(close_message+'\n\n')
        self.PostMessage(self.slack_channel, close_message, 'Correct_Log_PostgreSQL' if is_close_succeed else 'Error_Log_PostgreSQL')
        
        return is_close_succeed