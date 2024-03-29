import sys
sys.path.append('..')
from Slack.SlackHandler import SlackHandler
from func_timeout import func_set_timeout,FunctionTimedOut
from datetime import datetime
import json
import mysql.connector

class MySQLHandler(SlackHandler):
    '''
    purpose               : used for interacting with MySQL
    param host            : server for connecting
    param database        : database for connecting
    param connect_timeout : timeout setting for connection with default = 15 seconds
    param slack_proxy     : proxy setting for slack chosen from ('AWS' / 'GCP' / 'CORP' / 'LOCAL')
    param slack_channel   : slack channel for recording log
    param charset         : (optional) selected charset for connection
    param ssl_disabled    : (optional) option for deciding whether to disable SSL/TLS usage or not; if not assigned, the default value is False
    
    '''
    def __init__(self, server = 'dt-stage-wr-corp', connect_timeout = 15, slack_proxy = 'CORP', slack_channel = 'log-test', **kwargs):
        '''
        param host            : name of server
        param connect_timeout : timeout setting for connection with default = 15 seconds
        param slack_proxy     : proxy setting for slack chosen from 'AWS' / 'GCP' / 'CORP' / 'LOCAL' (use local settnig) with 'CORP' as default
        param slack_channel   : slack channel for recording log with default = 'log-test'
        param charset         : (optional) selected charset for connection
        param ssl_disabled    : (optional) option for deciding whether to disable SSL/TLS usage or not; if not assigned, the default value is False
        param database        : (optional) name of database to be connected, if not assgned any, bring default database set in key file. 
                                if don't want to assign database, set database = ''
        '''
        super().__init__(slack_proxy, slack_channel)
        
        # choose database to be connected
        self.database = kwargs['database'] if kwargs.get('database') is not None else self.key_dict['mysql'][server]['default_database']
        
        @func_set_timeout(connect_timeout)
        def ConnectToMySQL():
            
            if kwargs.get('charset'):
                connection = mysql.connector.connect(database    = self.database, 
                                                     user         = self.key_dict['mysql'][server]['user'], 
                                                     password     = self.key_dict['mysql'][server]['password'], 
                                                     host         = self.key_dict['mysql'][server]['host'], 
                                                     port         = self.key_dict['mysql'][server]['port'],
                                                     charset      = kwargs.get('charset'),
                                                     ssl_disabled = kwargs.get('ssl_disabled') if 'ssl_disabled' in kwargs else False)
            else:
                connection = mysql.connector.connect(database    = self.database, 
                                                     user         = self.key_dict['mysql'][server]['user'], 
                                                     password     = self.key_dict['mysql'][server]['password'], 
                                                     host         = self.key_dict['mysql'][server]['host'], 
                                                     port         = self.key_dict['mysql'][server]['port'],
                                                     ssl_disabled = kwargs.get('ssl_disabled') if 'ssl_disabled' in kwargs else False)
            return connection
        
        connect_starting_time = datetime.now()
        self.is_connect_succeed = False

        try:
            self.connection = ConnectToMySQL()
            self.cur = self.connection.cursor()
            self.is_connect_succeed = True

        except FunctionTimedOut as E:
            error_log = str(E)
        
        except Exception as E:
            error_log = str(E)
        
        connect_ending_time = datetime.now()

        message= '{} to connect to MySQL{}{} using time: {}'.format('Succeeded' if self.is_connect_succeed else 'Failed', 
                                                                    '' if not self.is_connect_succeed else ': {} {}'\
                                                                    .format(server, 'with not Database assigned' if self.database == '' else 'at Database: {}'.format(self.database)), 
                                                                    '' if self.is_connect_succeed else ' due to {}'.format(error_log), 
                                                                    connect_ending_time - connect_starting_time
                                                                    ) 
        
        print(message + '\n\n')
        self.PostMessage(self.slack_channel, message, '{}_Log_MySQL'.format('Correct' if self.is_connect_succeed else 'Error'))
        
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
        self.PostMessage(self.slack_channel, message, 'Correct_Log_MySQL' if is_read_succeed else 'Error_Log_MySQL')
        return read_dict
    
    def ReadQueryAsList(self, query):
        '''
        purpose            : read sql query result as List
        param query        : sql query
        param as_dict      : include column information or not with default = False
        reutrn read_dict   : is_read_succeed => if succeed to read sql query as list
                             read_lst => list of query result (type: list, only exists if is_read_succeed == True)
        '''

        read_starting_time = datetime.now()
        read_dict = {}
        is_read_succeed = False

        try:
            self.cur.execute(query)
            read_lst = self.cur.fetchall()
            self.connection.commit()         
            read_dict['read_lst'] = read_lst
            is_read_succeed = True
        except Exception as E:
            error_log = str(E)
            self.connection.rollback()
        
        read_dict['is_read_succeed'] = is_read_succeed
        read_ending_time = datetime.now()
        
        message = '{} to read sql query result as list{} using time: {}'.format('Succeeded' if is_read_succeed else 'Failed',  
                                                                                '' if is_read_succeed else ' due to {}'.format(error_log), 
                                                                                read_ending_time - read_starting_time)
        print(message +'\n\n')
        self.PostMessage(self.slack_channel, message, 'Correct_Log_MySQL' if is_read_succeed else 'Error_Log_MySQL')
        
        return read_dict
    
    def Truncate(self, table, database = 'Data_ETL'):
        '''
        purpose        : truncate table
        param table    : table for truncating
        param database : database for truncating
        return         : is_truncate_succeed (boolean)
        '''     
        truncate_starting_time = datetime.now()
        is_truncate_succeed = False
        try:
            # truncate assigned table
            self.cur.execute(''' TRUNCATE {}.{} '''.format(database, table))
            self.connection.commit()
            is_truncate_succeed = True
        
        except Exception as E:
            self.connection.rollback()
            error_log = str(E)
        
        truncate_ending_time = datetime.now()
        truncate_message = '{} to truncate {}.{} using time: {}{}'.format('Succeeded' if is_truncate_succeed else 'Failed', 
                                                                                    database, table, 
                                                                                    truncate_ending_time - truncate_starting_time, 
                                                                                    '' if is_truncate_succeed else ' with error log: {}'.format(error_log))
        
        print(truncate_message+'\n\n')
        self.PostMessage(self.slack_channel, truncate_message, 'Correct_Log_MySQL' if is_truncate_succeed else 'Error_Log_MySQL')
        return is_truncate_succeed
    
    def DataProcess(self, data, data_type):
        '''
        purpose         : process data for inserting into table
        param data      : data for processing
        param data_type : data type of processed data chosen from (dataframe / list)  
        reutrn          : nested list
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
    
    def InsertInto(self, data, table, data_type = 'lst', batch_row = 5000, is_need_process = False, database = 'Data_ETL'):
        
        '''
        purpose               : insert data into table
        param data            : data for inserting into table 
        param table           : table name of inserted table (case sensitive)
        param data_type       : type of data inserted into table (type chosen from dataframe / list with default = lst)
        param batch_row       : number of rows split for each round of table inserting
        param is_need_process : check if need to procees inserted data with default = False
        param database        : database name of inserted table with default = Data_ETL (case sensitive)
        return                : is_insert_succeed (boolean)
        '''     
        insert_starting_time = datetime.now()
        is_insert_succeed = False
        
        try:
            # acquire columns of inserted table
            self.cur.execute(''' SELECT COLUMN_NAME
                                 FROM INFORMATION_SCHEMA.COLUMNS
                                 WHERE TABLE_NAME = '{}' 
                                 AND TABLE_SCHEMA = '{}' '''.format(table, database))
            
            # column name of table to be inserted into
            columns_list = [item[0] for item in self.cur.fetchall()]
            
            # commit for avoiding idle in transaction
            self.connection.commit()
            
            # process data for inserting into if necessary
            if  is_need_process:
                data = self.DataProcess(data, data_type)
            
            # split all data into several interval wirh row number = assigned batch_row
            interval = list(range(0, len(data), batch_row)) + [len(data)]
            batch_bound = [[start, interval[idx + 1]] for idx, start in enumerate(interval) if idx != len(interval) - 1]
           
            for start, end in batch_bound:
                part_data = data[start : end]
                self.cur.executemany('''INSERT INTO {}.{} ({}) VALUES ({}) '''.format(database, 
                                                                                    table, 
                                                                                    ','.join(['`{}`'.format(col) for col in columns_list]),
                                                                                    ','.join(['%s' for _ in range(len(columns_list))])
                                                                                    ),  part_data)
                self.connection.commit()
                
                print('rows from {} to {} were successfully inserted into {}.{}'.format(start, end - 1, database, table))
            is_insert_succeed = True
        
        except Exception as E:
            self.connection.rollback()
            error_log = str(E)
        insert_ending_time = datetime.now()
        insert_message = '{} to insert data with row number : {} into {}.{} using time: {}{}'.format('Succeeded' if is_insert_succeed else 'Failed', 
                                                                                                     len(data), database, table, 
                                                                                                     insert_ending_time - insert_starting_time, 
                                                                                                     '' if is_insert_succeed else ' with error log: {}'.format(error_log))
        
        print(insert_message+'\n\n')
        self.PostMessage(self.slack_channel, insert_message, 'Correct_Log_MySQL' if is_insert_succeed else 'Error_Log_MySQL')
        return is_insert_succeed
    
    def DeleteFrom(self, query):
        '''
        purpose     : delete data from table
        param query : query for executing deleting action in MySQL
        return      : is_delete_succeed (boolean)
        '''
        
        delete_starting_time = datetime.now()
        is_delete_succeed = False
        
        try:
            self.cur.execute(query)
            self.connection.commit()
            is_delete_succeed = True
        
        except Exception as E:
            self.connection.rollback()
            error_log = str(E)
        
        delete_ending_time = datetime.now()
        
        message = '{} to delete data using time: {}{}'.format('Succeeded' if is_delete_succeed else 'Failed', 
                                                              delete_ending_time - delete_starting_time, 
                                                              '' if is_delete_succeed else ' with error log: {}'.format(error_log)) 
        
        print(message + '\n\n')
        self.PostMessage(self.slack_channel, message, '{}_Log_MySQL'.format('Correct' if is_delete_succeed else 'Error'))  
        
        return is_delete_succeed 
    
    def RetrieveTableInformation(self, **kwargs):
        '''
        purpose        : retrieve table information of assigned database
        param database : (optional) database assigned for retrieving information, if not assigned, use self.database as default
        return         : return dict including 1. (required) is_process_succeed (boolean) 2. (optional) return_data (dictionary) exists only if is_process_succeed == True
        '''
        starting_time = datetime.now()
        is_process_succeed = False
        return_dict = {}
        try:
            
            # if not assigned, use self.database as default
            database = kwargs['database'] if kwargs.get('database') is not None else self.database
            
            # if database == '', retrieve table information of all Databases
            query = ''' SELECT TABLE_SCHEMA, TABLE_NAME, COLUMN_NAME, DATA_TYPE FROM information_schema.columns{} '''.format(' WHERE TABLE_SCHEMA = "{}"'.format(database) if database != '' else '')
            # acquire columns of inserted table
            self.cur.execute(query)
            
            # column name of table to be inserted into
            raw_data_lst = self.cur.fetchall()
            self.connection.commit()
            return_data = dict()

            for db, tb, col, data_type in raw_data_lst:
                
                if db not in return_data:
                    
                    return_data[db] = {}
                
                if tb not in return_data[db]:
                    return_data[db][tb] = {}
                
                return_data[db][tb][col] = data_type
                        
            return_dict['return_data'] = return_data
            is_process_succeed = True
        
        except Exception as E:
            self.connection.rollback()
            error_log = str(E)

        return_dict['is_process_succeed'] = is_process_succeed
        ending_time = datetime.now()

        message = '{} to retrieve table information of {} using time: {}{}'.format('Succeeded' if is_process_succeed else 'Failed', 
                                                                                   'Database: {}'.format(database) if database != '' else 'all Databases',
                                                                                   ending_time - starting_time, 
                                                                                   '' if is_process_succeed else ' with error log: {}'.format(error_log))
        
        print(message+'\n\n')
        self.PostMessage(self.slack_channel, message, 'Correct_Log_MySQL' if is_process_succeed else 'Error_Log_MySQL') 

        return return_dict
            
    def CloseWork(self):
        '''
        purpose    : close connection to database
        return     : is_close_succeed (boolean)
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
        self.PostMessage(self.slack_channel, close_message, 'Correct_Log_MySQL' if is_close_succeed else 'Error_Log_MySQL')
        
        return is_close_succeed