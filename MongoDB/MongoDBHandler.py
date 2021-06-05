import sys
sys.path.append('..')
import pandas as pd
import json
from Slack.SlackHandler import SlackHandler
from datetime import datetime
from pymongo import MongoClient
from func_timeout import func_set_timeout, FunctionTimedOut

class MongoDBHandler(SlackHandler):
    '''
    purpose             : used for interacting with MongoDB
    param host_name     : host for connection
    param timeout       : timeout seconds for MongoDB connection
    param slack_proxy   : proxy setting for slack chosen from ('AWS' / 'GCP' / 'CORP' / 'LOCAL')
    param slack_channel : slack channel for recording log
    '''
    def __init__(self, host_name, timeout = 15, slack_proxy ='CORP', slack_channel = 'log-test'):
        '''
        purpose             : used for interacting with MongoDB
        param host_name     : host for connection
        param timeout       : timeout seconds for MongoDB connection
        param slack_proxy   : proxy setting for slack chosen from 'AWS' / 'GCP' / 'CORP' / 'LOCAL' (use local settnig) with 'CORP' as default
        param slack_channel : slack channel for recording log
        '''      
        # change initial slack proxy setting of SlackHandler      
        super().__init__(slack_proxy, slack_channel)
        
        starting_time = datetime.now()
        
        self.is_connect_succeed = False
        
        # connection information of MongoDB
        connect_info_dict = self.key_dict['mongodb'][host_name]
        
        # set connection timmeout
        @func_set_timeout(timeout)
        def connect_to_mongo():
            
            return MongoClient("mongodb://{}:{}@{}:{}/?authSource={}".format(connect_info_dict['user'], 
                                                                             connect_info_dict['pass'], 
                                                                             connect_info_dict['ip'], 
                                                                             connect_info_dict['port'], 
                                                                             connect_info_dict['auth_db']))
        
        try:          
            self.client = connect_to_mongo()
            
            self.is_connect_succeed = True
        
        except FunctionTimedOut as E:
            error_log = str(E)
            
        except Exception as E:
            error_log = str(E)
            
        ending_time = datetime.now()
        
        message = '{} to connect to MongoDB using time: {}{}'.format('Succeeded' if self.is_connect_succeed else 'Failed',
                                                                     ending_time - starting_time, 
                                                                     '' if self.is_connect_succeed else ' due to {}'.format(error_log))
        print(message + '\n\n')
        
        self.PostMessage(self.slack_channel, message, '{}_Log_MongoDB'.format('Correct' if self.is_connect_succeed else 'Error'))
        
    def RetrieveData(self, db_name, collection_name, by = 'find', condition_setting = None):
        '''
        purpose                     : filter or aggregrate data from collection and return result  as DataFrame format 
        param db_name               : database name
        param collection_name       : collection name
        param by                    : way for retrieving data chosen from find / aggregrate with default = find
        param condition_setting     : setting for retrieving data with default == None which means retrieving all data
        return read_dict            : is_read_succeed (boolean) 
                                      read_df (DataFrame) only exists if is_read_succeed == True
        
        '''
        
        starting_time = datetime.now()
        read_dict = {}
        is_read_succeed = False

        if condition_setting is None:
            condition_setting = {} if by == 'find' else []
     
        try:
            if by == 'find':
                cursor = self.client[db_name][collection_name].find(condition_setting)
            else:
                cursor = self.client[db_name][collection_name].aggregate(condition_setting)
            
            read_dict['read_df'] = pd.DataFrame(cursor)
            cursor.close()
            
            is_read_succeed = True
        
        except Exception as E:
            error_log = str(E)
        
        read_dict['is_read_succeed'] = is_read_succeed
        ending_time = datetime.now()
        
        message = '{} to retrieve data and return result as DataFrame from MongoDB at db: {} and collection: {} using time: {}{}'.format('Succeeded' if is_read_succeed else 'Failed',
                                                                                                                                       db_name, collection_name, ending_time - starting_time, 
                                                                                                                                       '' if is_read_succeed else ' due to {}'.format(error_log))
        print(message + '\n\n')   
        self.PostMessage(self.slack_channel, message, '{}_Log_MongoDB'.format('Correct' if is_read_succeed else 'Error'))
                
        return read_dict
    
    def CloseWork(self):
        '''
        purpose                 : close connection to MongoDB
        return is_close_succeed : boolean
        '''
        starting_time = datetime.now()
        is_close_succeed = False
        
        try:
            self.client.close() 
            is_close_succeed = True
        
        except Exception as E:
            error_log = str(E)
        
        ending_time = datetime.now()
        
        message = '{} to close connection using time: {}{}'.format('Succeeded' if is_close_succeed else 'Failed',  
                                                                                    ending_time - starting_time, 
                                                                                    '' if is_close_succeed else ' with error log: {}'.format(error_log))
        
        print(message + '\n\n')
        
        self.PostMessage(self.slack_channel, message, '{}_Log_MongoDB'.format('Correct' if is_close_succeed else 'Error'))
        
        return is_close_succeed