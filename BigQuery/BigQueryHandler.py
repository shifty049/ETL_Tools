import sys
sys.path.append('..')
import os
import google.auth
from S3.S3Handler import S3Handler
from google.cloud import bigquery
from datetime import datetime,timedelta

class BigQueryHandler(S3Handler):
    '''
    purpose                  : interact with BigQuery
    param bigquery_proxy     : proxy setting for bigquery chosen from ('AWS' / 'GCP' / 'CORP' / 'LOCAL')
    param s3_proxy           : proxy setting for boto3 chosen from ('AWS' / 'GCP' / 'CORP' / 'LOCAL')
    param slack_proxy        : proxy setting for slack chosen from ('AWS' / 'GCP' / 'CORP' / 'LOCAL')
    param slack_channel      : slack channel for recording log 
    '''
    
    def __init__(self, bigquery_proxy = 'CORP', s3_proxy = 'CORP', slack_proxy = 'CORP', slack_channel = 'log-test'):
        '''
        purpose                  : used for interacting with BigQuery
        param bigquery_proxy     : bigquery proxy chosen from AWS / GCP / CORP or LOCAL (not setting proxy) with default = 'CORP'
        param s3_proxy           : s3 proxy chosen from AWS /GCP / CORP or LOCAL (not setting proxy) with default = 'CORP'
        param slack_proxy        : slack proxy chosen from AWS /GCP / CORP or LOCAL (not setting proxy) with default = 'CORP'
        param slack_channel      : slack channel for recording log with default = 'log-test'
        '''
        # change initial slack proxy setting of SlackHandler & s3 proxy setting of S3Handler       
        super().__init__(s3_proxy , slack_proxy, slack_channel)    
    
        if bigquery_proxy != 'LOCAL':
            bigquery_proxy_setting = 'http://{}:{}'.format(self.key_dict['proxy'][bigquery_proxy]['host'], self.key_dict['proxy'][bigquery_proxy]['port'])
            os.environ['http_proxy'] = bigquery_proxy_setting 
            os.environ['https_proxy'] = bigquery_proxy_setting
        
        self.credentials, self.project = google.auth.default(
                                            scopes = [
                                                "https://www.googleapis.com/auth/drive",
                                                "https://www.googleapis.com/auth/cloud-platform",
                                                "https://www.googleapis.com/auth/bigquery",
                                                     ]
                                            )
         
    def ReadBigQueryAsDF(self, query):
        '''
        purpose        :  read query result as DataFrame
        param query    :  query for executing in BigQuery
        return result_dict : a dictionary containing 1. is_read_succeed (boolean) 2. read_df (DataFrame) => only exists if is_read_succeed == True
        '''
             
        result_dict ={}
        read_starting_time = datetime.now()
        is_read_succeed = False
        try:
            client = bigquery.Client(credentials = self.credentials, project = self.project)
            df = client.query(query).to_dataframe()
            is_read_succeed = True
            result_dict['read_df'] = df
        except Exception as E:
            error_log = str(E)
        
        result_dict['is_read_succeed'] = is_read_succeed
        read_ending_time = datetime.now()
        
        message= '{} to read query result as DataFrame using time: {}{}'.format('Succeeded' if is_read_succeed else 'Failed', 
                                                                         read_ending_time - read_starting_time, 
                                                                         '' if is_read_succeed else ' due to {}'.format(error_log)) 
        
        print(message + '\n\n')
        self.PostMessage(self.slack_channel, message, '{}_Log_BigQuery'.format('Correct' if is_read_succeed else 'Error'))  
        return result_dict
    
    def ReadBigQueryAsObject(self, query):
        '''
        purpose        :  read query result as list
        param query    :  query for executing in BigQuery
        return result_dict : a dictionary containing 1. is_read_succeed (boolean) 2. read_object (object) => only exists if is_read_succeed == True
        '''
        
        result_dict ={}
        read_starting_time = datetime.now()
        is_read_succeed = False
        try:
            client = bigquery.Client(credentials = self.credentials, project = self.project)
            result_object = client.query(query)
            is_read_succeed = True
            result_dict['read_object'] = result_object
        except Exception as E:
            error_log = str(E)
        
        result_dict['is_read_succeed'] = is_read_succeed
        read_ending_time = datetime.now()
        
        message= '{} to read query result as object using time: {}{}'.format('Succeeded' if is_read_succeed else 'Failed', 
                                                                         read_ending_time - read_starting_time, 
                                                                         '' if is_read_succeed else ' due to {}'.format(error_log)) 
        
        print(message + '\n\n')
        self.PostMessage(self.slack_channel, message, '{}_Log_BigQuery'.format('Correct' if is_read_succeed else 'Error'))  
        return result_dict
    
    def ClearProxy(self):
        '''
        purpose                       : clear proxy setting
        return is_clear_proxy_succeed : boolean
        '''
        clear_starting_time = datetime.now()
        is_clear_proxy_succeed = False
        try:
            if os.environ.get('http_proxy'):
                del os.environ['http_proxy']        
            
            if os.environ.get('https_proxy'):
                del os.environ['https_proxy']
            
            is_clear_proxy_succeed = True
        except Exception as E:
            error_log = str(E)
        
        clear_ending_time = datetime.now()
        message= '{} to clear BigQuery proxy setting on environment using time: {}{}'.format('Succeeded' if is_clear_proxy_succeed else 'Failed', 
                                                                         clear_ending_time - clear_starting_time, 
                                                                         '' if is_clear_proxy_succeed else ' due to {}'.format(error_log)) 
        
        print(message + '\n\n')
        self.PostMessage(self.slack_channel, message, '{}_Log_BigQuery'.format('Correct' if is_clear_proxy_succeed else 'Error'))  
        return is_clear_proxy_succeed
    

    def CheckIfTableExists(self, table_id):
        '''
        purpose        : check if table exists or not
        param table_id : table_id => checked table composed of project_name.dataset_name.table_name
        '''

        is_table_exist = False

        check_starting_time = datetime.now()
        try:
            bigquery.Client().get_table(table_id)
            is_table_exist = True
        
        except:
            pass
        
        check_ending_time = datetime.now()
        
        message = 'check that table {} {} using time: {}'.format(table_id, 'exists' if is_table_exist else 'does not exist', check_ending_time - check_starting_time)
        
        print(message + '\n\n')

        self.PostMessage(self.slack_channel, message, '{}_Log_BigQuery'.format('Correct' if is_table_exist else 'Error'))  
        return is_table_exist
    
    def InsertIntoTableFromDataFrame(self, df, table_id,job_config = {}):
        '''
        purpose                  : insert DataFrame into table
        param  df                : DataFrame for inserting into table
        param  table_id          : BigQuery table for inserting into
        param  job_config        : schema setting config
        return is_insert_succeed : boolean
        '''
        insert_starting_time = datetime.now()
        is_insert_succeed = False
        try:
            client = bigquery.Client()
            job = client.load_table_from_dataframe(
                df, table_id, job_config = job_config)
            job.result()
            is_insert_succeed = True
        except Exception as E: 
            
            error_log = str(E)
        
        insert_ending_time = datetime.now()
        
        message= '{} to insert dataframe into table {} using time: {}{}'.format('Succeeded' if is_insert_succeed else 'Failed', 
                                                                            table_id, insert_ending_time - insert_starting_time, 
                                                                            '' if is_insert_succeed else ' due to {}'.format(error_log)) 
            
        print(message + '\n\n')
        self.PostMessage(self.slack_channel, message, '{}_Log_BigQuery'.format('Correct' if is_insert_succeed else 'Error'))  
        
        return is_insert_succeed
    
    def DeleteFromTable(self, query):
        '''
        purpose                  : delete data from table in BigQuery
        param  query             : query for executing in BigQuery
        return is_delete_succeed : boolean
        '''
        
        delete_starting_time = datetime.now()
        is_delete_succeed = False
        
        try:
            client = bigquery.Client()
            client.query(query)
            is_delete_succeed = True
        except Exception as E:
            error_log = str(E)
        
        delete_ending_time = datetime.now()
        
        message= '{} to execute deleting process using time: {}{}'.format('Succeeded' if is_delete_succeed else 'Failed', 
                                                                         delete_ending_time - delete_starting_time, 
                                                                         '' if is_delete_succeed else ' due to {}'.format(error_log)) 
        
        print(message + '\n\n')
        self.PostMessage(self.slack_channel, message, '{}_Log_BigQuery'.format('Correct' if is_delete_succeed else 'Error'))  
        
        return is_delete_succeed