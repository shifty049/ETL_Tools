import sys
import pandas as pd
sys.path.append('/Users/benq/Documents/Project/github/data_etl/sub_functions')
from Slack.SlackHandler import SlackHandler
from azure.storage.blob import BlobServiceClient
from azure.storage.blob._shared.base_client import create_configuration
from datetime import datetime
from io import StringIO

class AzureBlobHandler(SlackHandler):
    '''
    purpose                  : interact with Azure Blob
    param azure_account      : Azure account chosen 
    param blob_proxy         : Blob proxy chosen from AWS /GCP / CORP with default = 'CORP'
    param slack_proxy        : proxy setting for slack chosen from ('AWS' / 'GCP' / 'CORP' / 'LOCAL')
    param slack_channel      : slack channel for recording log 
    '''
    
    def __init__(self, azure_account, blob_proxy= 'CORP', slack_proxy = 'CORP', slack_channel = 'log-test'):
        '''
        purpose                  : used for interacting with Azure Blob
        param azure_account      : Azure account chosen 
        param blob_proxy         : Blob proxy chosen from AWS /GCP / CORP with default = 'CORP'
        param slack_proxy        : slack proxy chosen from AWS /GCP / CORP or None (not setting proxy) with default = 'CORP'
        param slack_channel      : slack channel for recording log with default = 'log-test'
        '''
        
        super().__init__(slack_proxy, slack_channel)
        
        connection_string = self.key_dict['Azure'][azure_account]['Blob']['Connection string']
        self.default_container = self.key_dict['Azure'][azure_account]['Blob']['Container']
        
        if blob_proxy != 'LOCAL':
            blob_proxy_setting = 'http://{}:{}'.format(self.key_dict['proxy'][blob_proxy]['host'], self.key_dict['proxy'][blob_proxy]['port'])

            http_proxy = https_proxy = blob_proxy_setting

            config = create_configuration(storage_sdk = 'blob')
            config.proxy_policy.proxies = {
                'http': http_proxy,
                'https': https_proxy
            }

            self.service_client = BlobServiceClient.from_connection_string(connection_string, _configuration = config)               
        else:
            self.service_client = BlobServiceClient.from_connection_string(connection_string)
    
    def UploadFileASBlob(self,upload_file_path, container_name, blob_name):
        '''
        purpose                : used for uploading file as Blob
        param upload_file_path : absolute path of uploaded file
        param container_name   : container chosen to be uploaded  
        param blob_name        : Blob name (extension included) of uploaded file  
        return is_upload_succeed(boolean)
        '''
        starting_time = datetime.now()    
        is_upload_succeed = False
        
        try:
            blob_client = self.service_client.get_blob_client(container = container_name, blob = blob_name)

            with open(upload_file_path, "rb") as data:
                blob_client.upload_blob(data)
            
            is_upload_succeed = True
        
        except Exception as E:
            error_log = str(E)
        
        ending_time = datetime.now()
        
        message = '{} to upload file: {} to Container: {} as Blob: {}{} using time: {}'.format('Succeeded' if is_upload_succeed else 'Failed',
                                                                                                upload_file_path, 
                                                                                                container_name, 
                                                                                                blob_name, 
                                                                                                '' if is_upload_succeed else ' due to {}'.format(error_log), 
                                                                                                ending_time - starting_time)
        
        print(message + '\n\n')
        self.PostMessage(self.slack_channel, message, '{}_Log_Azure_Blob'.format('Correct' if is_upload_succeed else 'Error'))
        
        return is_upload_succeed
    
    def DeleteBlob(self, container_name, blob_name):
        '''
        purpose                : used for deleting Blob file
        param container_name   : container of blob chosen to be deleted
        param blob_name        : Blob name (extension included) to be deleted
        return is_delete_succeed(boolean)
        '''
        starting_time = datetime.now()    
        is_delete_succeed = False
        
        try:
            blob_client = self.service_client.get_blob_client(container = container_name, blob = blob_name)
            blob_client.delete_blob()
            is_delete_succeed = True
        
        except Exception as E:
            error_log = str(E)
        
        ending_time = datetime.now()
        
        message = '{} to delete Blob: {} in Container: {}{} using time: {}'.format('Succeeded' if is_delete_succeed else 'Failed',
                                                                                               blob_name, 
                                                                                               container_name,                                                                                                   
                                                                                               '' if is_delete_succeed else ' due to {}'.format(error_log), 
                                                                                               ending_time - starting_time)
        
        print(message + '\n\n')
        self.PostMessage(self.slack_channel, message, '{}_Log_Azure_Blob'.format('Correct' if is_delete_succeed else 'Error'))

        return is_delete_succeed
    
    def ReadCSVAsDF(self, container_name, blob_name):
        '''
        purpose                : used for reading a csv-format Blob as DataFrame
        param container_name   : container of blob chosen to be read
        param blob_name        : Blob name (extension included) to be read (must be csv format)
        return result_dict (dictionary): {'is_read_succeed': boolean (required), 'read_df': dataframe (only exists when is_read_succeed == True)}
        '''
        starting_time = datetime.now()    
        is_read_succeed = False
        result_dict = {}
        try:
            blob_client = self.service_client.get_blob_client(container = container_name, blob = blob_name)
            
            df = pd.read_csv(StringIO(blob_client.download_blob().content_as_text()))
            result_dict['read_df'] = df
            is_read_succeed = True
        
        except Exception as E:
            error_log = str(E)
        
        result_dict['is_read_succeed'] = is_read_succeed
        ending_time = datetime.now()
        
        message = '{} to read Blob: {} in Container: {} as DataFrame{} using time: {}'.format('Succeeded' if is_read_succeed else 'Failed',
                                                                                               blob_name, 
                                                                                               container_name,                                                                                                   
                                                                                               '' if is_read_succeed else ' due to {}'.format(error_log), 
                                                                                               ending_time - starting_time)
        
        print(message + '\n\n')
        self.PostMessage(self.slack_channel, message, '{}_Log_Azure_Blob'.format('Correct' if is_read_succeed else 'Error'))

        return result_dict