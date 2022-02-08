import sys
sys.path.append('..')
import os
import requests
from datetime import datetime
from func_timeout import func_set_timeout, FunctionTimedOut
from oauth2client.service_account import ServiceAccountCredentials
from googleapiclient.discovery import build
from apiclient.http import MediaFileUpload
from Slack.SlackHandler import SlackHandler


class GoogleAPIHandler(SlackHandler):
    '''
    purpose                                 : used for interacting with Google API
    param api_service_name (Requird)        : service name of Google API you want to call with default = analytics, which means calling Google Analytics API
    param cred_dict_id (Required)           : id of credential dictionary for calling API recorded in key file
    param google_proxy (Required)           : proxy setting for google api chosen from ('AWS' / 'GCP' / 'CORP' / 'LOCAL')
    param slack_proxy (Required)            : slack proxy chosen from AWS /GCP / CORP or None (not setting proxy) with default = 'CORP'
    param slack_channel (Required)          : slack channel for recording log with default = 'log-test'
    param custom_dataset_dict_id (Optional) : id of custom dataset dictionary recorded in key file necessary only if need to manipulate custom data API
    param api_version (Optional)            : version of Google API with default = v3 if not assigned any
    '''

    def __init__(self, api_service_name, cred_dict_id, google_proxy = 'CORP', slack_proxy = 'CORP', slack_channel = 'log-test', **kwargs):
        '''
        purpose                                 : used for interacting with Google API
        param api_service_name (Requird)        : service name of Google API you want to call with default = analytics, which means calling Google Analytics API
        param cred_dict_id (Required)           : id of credential dictionary for calling API recorded in key file
        param google_proxy (Required)           : proxy setting for google api chosen from ('AWS' / 'GCP' / 'CORP' / 'LOCAL')
        param slack_proxy (Required)            : slack proxy chosen from AWS /GCP / CORP or None (not setting proxy) with default = 'CORP'
        param slack_channel (Required)          : slack channel for recording log with default = 'log-test'
        param custom_dataset_dict_id (Optional) : id of custom dataset dictionary recorded in key file necessary only if need to manipulate custom data API   
        '''
    
        super().__init__(slack_proxy, slack_channel)
        
        # set api service name
        self.api_service_name = api_service_name
        
        # set api version
        self.api_version = kwargs['api_version'] if kwargs.get('api_version') else 'v3'
        
        if google_proxy != 'LOCAL':
            self.google_proxy = 'http://{}:{}'.format(self.key_dict['proxy'][google_proxy]['host'], self.key_dict['proxy'][google_proxy]['port'])  
        else:
            self.google_proxy = None
        
        SCOPES = []
        if self.api_service_name == 'analytics':
            SCOPES = ['https://www.googleapis.com/auth/analytics']
        
        if self.api_service_name == 'drive':
            SCOPES = ['https://www.googleapis.com/auth/drive']
        
        self._credentials = ServiceAccountCredentials.from_json_keyfile_dict(
        keyfile_dict = self.key_dict['google_credential'][cred_dict_id], scopes = SCOPES)

        # check if assign custom dataset dictionary id for manipulating custom data API
        self.is_custom_dataset_dict_id_assign = False
        
        if kwargs.get('custom_dataset_dict_id'):
            self.custom_dataset_dict_id = kwargs['custom_dataset_dict_id']
            self.account_id, self.web_property_id, self.custom_data_source_id = [self.key_dict['ga_custom_data_info'][self.custom_dataset_dict_id][key] \
                                                                                for key in ['account_id', 'web_property_id', 'custom_data_source_id']]
            self.is_custom_dataset_dict_id_assign = True
    
    def ListCustomData(self, **kwargs):
        '''
        purpose              : (Google Analytics custom data API) used for listing all custom datasources of assigned custom dataset
        param max_result     : (Optional) maximal number of upload custom data per page with default = 1000
        param timeout_second : (Optional) timeout seconds with default = 60
        return return_dict   : (Required) is_process_succeed (type : boolean),  (Optional) return_data (type : list) only exists if is_process_succeed == True
        '''
        starting_time = datetime.now()
        is_process_succeed = False

        return_dict = {}
        try:
            if not self.is_custom_dataset_dict_id_assign:
                error_log = 'information of custom dataset dict id not found'
                raise ValueError(error_log)
            
            if self.google_proxy:
                os.environ['https_proxy'] = self.google_proxy
            
            # if timeout second is not assigned, the default value is 60 (seconds)
            timeout_second = kwargs['timeout_second'] if kwargs.get('timeout_second') else 60
            
            @func_set_timeout(timeout_second)
            def list_custom_data():
                start_index = 1
                max_result = kwargs['max_result'] if kwargs.get('max_result') else 1000
                return_data = []
                first_page = True
                sub_lst = []
                while first_page or sub_lst:
                    
                    # refresh token
                    self._credentials.refresh_token
                    
                    # get a refreshed access token because access token will expire after 3600 seconds since created
                    self._credentials.get_access_token()
                    
                    if first_page:
                        first_page = False
                    api_url = 'https://www.googleapis.com/analytics/v3/management/accounts/{}/webproperties/{}/customDataSources/{}/uploads?start-index={}&max-results={}'.format(self.account_id, 
                                                                                                                                                                                  self.web_property_id, 
                                                                                                                                                                                  self.custom_data_source_id, 
                                                                                                                                                                                  start_index, 
                                                                                                                                                                                  max_result)               
                    rs = requests.get(api_url, headers = {"Authorization": "Bearer " + self._credentials.access_token})
                    sub_lst = rs.json()['items']
                    
                    print('Succeeded to list cumstom data of assigned cum dataset at page index: {} with number: {}'.format(start_index, len(sub_lst)))          
                    if sub_lst:
                        start_index += 1
                        return_data.extend(sub_lst)

                return return_data
            
            return_dict['return_data'] = list_custom_data()
            is_process_succeed = True
        
        except FunctionTimedOut as E:
            error_log = '{}-second timeout'.format(timeout_second)
        
        except Exception as E:
            error_log = str(E)
        
        return_dict['is_process_succeed'] = is_process_succeed
        self.ClearProxy()
        
        ending_time = datetime.now()
        
        message = '{} to list all data information of assigned custom dataset{} by calling Google {} API {} using time: {}{}'.format('Succeeded' if is_process_succeed else 'Failed', 
                                                                                                                                     ': {}'.format(self.custom_dataset_dict_id) if self.is_custom_dataset_dict_id_assign else '',
                                                                                                                                     self.api_service_name.capitalize(),
                                                                                                                                     self.api_version.capitalize(),
                                                                                                                                     ending_time - starting_time, 
                                                                                                                                     '' if is_process_succeed else ' due to {}'.format(error_log)) 
                    
        print(message + '\n\n')
        self.PostMessage(self.slack_channel, message, '{}_Log_GoogleAPI'.format('Correct' if is_process_succeed else 'Error'))

        return return_dict
    
    def UploadCustomData(self, custom_data_file_path, **kwargs):
        '''
        purpose                     : (Google Analytics custom data API) used for update custom data file to assigned custom dataset
        param custom_data_file_path : (Required) local file path of csv file to be uploaded
        param timeout_second        : (Optional) timeout seconds with default = 240
        return is_process_succeed   : (type : boolean)
        '''
        
        starting_time = datetime.now()
        is_process_succeed = False
        
        try:
            if not self.is_custom_dataset_dict_id_assign:
                error_log = 'information of custom dataset dict id not found'
                raise ValueError(error_log)
            
            if self.google_proxy:
                os.environ['https_proxy'] = self.google_proxy
            
            # if timeout second is not assigned, the default value is 240 (seconds)
            timeout_second = kwargs['timeout_second'] if kwargs.get('timeout_second') else 240
            
            @func_set_timeout(timeout_second)
            def upload_custom_data():
                # Build object for manipulating Google API with assigned version
                analytics = build(self.api_service_name, self.api_version, credentials = self._credentials)
                
                media = MediaFileUpload(custom_data_file_path,
                                        mimetype = 'application/octet-stream',
                                        resumable = False)
                analytics.management().uploads().uploadData(
                                                            accountId = self.account_id,
                                                            webPropertyId = self.web_property_id,
                                                            customDataSourceId = self.custom_data_source_id,
                                                            media_body = media,
                                                            ).execute()
            upload_custom_data()
            is_process_succeed = True
        
        except FunctionTimedOut as E:
            error_log = '{}-second timeout'.format(timeout_second)
        
        except Exception as E:
            error_log = str(E)
        
        self.ClearProxy()
        
        ending_time = datetime.now()
        
        message = '{} to upload custom data to assigned custom dataset{} by calling Google {} API {} using time: {}{}'.format('Succeeded' if is_process_succeed else 'Failed', 
                                                                                                                              ': {}'.format(self.custom_dataset_dict_id) if self.is_custom_dataset_dict_id_assign else '',
                                                                                                                              self.api_service_name.capitalize(),
                                                                                                                              self.api_version.capitalize(),
                                                                                                                              ending_time - starting_time, 
                                                                                                                              '' if is_process_succeed else ' due to {}'.format(error_log)) 
                    
        print(message + '\n\n')
        self.PostMessage(self.slack_channel, message, '{}_Log_GoogleAPI'.format('Correct' if is_process_succeed else 'Error'))

        return is_process_succeed

    def DeleteUploadCustomData(self, delete_id_lst, **kwargs):
        '''
        purpose                   : (Google Analytics custom data API) used for delete upload custom data file in assigned custom dataset
        param delete_id_lst       : (Required)list of data id for all data to be deleted
        param timeout_second      : (Optional) timeout seconds with default = 60
        return is_process_succeed : (type : boolean)
        '''

        starting_time = datetime.now()
        is_process_succeed = False
        
        try:
            if not self.is_custom_dataset_dict_id_assign:
                error_log = 'information of custom dataset dict id not found'
                raise ValueError(error_log)
            
            if self.google_proxy:
                os.environ['https_proxy'] = self.google_proxy

            # if timeout second is not assigned, the default value is 60 (seconds)
            timeout_second = kwargs['timeout_second'] if kwargs.get('timeout_second') else 60
            
            @func_set_timeout(timeout_second)
            def delete_custom_data():
                # Build object for manipulating Google API with assogned version
                analytics = build(self.api_service_name, self.api_version, credentials = self._credentials)
        
                analytics.management().uploads().deleteUploadData(
                                                                accountId = self.account_id,
                                                                webPropertyId = self.web_property_id,
                                                                customDataSourceId = self.custom_data_source_id,
                                                                body = {
                                                                        'customDataImportUids': delete_id_lst
                                                                    }
                                                                ).execute()
            delete_custom_data()
            is_process_succeed = True
        
        except FunctionTimedOut as E:
            error_log = '{}-second timeout'.format(timeout_second)

        except Exception as E:
            error_log = str(E)
        
        self.ClearProxy()
        
        ending_time = datetime.now()
        
        message = '{} to delete assigned custom data from assigned custom dataset{} by calling Google {} API {} using time: {}{}'.format('Succeeded' if is_process_succeed else 'Failed', 
                                                                                                                                         ': {}'.format(self.custom_dataset_dict_id) if self.is_custom_dataset_dict_id_assign else '',
                                                                                                                                         self.api_service_name.capitalize(),
                                                                                                                                         self.api_version.capitalize(),
                                                                                                                                         ending_time - starting_time, 
                                                                                                                                         '' if is_process_succeed else ' due to {}'.format(error_log)) 
                    
        print(message + '\n\n')
        self.PostMessage(self.slack_channel, message, '{}_Log_GoogleAPI'.format('Correct' if is_process_succeed else 'Error'))

        return is_process_succeed
    
    def ListGoogleDriveFile(self, **kwargs):
        '''
        purpose              : (Google Drive API) used for listing information of all files on Google Drive
        param page_size      : (Optional) maximum number of files to be returned per page from API chosen from 1 - 1000 with default = 1000
        return return_dict   : (type : dictionary) including (Required) is_process_succeed (type : boolean), (Optional) return_data (type : list) only exists if is_process_succeed == True
        '''

        starting_time = datetime.now()
        is_process_succeed = False  
        return_dict = {}     
        try:      
            if self.google_proxy:
                os.environ['https_proxy'] = self.google_proxy
            
            page_token = None
            page_size = kwargs['page_size'] if kwargs.get('page_size') else 1000
            raw_api_url = 'https://www.googleapis.com/drive/{}/files?pageSize={}'.format(self.api_version, page_size)
            first_page = True
            item_lst = []          
            while first_page or page_token:
                # refresh access token (access token will expired within 3600 seconds after created)
                self._credentials.refresh_token
                
                # get access token
                self._credentials.get_access_token()

                headers = {"Authorization": "Bearer " + self._credentials.access_token}
                
                # calling for first page without pageToken
                if first_page:
                    api_url = raw_api_url
                    first_page = False
                else:
                    api_url = raw_api_url + '&pageToken={}'.format(page_token)
                
                rs = requests.get(api_url,
                                headers = headers)
                result_dict = rs.json()
                page_token = result_dict.get('nextPageToken')
                
                # accumulate all results from each page
                item_lst.extend(result_dict['files'])
                
                print('Succeeded to retrieve information of files with number: {}'.format(len(result_dict['files'])))
            
            return_dict['return_data'] = item_lst
            
            is_process_succeed = True

        except Exception as E:
            error_log = str(E)
        
        return_dict['is_process_succeed'] = is_process_succeed
        self.ClearProxy()
        
        ending_time = datetime.now()
        
        message = '{} to list all files{} by calling Google {} API using time: {}{}'.format('Succeeded' if is_process_succeed else 'Failed', 
                                                                                            ' with number: {}'.format(len(item_lst)) if is_process_succeed else '',
                                                                                            self.api_service_name.capitalize(),
                                                                                            ending_time - starting_time, 
                                                                                            '' if is_process_succeed else ' due to {}'.format(error_log)
                                                                                            )
                    
        print(message + '\n\n')
        self.PostMessage(self.slack_channel, message, '{}_Log_GoogleAPI'.format('Correct' if is_process_succeed else 'Error'))

        return return_dict

    def ReadGoogleDriveCSVFile(self, file_id, **kwargs):
        '''
        purpose              : (Google Drive API) used for listing information of all files on Google Drive
        param file_id        : (Required)id of file to be read
        param return_type    : (Optional) type of data to be return chosen from list / text with default = list
        param encoding       : (Optional) type of encoding chosen for text from request with default = ISO-8859-1 if not assigned
        return return_dict   : (type : dictionary) including (Required) is_process_succeed (type : boolean), (Optional) return_data (type : {{return_type}}) only exists if is_process_succeed == True
        '''
        starting_time = datetime.now()
        is_process_succeed = False  
        return_dict = {}     
        try:      
            if self.google_proxy:
                os.environ['https_proxy'] = self.google_proxy

            # refresh access token (access token will expired within 3600 seconds after created)
            self._credentials.refresh_token
            
            # get access token
            self._credentials.get_access_token()

            headers = {"Authorization": "Bearer " + self._credentials.access_token}

            api_url = 'https://www.googleapis.com/drive/v3/files/{}?alt=media'.format(file_id)

            rs = requests.get(api_url, headers = {"Authorization": "Bearer " + self._credentials.access_token})
            
            # use assigned encoding if any else keep the raw one which is ISO-8859-1
            if kwargs.get('encoding'):
                rs.encoding = kwargs['encoding']
            
            # type of return data if assigned any else list type as default
            return_type = 'list' if not kwargs.get('return_type') else kwargs['return_type']
            
            if return_type == 'list':
                from io import StringIO
                import csv

                reader = csv.reader(StringIO(rs.text), delimiter = ',')
                return_list = []
                for row in reader:
                    return_list.append(row)
                
                return_data = return_list 
            
            elif return_type == 'text':
                return_data = rs.text
            
            else:
                error_log = 'wrong return_type: {} assigned'.format(return_type)
                raise ValueError(error_log)

            return_dict['return_data'] = return_data
            is_process_succeed = True

        except Exception as E:
            error_log = str(E)
        
        return_dict['is_process_succeed'] = is_process_succeed
        self.ClearProxy()
        
        ending_time = datetime.now()
        
        message = '{} to read assigned csv file as type: {} by calling Google {} API using time: {}{}'.format('Succeeded' if is_process_succeed else 'Failed',
                                                                                                              return_type,
                                                                                                              self.api_service_name.capitalize(),
                                                                                                              ending_time - starting_time, 
                                                                                                              '' if is_process_succeed else ' due to {}'.format(error_log)
                                                                                                              )
        print(message + '\n\n')
        self.PostMessage(self.slack_channel, message, '{}_Log_GoogleAPI'.format('Correct' if is_process_succeed else 'Error'))

        return return_dict
    
    def ClearProxy(self):
        '''
        purpose                   : clear proxy setting
        return is_process_succeed : boolean
        '''
        starting_time = datetime.now()
        is_process_succeed = False
        try:
            if os.environ.get('http_proxy'):
                del os.environ['http_proxy']        
            
            if os.environ.get('https_proxy'):
                del os.environ['https_proxy']
            
            is_process_succeed = True

        except Exception as E:
            error_log = str(E)
        
        ending_time = datetime.now()
        
        message = '{} to clear Google proxy setting on environment using time: {}{}'.format('Succeeded' if is_process_succeed else 'Failed', 
                                                                                           ending_time - starting_time, 
                                                                                           '' if is_process_succeed else ' due to {}'.format(error_log)) 
        
        print(message + '\n\n')
        self.PostMessage(self.slack_channel, message, '{}_Log_GoogleAPI'.format('Correct' if is_process_succeed else 'Error'))  
        
        return is_process_succeed