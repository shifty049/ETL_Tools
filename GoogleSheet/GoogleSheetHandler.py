import sys
sys.path.append('..')
import os
import gspread
from datetime import datetime
from oauth2client.service_account import ServiceAccountCredentials
from S3.S3Handler import S3Handler
from Slack.SlackHandler import SlackHandler
from func_timeout import func_set_timeout, FunctionTimedOut

auth_json_path = '/Users/benq/Desktop/my-project-etl-290207-41dd883992be.json'
spreadsheet_key = '1f9wGqjnOruOHabAPe2mDsZYODwHyWsu0N-BT2Y7udz8'


class GoogleSheetHandler(S3Handler):
    '''
    purpose                  : interact with Googlesheet API
    param gspread_key_name   : name of gspread key name at key.txt with etl_schdule for BenQ-Data_ETL schedule management 
                               and si_company_list for SI Company List and etl_schdule as default
    param sheet_name         : sheet name of google sheet
    param gs_proxy           : proxy setting for gspread chosen from ('AWS' / 'GCP' / 'CORP' / 'LOCAL')
    param s3_proxy           : proxy setting for boto3 chosen from ('AWS' / 'GCP' / 'CORP' / 'LOCAL')
    param slack_proxy        : proxy setting for slack chosen from ('AWS' / 'GCP' / 'CORP' / 'LOCAL')
    param slack_channel      : slack channel for recording log 
    '''

    def __init__(self, gspread_key_name = 'etl_schdule', gs_proxy = 'CORP', gs_timeout = 10, s3_proxy = 'CORP', slack_proxy = 'CORP', slack_channel = 'log-test'):
        '''
        purpose                  : used for interacting with Googlesheet API
        param gspread_key_name   : name of gspread key name at key.txt with etl_schdule for BenQ-Data_ETL schedule management 
                                   and si_company_list for SI Company List and etl_schdule as default
        param gs_proxy           : gspread proxy chosen from AWS /GCP / CORP with default = 'CORP'
        param gs_timeout         : timeout setting for gs connection with default = 15 seconds
        param s3_proxy           : s3 proxy chosen from AWS /GCP / CORP or None (not setting proxy) with default = 'CORP'
        param slack_proxy        : slack proxy chosen from AWS /GCP / CORP or None (not setting proxy) with default = 'CORP'
        param slack_channel      : slack channel for recording log with default = 'log-test'
        '''
     
        super().__init__(s3_proxy , slack_proxy, slack_channel)
        
        connect_starting_time = datetime.now()
        self.is_connect_succeed = False
        if gs_proxy != 'LOCAL':
            gs_proxy_setting = 'http://{}:{}'.format(self.key_dict['proxy'][gs_proxy]['host'], self.key_dict['proxy'][gs_proxy]['port'])
            os.environ['http_proxy'] = gs_proxy_setting 
            os.environ['https_proxy'] = gs_proxy_setting 
        

        gss_scopes = ['https://spreadsheets.google.com/feeds']

        # connect to google sheet
        credentials = ServiceAccountCredentials.from_json_keyfile_name(self.key_dict['gspread']['key_file_path'],gss_scopes)

        gss_client = gspread.authorize(credentials)

        
        # open google Sheet with assigned sheet name
       
        @func_set_timeout(gs_timeout)
        def gs_connect():
            
            self.sheet_info_dict = self.key_dict['gspread']['gspread_key_name'][gspread_key_name]
            self.sh = gss_client.open_by_key(self.sheet_info_dict['spreadsheet_key'])
            self.sheet = self.sh.worksheet(self.sheet_info_dict['sheet_name'])
       
        try:
            gs_connect()
            self.is_connect_succeed = True
        
        except FunctionTimedOut as E:
            error_log = '{}-second timeout'.format(gs_timeout)
            
        except Exception as E:
            error_log = str(E)
        
        if not self.is_connect_succeed:
            self.ClearProxy()
        
        connect_ending_time = datetime.now()


        
        message= '{} to connect to GoogleSheets: {} using time: {}{}'.format('Succeeded' if self.is_connect_succeed else 'Failed', 
                                                                         self.sheet_info_dict['sheet_name'], connect_ending_time - connect_starting_time, 
                                                                         '' if self.is_connect_succeed else ' due to {}'.format(error_log)) 
        
        print(message + '\n\n')
        self.PostMessage(self.slack_channel, message, '{}_Log_GS'.format('Correct' if self.is_connect_succeed else 'Error'))
            
    def ReadAsDataFrame(self):
        '''
        purpose  : read assigned GoogleSheets as DataFrame format
        return read_df : {is_reaf_succeed : boolean, read_df : DataFrame (only exists if is_reaf_succeed == True)}
        '''
        read_starting_time = datetime.now()
        is_read_succeed = False
        read_dict = {}
        try:
            import pandas as pd
            data_list = self.sheet.get_all_values()
            df = pd.DataFrame(data_list[1:],columns = data_list[0])
            is_read_succeed = True        
            read_dict['read_df'] = df
        
        except Exception as E:
            error_log = str(E)
        
        read_dict['is_read_succeed'] = is_read_succeed
        read_ending_time = datetime.now()
        
        message= '{} to read GoogleSheets: {} as DataFrame using time: {}{}'.format('Succeeded' if is_read_succeed else 'Failed', 
                                                                         self.sheet_info_dict['sheet_name'], read_ending_time - read_starting_time, 
                                                                         '' if is_read_succeed else ' due to {}'.format(error_log)) 
        
        print(message + '\n\n')
        self.PostMessage(self.slack_channel, message, '{}_Log_GS'.format('Correct' if is_read_succeed else 'Error'))
        
        return read_dict

    def UpdateCell(self, cell_location, updated_content):
        '''
        purpose                  : update assigned cell's content of GoogleSheets
        param cell_location      : assigned cell location to be updatd e.g "A1", "D2" and so on
        param updated_content    : updated content for assigned cell
        return is_update_succeed : (boolean)
        '''
        is_update_succeed = False
        update_starting_time = datetime.now()
        try:
            self.sheet.update_acell(cell_location, updated_content)
            is_update_succeed = True
        except Exception as E:
            error_log = str(E)
        
        update_ending_time = datetime.now()

        message = '{} to update {} of GoogleSheets: {} with content: {} using time: {}{}'.format('Succeeded' if is_update_succeed else 'Failed', 
                                                                          cell_location, self.sheet_info_dict['sheet_name'], updated_content, 
                                                                          update_ending_time - update_starting_time, 
                                                                          '' if is_update_succeed else ' due to {}'.format(error_log))
        print(message + '\n\n')
        self.PostMessage(self.slack_channel, message, '{}_Log_GS'.format('Correct' if is_update_succeed else 'Error'))  
        return is_update_succeed

    def UpdateCellByRange(self, sheet_range, updated_value_list):
        '''
        purpose                        : update GoogleSheets content by range
        param sheet_range              : sheet range to be updated
        param updated_value_list       : nested list containing values for updating
        return is_range_update_succeed : (boolean)
        '''
        
        starting_time = datetime.now()
        is_range_update_succeed = False
        
        try:
            self.sheet.update(sheet_range,updated_value_list)
        
            is_range_update_succeed = True
        
        except Exception as E:
            
            error_log = str(E)
            
        ending_time = datetime.now()
        
        message = '{} to update GoogleSheets: {} within range: {} using time: {}{}'.format('Succeeded' if is_range_update_succeed else 'Failed',
                                                                                            self.sheet_info_dict['sheet_name'], sheet_range, ending_time - starting_time, 
                                                                                            '' if is_range_update_succeed else ' due to {}'.format(error_log))
        print(message + '\n\n')
        self.PostMessage(self.slack_channel, message, '{}_Log_GS'.format('Correct' if is_range_update_succeed else 'Error'))  
        
        return is_range_update_succeed

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
        message= '{} to clear proxy setting on environment using time: {}{}'.format('Succeeded' if is_clear_proxy_succeed else 'Failed', 
                                                                         clear_ending_time - clear_starting_time, 
                                                                         '' if is_clear_proxy_succeed else ' due to {}'.format(error_log)) 
        
        print(message + '\n\n')
        self.PostMessage(self.slack_channel, message, '{}_Log_GS'.format('Correct' if is_clear_proxy_succeed else 'Error'))  
        return is_clear_proxy_succeed
    
    def ClearSheetByRange(self,range_to_clear):
        '''
        purpose              : clear assigned googlesheet by range provided
        param range_to_clear : range of assigned GoogleSheet for clearing
        return               : is_clear_succeed (boolean)
        '''
        starting_time = datetime.now()
        is_clear_succeed = False
        
        try:
            self.sh.values_clear("{}!{}".format(self.sheet_info_dict['sheet_name'], range_to_clear))
            is_clear_succeed = True
        except Exception as E:
            error_log = str(E)
        
        ending_time = datetime.now()
        message = '{} to clear GoogleSheets: {} within range: {}{} using time {}'.format('Succeeded' if is_clear_succeed else 'Failed', 
                                                                                     self.sheet_info_dict['sheet_name'], 
                                                                                     range_to_clear, 
                                                                                     '' if is_clear_succeed else ' due to {}'.format(error_log), 
                                                                                     ending_time - starting_time)
        
        print(message + '\n\n')
        self.PostMessage(self.slack_channel, message, '{}_Log_GS'.format('Correct' if is_clear_succeed else 'Error'))  
        return is_clear_succeed