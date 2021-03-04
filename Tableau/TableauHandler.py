import sys
sys.path.append('..')
import os
import tableauserverclient as TSC
from S3.S3Handler import S3Handler
from datetime import datetime

class TableauHandler(S3Handler):
    '''
    purpose             : Interact with Tableau Server using API
    param s3_proxy      : proxy setting for boto3 chosen from ('AWS' / 'GCP' / 'CORP' / 'LOCAL')
    param slack_proxy   : proxy setting for slack chosen from ('AWS' / 'GCP' / 'CORP' / 'LOCAL')
    param slack_channel : slack channel for recording log
    '''
    def __init__(self, s3_proxy='CORP', slack_proxy='CORP', slack_channel = 'log-test'):
        '''
        param s3_proxy      : S3 proxy chosen from AWS /GCP / CORP or None (not setting proxy) with default = 'CORP'
        param slack_proxy   : Slack proxy chosen from AWS /GCP / CORP or None (not setting proxy) with default = 'CORP'
        param slack_channel : slack channel for recording log with default = 'log-test'
        '''
        super().__init__(s3_proxy, slack_proxy, slack_channel)

#===========================================================  Login  =======================================================================

    def Login(self):
        '''
        purpose                  : login tableau server
        return is_login_succeed  : a boolean presents if Login succeeded or nit
        '''
        login_starting_time = datetime.now()
        is_login_succeed = False
        try:
            tableau_info_dict = self.key_dict['tableau']
            #===============================================================================================#
            # tableau_info_dict : a dictionary containing all information necessary for login Tableau server 
            #                     server      => tableau server address
            #                     username    => tableau server username
            #                     password    => tableau server password 
            #===============================================================================================#
            tableau_auth = TSC.TableauAuth(tableau_info_dict['username'],tableau_info_dict['password'])
            # define tableau server address
            self.url_address = tableau_info_dict['url']
            self.server = TSC.Server(self.url_address)
            # use API of server verison
            self.server.use_server_version()
            self.server.auth.sign_in(tableau_auth)          
            self.all_views = list(TSC.Pager(self.server.views))
            self.all_workbooks = list(TSC.Pager(self.server.workbooks))
            self.all_datasources = list(TSC.Pager(self.server.datasources))
            
            # a dictionary for mapping workbook information together with view information
            mapping_dict={}
            for workbook in self.all_workbooks:
                mapping_dict.update({workbook.id:\
                             {'workbook_name' : workbook.name,
                              'project_name'  : workbook.project_name,\
                              'project_id'    : workbook.project_id}})

            # a list containing all information
            self.view_list = []    
            for view in self.all_views:   
                self.view_list.append\
                (
                 {'project_name' : mapping_dict[view.workbook_id]['project_name'],
                  'project_id' : mapping_dict[view.workbook_id]['project_id'],
                  'workbook_name' : mapping_dict[view.workbook_id]['workbook_name'],
                  'workbook_id' : view.workbook_id,
                  'view_name' : view.name,
                  'view_id' : view.id
                 }
                )
            is_login_succeed = True
            
        except Exception as E:
            error_log = str(E)
        
        login_ending_time = datetime.now()
        login_message = '{} to login {} using time: {}{}'.format('Succeeded' if is_login_succeed else 'Failed',self.url_address, 
                                                                 login_ending_time - login_starting_time, 
                                                                 '' if is_login_succeed else ' due to {}'.format(error_log))
        
        self.PostMessage(self.slack_channel, login_message, 'Correct_Log_Tableau' if is_login_succeed else 'Error_Log_Tableau')
        print(login_message + '\n\n')
        
        return is_login_succeed
        
#====================================================  Download By View ID  ================================================================

    def DownloadByViewId(self, view_id, local_path, file_name, file_type = 'csv'):
        '''
        purpose                     : download view with view_id given from tableau server to local directory
        param view_id               : view id for downloading
        param local_path            : setting local file directory
        param file_name             : setting local file name       
        prarm file_type             : setting file type (default csv)
        return is_download_succeed  : a boolean presents if DownloadByViewId succeeded or not
        '''
        
        download_starting_time = datetime.now()
        is_download_succeed = False
        # absolute path of downloaded file
        absoute_local_file_path = '{}/{}.{}'.format(local_path,file_name,file_type)
        if os.path.exists(absoute_local_file_path):
            os.remove(absoute_local_file_path)     
        try:
            view_item = next(filter(lambda x: x.id == view_id,self.all_views),None)
            if not view_item:
                error_log = 'item_view not found'
            else:
                              
                with open(absoute_local_file_path, 'wb') as f:
                    if file_type == 'csv':
                        self.server.views.populate_csv(view_item)             
                        f.write(b''.join(view_item.csv))
                    elif file_type == 'png':
                        self.server.views.populate_image(view_item)
                        f.write(view_item.image)
                    
                is_download_succeed = True
        except Exception as E:
            is_download_succeed = False
            error_log = str(E)
        
        download_ending_time = datetime.now()
        downlod_message = '{} to download {} file to {} using time: {}{}'.format('Succeeded' if is_download_succeed else 'Failed', 
                                                                                 file_type, absoute_local_file_path, 
                                                                                 download_ending_time - download_starting_time, 
                                                                                 '' if is_download_succeed else ' due to {}'.format(error_log))
        
        
        print(downlod_message + '\n\n')
        self.PostMessage(self.slack_channel, downlod_message, 'Correct_Log_Tableau' if is_download_succeed else 'Error_Log_Tableau')
        
        return is_download_succeed
                       
#===========================================================  Refresh  ======================================================================                      

    def RefreshDataSource(self, datasource_id):
        '''
        purpose                     : download view with view_id given from tableau server to local directory
        param datasource_id         : datasource id for refreshing
        return is_refresh_succeed  : a boolean presents if RefreshDataSource succeeded or not
        '''
        is_refresh_succeed = False
        refresh_starting_time = datetime.now()
        try:
            datasource_item = self.server.datasources.get_by_id(datasource_id)
            self.server.datasources.refresh(datasource_item)
            is_refresh_succeed = True
        except Exception as E:
            error_log = str(E)
            
        refresh_ending_time = datetime.now()   
        refresh_message = '{} to refresh datasource id {} using time: {}{}'.format('Succeeded' if is_refresh_succeed else 'Failed', datasource_id, 
                                                                                refresh_ending_time - refresh_starting_time, 
                                                                                '' if is_refresh_succeed else ' due to {}'.format(error_log))
        
        print(refresh_message + '\n\n')
        self.PostMessage(self.slack_channel, refresh_message, 'Correct_Log_Tableau' if is_refresh_succeed else 'Error_Log_Tableau')                                                                  
        return is_refresh_succeed
#===========================================================  Logout  ======================================================================
    
    def Logout(self):
        '''
        purpose                   : logout from tableu server
        return is_logout_succeed  : a boolean presents if Login succeeded or not
        '''
        logout_starting_time = datetime.now()
        is_logout_succeed = False
        try:
            self.server.auth.sign_out()
            is_logout_succeed=True
            
        except Exception as E:
            error_log=str(E)

        logout_ending_time = datetime.now()   
        logout_message = '{} to logout from {} using time: {}{}'.format('Succeeded' if is_logout_succeed else 'Failed',
                                                                         self.url_address, logout_ending_time - logout_starting_time, 
                                                                         '' if is_logout_succeed else ' due to {}'.format(error_log))
        
        
        print(logout_message + '\n\n')
        self.PostMessage(self.slack_channel, logout_message, 'Correct_Log_Tableau' if is_logout_succeed else 'Error_Log_Tableau')
        
        return is_logout_succeed