import sys
sys.path.append('..')
import boto3
import pandas as pd
from botocore.config import Config
from io import StringIO
from Slack.SlackHandler import SlackHandler
from datetime import datetime
from func_timeout import func_set_timeout, FunctionTimedOut


class S3Handler(SlackHandler):
    '''
    purpose             : used for interacting with AWS S3 bucket
    param s3_proxy      : proxy setting for boto3 chosen from ('AWS' / 'GCP' / 'CORP' / 'LOCAL')
    param slack_proxy   : proxy setting for slack chosen from ('AWS' / 'GCP' / 'CORP' / 'LOCAL')
    param slack_channel : slack channel for recording log
    
    '''
    
    def __init__(self, s3_proxy = 'CORP', slack_proxy = 'CORP', slack_channel = 'log-test'):
        '''
        param s3_proxy      : proxy setting for boto3 chosen from 'AWS' / 'GCP' / 'CORP' / 'LOCAL' (use local settnig) with 'CORP' as default
        param slack_proxy   : proxy setting for slack chosen from 'AWS' / 'GCP' / 'CORP' / 'LOCAL' (use local settnig) with 'CORP' as default
        param slack_channel : slack channel for recording log with default = 'log-test'
        '''
        # change initial slack proxy setting of SlackHandler      
        super().__init__(slack_proxy, slack_channel)
    
        s3_info_dict = self.key_dict['s3']
        
        #===============================================================================================#
        # s3_info_dict : a dictionary containing all information necessary for s3
        #                     aws_access_key_id       => aws_access_key_id
        #                     aws_secret_access_key   => aws_secret_access_key
        #                     bucket                  => bucket name
        #===============================================================================================#
        
        session = boto3.Session(s3_info_dict['aws_access_key_id'],s3_info_dict['aws_secret_access_key'])
        
        # not use proxy 
        if s3_proxy == 'LOCAL':
            self.s3_client = session.client('s3')
            self.s3_resource = session.resource('s3')
        # use assigned proxy with CORP as default
        else: 
            proxy = '{}:{}'.format(self.key_dict['proxy'][s3_proxy]['host'], self.key_dict['proxy'][s3_proxy]['port'])
            self.s3_client = session.client('s3', config = Config(proxies = {'http': proxy, 'https': proxy}))
            self.s3_resource = session.resource('s3', config = Config(proxies = {'http': proxy, 'https': proxy}))
        
        self.bucket = s3_info_dict['bucket']
        self.csv_buffer = StringIO()

 #=======================================================  List All Objects  ==================================================================

    def ListObjects(self, directory = None, timeout_second = 15):
        '''
        purpose                  : used for listing all objects in s3 bucket (under a specific directory)
        param  directory         : directory of bucket (if None => list all objects in bucket else => list all objects under directory)   
        param  timeout_second    : limitation (seconds) for timeout
        return list_dict         : is_list_object_succeed => a booolean to present if ListAllObjects process succeeds
                                   object_list            => object list for a specific bucket under(specific directory) only exists if is_list_object_succeed == True
        '''       
        is_list_object_succeed = False
        list_starting_time = datetime.now()
        list_dict ={}
        # set up timeout upper bound : default 25 seconds
        @func_set_timeout(timeout_second)
        def list_objects():
            '''
            purpose : list all objects in s3 bucket (under a specific directory)
            '''
            s3_bucket = self.s3_resource.Bucket(self.bucket)
            if not directory:
                objs = s3_bucket.objects.filter()
            else:
                objs = s3_bucket.objects.filter(Prefix = directory)

            return [obj.key for obj in objs]
        
        try:
            list_dict['object_list'] = list_objects()
            is_list_object_succeed = True
            
        except FunctionTimedOut as E:
            error_log=str(E)
            
        except Exception as E:
            error_log=str(E)
        
        list_dict['is_list_object_succeed'] = is_list_object_succeed
        list_ending_time = datetime.now()
        list_message = '{} to list all objects in bucket: {}{} using time: {}{}'.format('Succeeded' if is_list_object_succeed else 'Failed', self.bucket,
                                                                                     '' if not directory else ' under directory: {}'.format(directory), 
                                                                                     list_ending_time - list_starting_time, 
                                                                                     '' if is_list_object_succeed else ' due to {}'.format(error_log))
        
        print(list_message + '\n\n')
        self.PostMessage(self.slack_channel, list_message, 'Correct_Log_S3' if is_list_object_succeed else 'Error_Log_S3')
        return list_dict
    
#=========================================================  Upload File  ====================================================================

    def UploadFile(self,upload_file_path, upload_object_name, timeout_second = 35):
        '''
        purpose                  : used for uploading file to s3 bucket
        param upload_file_path   : local path of uploaded file
        param upload_object_name : object name of uploaded file in s3
        param timeout_second     : limitation (seconds) for timeout
        return is_upload_succeed : a booolean to present if UploadFile process succeeds
        '''     
        
        # set up timeout upper bound : default 25 seconds
        @func_set_timeout(timeout_second)
        def s3_uploader():
            '''
            purpose : upload file to s3 bucket
            '''
            self.s3_client.upload_file(upload_file_path,self.bucket,upload_object_name)
        
        is_upload_succeed = False
        upload_starting_time = datetime.now()
        try:
            s3_uploader()
            is_upload_succeed=True
        except FunctionTimedOut as E:
            error_log=str(E)
        
        except Exception as E:
            error_log=str(E)
        
        upload_ending_time = datetime.now()
        upload_message = '{} to upload {} to {} in bucket {} using time: {}{}'.format('Succeeded' if is_upload_succeed else 'Failed', 
                                                                     upload_file_path, upload_object_name, self.bucket, 
                                                                     upload_ending_time - upload_starting_time, 
                                                                     '' if is_upload_succeed else ' due to {}'.format(error_log))
        
        print(upload_message + '\n\n')
        self.PostMessage(self.slack_channel, upload_message, 'Correct_Log_S3' if is_upload_succeed else 'Error_Log_S3')
        return is_upload_succeed
    
#=========================================================  Download File  ====================================================================

    def DownloadFile(self,download_object_name, download_file_name, timeout_second = 35):
        '''
        purpose                    : used for downloading file from s3 bucket
        param download_file_path   : local path of downloaded file
        param download_object_name : object name of downloaded name in s3
        param timeout_second       : limitation (seconds) for timeout
        return is_download_succeed : a booolean to present if DownloadFile process succeeds                  
        '''       
        # set up timeout upper bound : default 25 seconds
        @func_set_timeout(timeout_second)
        def s3_downloader():
            '''
            purpose : download file from s3 bucket
            '''
            self.s3_client.download_file(self.bucket,download_object_name,download_file_name)

        is_download_succeed = False
        download_starting_time = datetime.now()
        try:
            # set up timeout upper bound :15 seconds
            s3_downloader()
            is_download_succeed = True
        
        except FunctionTimedOut as E:
            error_log=str(E)
        
        except Exception as E:
            error_log=str(E)
        
        download_ending_time = datetime.now()
        download_message='{} to download {} from bucket {} to {} using time: {}{}'.format('Succeeded' if is_download_succeed else 'Failed',
                                                                                          download_object_name, self.bucket ,download_file_name, 
                                                                                          download_ending_time - download_starting_time, 
                                                                                          '' if is_download_succeed else ' due to {}'.format(error_log))
        
        print(download_message + '\n\n')
        self.PostMessage(self.slack_channel, download_message, 'Correct_Log_S3' if is_download_succeed else 'Error_Log_S3')
        return is_download_succeed
    

#====================================================  Read File As DataFrame   ===============================================================

    def ReadCSVFromS3AsDF(self, object_list, time_out_second = 35):
        '''
        purpose                : used for reading csv file from s3 as DataFrame
        param object_list      : list of objects for concatenating as DataFrame
        param timeout_second   : limitation (seconds) for timeout
        return return_dict     : a dictionary including is_read_succeed =>  if succeed to read S3 object as DataFrame                                      
                                                                            if is_read_succeed == True:
                                                                                read_df => dataframe transformed from S3 object
        '''    
        return_dict={}
        is_read_succeed = False
        read_starting_time = datetime.now()
        @func_set_timeout(time_out_second)
        def s3_df_formatter(object_name):
            '''
            purpose : get csv object from s3 bucket
            '''
            csv_object = self.s3_client.get_object(Bucket = self.bucket, Key = object_name)
            csv_string = csv_object['Body'].read().decode('utf-8')
            df = pd.read_csv(StringIO(csv_string))
            return df
            
        try:
            # get csv object and transform csv object into dataframe
            df_list = []
            for obj in object_list:
                df_list.append(s3_df_formatter(obj))

            concatenation_df = pd.concat(df_list)
            
            is_read_succeed=True
            return_dict={'read_df':concatenation_df}

        except FunctionTimedOut as E:
            error_log=str(E)

        except Exception as E:
            error_log = str(E)        
        
        read_ending_time = datetime.now()
        read_message = '{} to read {} object{} from S3 Bucket {} as one DataFrame using time: {}{}'.format('Succeeded' if is_read_succeed else 'Failed', len(object_list), 
                                                                                            '' if len(object_list) < 1 else 's', self.bucket, 
                                                                                            read_ending_time - read_starting_time, 
                                                                                            '' if is_read_succeed else ' with error log : {}'.format(error_log))
        return_dict['is_read_succeed']=is_read_succeed
        print(read_message + '\n\n')
        self.PostMessage(self.slack_channel, read_message, 'Correct_Log_S3' if is_read_succeed else 'Error_Log_S3')
        return return_dict

#===================================================  Upload CSV From DataFrame  ==============================================================

    def UploadCSVToS3FromDF(self, object_name, timeout_second = 35):
        '''
        purpose                      : used for uploading DataFrame to s3 as csv file
        param object_name            : name of uploaded object in s3
        param timeout_second         : limitation (seconds) for timeout default: 25 seconds
        return is_upload_succeed     : if succeed to upload dataframe to s3 as csv file                                                                                                            
        '''
        
        @func_set_timeout(timeout_second)
        def s3_df_uploader():
            '''
            purpose : upload DataFrame to s3 bucket as csv file
            '''
            self.s3_client.put_object(Bucket = self.bucket, Body = self.csv_buffer.getvalue(), Key = object_name)    
        
        is_upload_succeed = False
        upload_starting_time = datetime.now()
        try:
            s3_df_uploader()
            is_upload_succeed = True
            
        except FunctionTimedOut as E:
            error_log=str(E)
        
        except Exception as E:
            error_log=str(E)

        # clear self.csv_buffer
        self.csv_buffer = StringIO()
        
        upload_ending_time = datetime.now()
        upload_message = '{} to upload DataFrame to {} in bucket {} using time{}{}'.format('Succeeded' if is_upload_succeed else 'Failed', object_name, 
                                                                                         self.bucket, upload_ending_time - upload_starting_time, 
                                                                                         '' if is_upload_succeed else ' due to {}'.format(error_log))
        
        print(upload_message + '\n\n')
        self.PostMessage(self.slack_channel, upload_message, 'Correct_Log_S3' if is_upload_succeed else 'Error_Log_S3')
        return is_upload_succeed

#=========================================================  Upload Object  ===================================================================

    def PutObjectToS3(self, body, object_name, timeout_second = 35):
        '''
        purpose                      : used for uploading object to s3
        param body                   : content in this object
        param object_name            : object name for storing in s3 (extension included e.g. test/hello.txt)
        param timeout_second         : limitation (seconds) for timeout default: 25 seconds
        return is_upload_succeed     : if object succeed to be uploaded                                                                                                             
        '''
        is_upload_succeed = False
        upload_starting_time = datetime.now()
        
        @func_set_timeout(timeout_second)
        def s3_object_uploader():
            '''
            purpose : upload object to s3 bucket
            '''
            self.s3_client.put_object(Bucket = self.bucket, Body = body, Key = object_name) 

        try:
            s3_object_uploader
            is_upload_succeed = True
            
        except FunctionTimedOut as E:
            error_log=str(E)
        
        except Exception as E:
            error_log=str(E)   
        
        upload_ending_time = datetime.now()
        upload_message = '{} to upload object to {} in bucket {} using time{}{}'.format('Succeeded' if is_upload_succeed else 'Failed', object_name, 
                                                                                         self.bucket, upload_ending_time - upload_starting_time, 
                                                                                         '' if is_upload_succeed else ' due to {}'.format(error_log))
        
        print(upload_message+'\n\n')
        self.PostMessage(self.slack_channel, upload_message, 'Correct_Log_S3' if is_upload_succeed else 'Error_Log_S3')
        return is_upload_succeed
    
#=========================================================  Delete Object  ====================================================================

    def DeleteObject(self,delete_object_name, timeout_second = 25):
        '''
        purpose                    : used for deleting object from s3 bucket
        param delete_object_name   : deleted object name
        param timeout_second       : limitation (seconds) for timeout
        return is_delete_succeed   : a booolean to present if DownloadFile process succeeds                  
        '''       
        # set up timeout upper bound : default 25 seconds
        @func_set_timeout(timeout_second)
        def s3_deleter():
            '''
            purpose : download file from s3 bucket
            '''
            self.s3_client.delete_object(Bucket = self.bucket, Key = delete_object_name)
        
        is_delete_succeed = False
        delete_starting_time = datetime.now()
        try:
            # set up timeout upper bound :15 seconds
            s3_deleter()
            is_delete_succeed = True
        
        except FunctionTimedOut as E:
            error_log=str(E)
        
        except Exception as E:
            error_log=str(E)
        
        delete_ending_time = datetime.now()
        delete_message = '{} to delete {} from bucket {} using time: {}{}'.format('Succeeded' if is_delete_succeed else 'Failed',
                                                                                          delete_object_name, self.bucket , 
                                                                                          delete_ending_time - delete_starting_time, 
                                                                                          '' if is_delete_succeed else ' due to {}'.format(error_log))
        
        print(delete_message + '\n\n')
        self.PostMessage(self.slack_channel, delete_message, 'Correct_Log_S3' if is_delete_succeed else 'Error_Log_S3')
        return is_delete_succeed
    

        