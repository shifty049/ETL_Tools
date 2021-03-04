# S3Handler</br></br>

## Purpose
### interact with AWS S3

## Install
```
1. Python3
2. boto3
3. func_timeout
4. idna
5. idna_ssl
```
***
## Dependency</br></br>      
- S3.S3Handler.py - `used for interacting with s3 bucket`</br></br>   
    - Slack.SlackHandler.py - `used for interacting with Slack API`</br></br>     
        - Key_Management.KeyManagement.py - `used for key management`</br></br>     
            - .keys/key.txt - ```used for recording all keys```</br></br>

## Parameters</br></br>
>1. s3_proxy      : proxy setting for boto3 chosen from ('AWS' / 'GCP' / 'CORP' / 'LOCAL')
>2. slack_proxy   : proxy setting for slack chosen from ('AWS' / 'GCP' / 'CORP' / 'LOCAL')
>3. slack_channel : slack channel for recording log

## Functions</br></br>


>ListObjects - `used for listing all objects in S3 bucket (under a specific directory)`
- parameter:
    - directory       : directory of bucket (if None => list all objects in bucket else => list all objects under directory) 
    - timeout_second  : limitation (seconds) for timeout
- return : list_dict  : is_list_object_succeed => a booolean to present if ListAllObjects process succeeds
                        object_list            => object list for a specific bucket under(specific directory) only exists if is_list_object_succeed == True

>UploadFile - `used for uploading file to s3 bucket`      
- parameters:
    - upload_file_path    : local path of uploaded file
    - upload_object_name  : object name of uploaded file in s3
    - ltimeout_second     : limitation (seconds) for timeout
- return : is_upload_succeed (booolean)

>DownloadFile - `used for downloading file from s3 bucket`
- parameters :
    - download_file_path   : local path of downloaded file
    - download_object_name : object name of downloaded name in s3
    - timeout_second       : limitation (seconds) for timeout
- return : is_download_succeed (boolean)

>ReadCSVFromS3AsDF - `used for reading csv file from s3 as DataFrame`
- parameters :
    - object_name          : object name of read object in s3
    - timeout_second       : limitation (seconds) for timeout
- return : return_dict => a dictionary containing 
    1. is_read_succeed (boolean)
    2. read_df (dataframe) only if is_read_succeed == True)

>UploadCSVToS3FromDF - `used for uploading DataFrame to s3 bucket as csv file`
- parameters :
    - object_name          : name of uploaded object in s3
    - timeout_second       : limitation (seconds) for timeout
- return : is_upload_succeed (boolean)

>PutObjectToS3 - `used for uploading object to s3`
- parameters :
    - body                 : content of uploaded object 
    - object_name          : name of uploaded object
    - timeout_second       : limitation (seconds) for timeout
- return : is_upload_succeed (boolean)

>DeleteObject - `used for deleting object from s3 bucket`
- parameters :
    - delete_object_name   : name of deleted object in s3
    - timeout_second       : limitation (seconds) for timeout
- return : is_delete_succeed (boolean)