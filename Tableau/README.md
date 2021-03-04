# TableauHandler</br></br>

## Purpose
### interact with tableau server

## Install
```
1. Python3
2. tableauserverclient
```
***
## Dependency</br></br>      
- Tableau.TableauHandler.py - `used for downloading file from tableau server to local`</br></br>
    - Key_Management.KeyManagement.py - `used for key management`</br></br>
        - .keys/key.txt - `used for recording all keys`
***

## Parameters</br></br>
>1. s3_proxy      : proxy setting for boto3 chosen from ('AWS' / 'GCP' / 'CORP' / 'LOCAL')
>2. slack_proxy   : proxy setting for slack chosen from ('AWS' / 'GCP' / 'CORP' / 'LOCAL')
>3. slack_channel : slack channel for recording log

## Functions</br></br>

>Login - `used for logging in tableau server`
 - return : is_login_succeed (boolean)

>DownloadByViewId - `used for downloading view with view_id given from tableau server to local directory`      

- parameters :
    - view_id    : view id for downloading
    - file_name  : setting local file name
    - local_path : setting local file directory
    - file_type  : setting file type (default csv)
- return : is_download_succeed (boolean)

>Logout - `used for logging out tableau server`
 - return : is_logout_succeed (boolean)