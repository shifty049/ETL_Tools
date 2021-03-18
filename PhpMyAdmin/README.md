# PhpMyAdminHandler</br></br>

## Purpose
### interact with Selenium for automating PhpMyAdmin process

## Install
```
1. Python3
2. selenium
3. FireFox
4. Geckodriver
```
***
## Dependency</br></br>      
- PhpMyAdmin.PhpMyAdminHandler.py - `automate sql query process on PhpMyAdmin  using information given in download_information_dictionary`</br></br>
    - S3.S3Handler.py - `used for interacting with s3 bucket`</br></br> 
        - Slack.SlackHandler.py - `used for interacting with Slack API`</br></br>     
            - Key_Management.KeyManagement.py - `used for key management`</br></br>     
                - .keys/key.txt - ```used for recording all keys```</br></br>

## Parameters</br></br>
>1. php_proxy     : proxy setting for boto3 chosen from ('AWS' / 'GCP' / 'CORP' / 'LOCAL' / None)
>2. s3_proxy      : proxy setting for boto3 chosen from ('AWS' / 'GCP' / 'CORP' / 'LOCAL')
>3. slack_proxy   : proxy setting for slack chosen from ('AWS' / 'GCP' / 'CORP' / 'LOCAL')
>4. slack_channel : slack channel for recording log


## Functions</br></br>


>FileHandler - `used for integrating Login(self) and SQLOperater(self)`
- parameter:
    - task_name :  task name for execution
- return : is_process_succeed (booolean)

>Login - `used for automatically logging PhpMyAdmin with bring basic authencation`      
- parameters:
    - timeout : setting timeout seconds with 15 seconds as default
- return : is_login_succeed (booolean)

>SQLOperater - `used for automatically operating on PhpMyAdmin and downloading file to local directory`
- parameters :
    - timeout : setting timeout seconds with 15 seconds as default
- return : is_operate_succeed (boolean)

```
# CALL MODULE
php = PhpMyAdminHandler(path_of_task, php_proxy = 'CORP', s3_proxy = 'CORP', slack_proxy = 'CORP', slack_channel = 'log-test')

# EXECUTE DOWNLOAD PROCESS TO DOWNLOAD QUERY RESULT AS CSV FILE
php.FileHandler(task_name)
```