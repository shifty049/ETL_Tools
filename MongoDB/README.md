# MongoDBHandler</br></br>

## Purpose
### interact with MongoDB

## Install
```
1. python3
2. pymongo
3. pandas
```
***
## Dependency</br></br>      
- MongoDB.MongoDBHandler.py - `used for interacting with MongoDB`</br></br>   
    - Slack.SlackHandler.py - `used for interacting with Slack API`</br></br>     
        - Key_Management.KeyManagement.py - `used for key management`</br></br>     
            - .keys/key.txt - ```used for recording all keys```</br></br>

## Parameters</br></br>
>1. host_name       : host for connection
>2. timeout         : timeout setting for connection with default = 15 seconds
>3. slack_proxy     : proxy setting for slack chosen from ('AWS' / 'GCP' / 'CORP' / 'LOCAL')
>4. slack_channel   : slack channel for recording log


## Functions</br></br>

>RetrieveData - `filter or aggregrate data from collection and return result  as DataFrame format`
- parameters:
    - db_name               : database name
    - collection_name       : collection name
    - by                    : way for retrieving data chosen from find / aggregrate with default = find
    - condition_setting     : setting for retrieving data with default == None which means retrieving all data
- return : is_read_succeed (boolean) 
           read_df (DataFrame) only exists if is_read_succeed == True

>CloseWork - `close connection to database`

- return : is_close_succeed (boolean)