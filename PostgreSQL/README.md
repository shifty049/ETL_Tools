# PostgreSQLHandler</br></br>

## Purpose
### interact with PostgreSQL

## Install
```
1. python3
2. psycopg2
3. pandas
```
***
## Dependency</br></br>      
- PostgreSQL.PostgreSQLHandler.py - `used for interacting with PostgreSQL`</br></br>   
    - Slack.SlackHandler.py - `used for interacting with Slack API`</br></br>     
        - Key_Management.KeyManagement.py - `used for key management`</br></br>     
            - .keys/key.txt - ```used for recording all keys```</br></br>

## Parameters</br></br>
>1. host            : server for connecting
>2. database        : database for connecting
>3. connect_timeout : timeout setting for connection with default = 15 seconds
>4. slack_proxy     : proxy setting for slack chosen from ('AWS' / 'GCP' / 'CORP' / 'LOCAL')
>5. slack_channel   : slack channel for recording log


## Functions</br></br>
>InsertInto - `insert data into table`
- parameter:
    - data   : data for inserting into table (type: dataframe)
    - table  : table name of inserted table (case sensitive)
    - schema : schama name of inserted table with default = 'public' (case sensitive)
- return : is_insert_succeed (boolean)

>Truncate - `truncate table`      
- parameters:
    - table  : table name of truncated table
    - schema : schama name of truncated table
- return : is_truncate_succeed (boolean)

>DataProcess - `process data for inserting into table`
- parameters:
    - data                 : data for processing
    - download_object_name : data type of processed data chosen from (dataframe / list)
    - timeout_second       : limitation (seconds) for timeout
- return : nested list

>ReadQueryAsDataFrame - `read sql query result as DataFrame`
- parameters:
    - query  : sql query
- return : is_read_succeed => if succeed to read sql query as DataFrame
           read_df => DataFrame of query result (only exists if is_read_succeed == True)

>CloseWork - `close connection to database`

- return : is_close_succeed (boolean)
