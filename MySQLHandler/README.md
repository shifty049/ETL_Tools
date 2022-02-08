# MySQLHandler</br></br>

## Purpose
### interact with MySQL

## Install
```
1. python3
2. mysql-connector-python
3. pandas
```
***
## Dependency</br></br>      
- MySQLHandler.MySQLHandler.py - `used for interacting with MySQL`</br></br>   
    - Slack.SlackHandler.py - `used for interacting with Slack API`</br></br>     
        - Key_Management.KeyManagement.py - `used for key management`</br></br>     
            - .keys/key.txt - `used for recording all keys`</br></br>

## Parameters</br></br>
>1. host            : server for connecting
>2. connect_timeout : timeout setting for connection with default = 15 seconds
>3. slack_proxy     : proxy setting for slack chosen from ('AWS' / 'GCP' / 'CORP' / 'LOCAL')
>4. slack_channel   : slack channel for recording log
>5. chaeset         : (optional) selected charset for connection
>6. sl_disabled     : (optional) option for deciding whether to disable SSL/TLS usage or not; if not assigned, the default value is False
>7. database        : (optional) database for connecting

## Functions</br></br>
>ReadQueryAsDataFrame - `read sql query result as DataFrame`
- parameters :
    - query  : sql query
- return : is_read_succeed => if succeed to read sql query as DataFrame
           read_df => DataFrame of query result (only exists if is_read_succeed == True)

>ReadQueryAsList - `read sql query result as List`
- parameters:
    - query   : sql query
- return : is_read_succeed => if succeed to read sql query as DataFrame
           read_lst => list of query result (type: list, only exists if is_read_succeed == True)

>Truncate - `truncate table`      
- parameters :
    - table  : table name of truncated table
    - schema : schama name of truncated table
- return : is_truncate_succeed (boolean)

>DataProcess - `process data for inserting into table`
- parameters :
    - data                 : data for processing
    - download_object_name : data type of processed data chosen from (dataframe / list)
    - timeout_second       : limitation (seconds) for timeout
- return : nested list

>InsertInto - `insert data into table`
- parameters :
    - data              : data for inserting into table (type: dataframe)
    - table             : table name of inserted table (case sensitive)
    - data_type         : type of data inserted into table (type chosen from dataframe / list with default = lst)
    - batch_row         : number of rows split for each round of table inserting with default = 5000
    - is_need_process   : check if need to procees inserted data with default = False 
    - schema            : schama name of inserted table with default = Data_ETL (case sensitive)
    - is_neeed_process  : check if need to procees inserted data with default = True
- return : is_insert_succeed (boolean)

>DeleteFrom - `delete data from table`
- parameters:
    - query : query for executing deleting action in MySQL
- return : is_delete_succeed (boolean)

>RetrieveTableInformation - `retrieve table information of assigned database`
- parameters:
    - database : (optional) database assigned for retrieving information, if not assigned, use self.database as default
- return : return dict including 1. (required) is_process_succeed (boolean) 2. (optional) return_data (dictionary) exists only if is_process_succeed == True

>CloseWork - `close connection to database`
- return : is_close_succeed (boolean)