# BigQueryHandler</br></br>

## Purpose
### interact with BigQuery

## Install
```
1. Python3
2. pandas
3. google-cloud-bigquery
```
***
## Dependency</br></br>      
- BigQuery.BigQueryHandler.py - `used for interacting with BigQuery`</br></br>
    - S3.S3Handler.py - `used for interacting with s3 bucket`</br></br> 
        - Slack.SlackHandler.py - `used for interacting with Slack API`</br></br>     
            - Key_Management.KeyManagement.py - `used for key management`</br></br>     
                - .keys/key.txt - ```used for recording all keys```</br></br>

## Parameters</br></br>
>1. bigquery_proxy     : proxy setting for bigquery chosen from >('AWS' / 'GCP' / 'CORP' / 'LOCAL' / None)
>2. s3_proxy           : proxy setting for boto3 chosen from ('AWS' / 'GCP' / 'CORP' / 'LOCAL')
>3. slack_proxy        : proxy setting for slack chosen from ('AWS' / 'GCP' / 'CORP' / 'LOCAL')
>4. slack_channel      : slack channel for recording log 

## Functions</br></br>

>ReadBigQueryAsDF - `used for reading query result as DataFrame`
- parameters :
    - query : query for executing in BigQuery
- return : result_dict => {is_reaf_succeed : boolean, read_df : DataFrame (only exists if is_read_succeed == True)}

>ReadBigQueryAsObject - `used for reading query result as Object`
- parameters :
    - query : query for executing in BigQuery
- return : result_dict => {is_reaf_succeed : boolean, read_object : object (only exists if is_read_succeed == True)}

>ClearProxy - `used for clearing BigQuery proxy setting`      
- return : is_clear_proxy_succeed (boolean)

>CheckIfTableExists - `used for checking if table exists or not`      
- parameters :
    - table_id : checked table composed of project_name.dataset_name.table_name
- return : is_table_exist (boolean)

>InsertIntoTableFromDataFrame - `used for inserting DataFrame into table`      
- parameters :
    - df : DataFrame for inserting into table
    - table_id : BigQuery table for inserting into
    - job_config : schema setting config


- return : is_insert_succeed (boolean)

>DeleteFromTable - `used for deleting data from table in BigQuery`      
- parameters :
    - query : query for executing in BigQuery
- return : is_delete_succeed (boolean)

## Sample Code</br></br>
```
# CALL MODULE
bqh = BigQueryHandler(bigquery_proxy = 'CORP', s3_proxy = 'CORP', slack_proxy = 'CORP', slack_channel = 'log-test')

# CHECK IF TABLE EXISTS
bqh.CheckIfTableExists(table_id)

# READ QUERY AS DATAFRAME
bqh.ReadBigQueryAsDF(select_query)

# DELETE FROM TABLE
bqh.DeleteFromTable(delete_query)

# INSERT INTO TABLE
bqh.InsertIntoTableFromDataFrame(df, table_id, job_config)

# CLEAR PROXY
bqh.ClearProxy()
```