# AzureBlobHandler</br></br>

## Purpose
### used for interacting with Azure Blob

## Install
```
1. Python3
2. azure-storage-blob
3. pandas
```
***
## Dependency</br></br>      
- AzureBlobHandler.AzureBlobHandler.py - `used for interacting with Azure Blob`</br></br>
    - Slack.SlackHandler.py - `used for interacting with Slack API`</br></br>     
        - Key_Management.KeyManagement.py - `used for key management`</br></br>     
            - .keys/key.txt - ```used for recording all keys```</br></br>

## Parameters</br></br>
azure_account, blob_proxy= 'CORP'
>1. azure_account      : Azure account name
>2. blob_proxy         : proxy setting for Azure Blob chosen from ('AWS' / 'GCP' / 'CORP' / 'LOCAL')
>3. slack_proxy        : proxy setting for slack chosen from ('AWS' / 'GCP' / 'CORP' / 'LOCAL')
>4. slack_channel      : slack channel for recording log 

## Functions</br></br>
UploadFileASBlob
>UploadFileASBlob - `used for uploading file as Blob`
- parameters :
    - upload_file_path : absolute path of uploaded file
    - container_name   : container chosen to be uploaded
    - blob_name        : Blob name (extension included) of uploaded file
    - if_overwrite     : decide whether to overwrite the blob file already existing in container with the same name as the uploaded file. set default = True
- return : is_upload_succeed (boolean)

>DeleteBlob - `used for uploading file as Blob`
- parameters :
    - upload_file_path : used for deleting Blob file
    - container_name   : container of blob chosen to be deleted
    - blob_name        : Blob name (extension included) to be deleted
- return : is_delete_succeed (boolean)

>ReadCSVAsDF - `used for uploading file as Blob`
- parameters :
    - upload_file_path : used for reading a csv-format Blob as DataFrame
    - container_name   : container of blob chosen to be read
    - blob_name        : Blob name (extension included) to be read (must be csv format)
- return : result_dict (dictionary)=> {'is_read_succeed': boolean (required), 'read_df': dataframe (only exists when is_read_succeed == True)}

## Sample Code</br></br>
```
# CALL MODULE
abh = AzureBlobHandler(account_name)

# UPLOAD
abh.UploadFileASBlob(upload_file_path, container_name, blob_name)

# DELETE
abh.DeleteBlob(container_name, blob_name)

# READ AS DATAFRAME
abh.ReadCSVAsDF(container_name, blob_name)
```