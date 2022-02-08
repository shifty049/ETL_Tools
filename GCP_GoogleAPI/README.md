# GoogleAPIHandler</br></br>

## Purpose
### interact with Google API

## Install(Optional)
```
1. Python3.7
2. google-api-python-client
3. oauth2client
```
***
## Dependency</br></br>      
- GCP_GoogleAPI.GoogleAPI.py - `used for interacting with Google API`</br></br>
    - Slack.SlackHandler.py - `used for interacting with Slack API`</br></br>     
        - Key_Management.KeyManagement.py - `used for key management`</br></br>     
            - .keys/key.txt - ```used for recording all keys```</br></br>

## Parameters</br></br>
>1. param api_service_name (Requird)        : service name of Google API you want to call with default = analytics, which means calling Google Analytics API
>2. param cred_dict_id (Required)           : id of credential dictionary for calling API recorded in key file
>3. param google_proxy (Required)           : proxy setting for google api chosen from ('AWS' / 'GCP' / 'CORP' / 'LOCAL')
>4. param slack_proxy (Required)            : slack proxy chosen from AWS /GCP / CORP or None (not setting proxy) with default = 'CORP'
>5. param slack_channel (Required)          : slack channel for recording log with default = 'log-test'
>6. param custom_dataset_dict_id (Optional) : id of custom dataset dictionary recorded in key file necessary only if need to manipulate custom data API
>7. param api_version (Optional)            : version of Google API with default = v3 if not assigned any

## Functions</br></br>

>ListCustomData - `Google Analytics custom data API) used for listing all custom datasources of assigned custom dataset`
- parameters :
    - max_result     : (Optional) maximal number of upload custom data per page with default = 1000
    - timeout_second : (Optional) timeout seconds with default = 60
- return : return_dict (type : dictionary) => (Required) is_process_succeed (type : boolean),  (Optional) return_data (type : list) only exists if is_process_succeed == True

>UploadCustomData - `(Google Analytics custom data API) used for update custom data file to assigned custom dataset`
- parameters :
    - custom_data_file_path : (Required) local file path of csv file to be uploaded
    - timeout_second        : (Optional) timeout seconds with default = 240
- return : is_process_succeed (type : boolean)

>DeleteUploadCustomData - `(Google Analytics custom data API) used for delete upload custom data file in assigned custom dataset`
- parameters :
    - delete_id_lst       : (Required)list of data id for all data to be deleted
    - timeout_second      : (Optional) timeout seconds with default = 60
- return is_process_succeed : (type : boolean)

>ListGoogleDriveFile - `(Google Drive API) used for listing information of all files on Google Drive`
- parameters :
    - param page_size      : (Optional) maximum number of files to be returned per page from API chosen from 1 - 1000 with default = 1000
- return : return_dict (type : dictionary) => (Required) is_process_succeed (type : boolean), (Optional) return_data (type : list) only exists if is_process_succeed == True
 
>ReadGoogleDriveCSVFile - `(Google Drive API) used for listing information of all files on Google Drive`
- parameters :
    - file_id        : (Required)id of file to be read
    - return_type    : (Optional) type of data to be return chosen from list / text with default = list
    - encoding       : (Optional) type of encoding chosen for text from request with default = ISO-8859-1 if not assigned
- return : return_dict (type : dictionary) => (Required) is_process_succeed (type : boolean), (Optional) return_data (type : {{return_type}}) only exists if is_process_succeed == True

>ClearProxy - `used for clearing proxy setting`
- return : is_process_succeed (boolean)

## Sample Code</br></br>
```
# Call Module for calling Google Analytics API
from GCP.GoogleAPI.GoogleAPI import GoogleAPIHandler
gah = GoogleAPIHandler(api_service_name = 'analytics', cred_dict_id = 'cic-service-ga360', custom_dataset_dict_id = 'rfm_model')

# Call Module for calling Google Drive API
from GCP.GoogleAPI.GoogleAPI import GoogleAPIHandler
gah = GoogleAPIHandler(api_service_name = 'drive', cred_dict_id = 'google-drive-service')

# List Custom Data
gah.ListCustomData()

# Upload Custom Data
gah.UploadCustomData(csv_file_path)

# Delete Uploaded Custom Data
gah.DeleteUploadCustomData(delete_id_lst = id_to_delete_lst)

# List All Google Drive Files
gah.ListGoogleDriveFile()

# Read Google Drive CSV file As List
gah.ReadGoogleDriveCSVFile(file_id)

# Clear Proxy
gah.ClearProxy()
```