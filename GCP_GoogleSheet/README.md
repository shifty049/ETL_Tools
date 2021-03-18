# GoogleSheetHandler</br></br>

## Purpose
### interact with GoogleSheet API

## Install
```
1. Python3
2. pandas
3. gspread
4. oauth2client
```
***
## Dependency</br></br>      
- GoogleSheet.GoogleSheetHandler.py - `used for interacting with GoogleSheet API`</br></br>
    - Slack.SlackHandler.py - `used for interacting with Slack API`</br></br>     
        - Key_Management.KeyManagement.py - `used for key management`</br></br>     
            - .keys/key.txt - ```used for recording all keys```</br></br>

## Parameters</br></br>
>1. spreadsheet_key    : key of google sheet
>2. sheet_name         : name of google sheet
>3. gs_proxy           : proxy setting for gspread chosen from >('AWS' / 'GCP' / 'CORP' / 'LOCAL' / None)
>4. slack_proxy        : proxy setting for slack chosen from ('AWS' / 'GCP' / 'CORP' / 'LOCAL')
>5. slack_channel      : slack channel for recording log 

## Functions</br></br>


>ReadAsDataFrame - `used for reading GoogleSheet as DataFrame format`
- return : read_dict => {is_reaf_succeed : boolean, read_df : DataFrame (only exists if is_reaf_succeed == True)}

>UpdateCell - `used for updating content at assigned cell`
- parameters :
    - cell_location         : assigned cell location to be updatd e.g "A1", "D2" and so on
    - param updated_content : updated content for assigned cell
- return : is_update_succeed (boolean)

>UpdateCellByRange - `used for updating GoogleSheets content by range`
- parameters :
    - sheet_range              : sheet range to be updated
    - param updated_value_list : nested list containing values for updating
- return : is_update_succeed (boolean)

>ClearProxy - `used for clearing proxy setting`
- return : is_clear_proxy_succeed (boolean)

>ClearSheetByRange - `used for clearing assigned googlesheet by range provided`
- parameters :
    - range_to_clear            : range of assigned GoogleSheet for clearing
- return : is_clear_succeed (boolean)

## Sample Code</br></br>
```
# CALL MODULE
gs = GoogleSheetHandler(gspread_key_name, gs_proxy = 'CORP', gs_timeout = 10, slack_proxy = 'CORP', slack_channel = 'log-test')

# READ GOOGLESHEET AS DATAFRAME
gs.ReadAsDataFrame()

# UPDATE SINGLE CELL CONTENT
gs.UpdateCell(cell_location, updated_content)

# UPDATE CELL CONTENT BY RANGE
gs.UpdateCellByRange(sheet_range, updated_value_list)

# CLEAR PROXY
gs.ClearProxy()
```