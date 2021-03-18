# ExchangeHandler</br></br>

## Purpose
### used for automatically handling emails on exchange mail server

## Install
```
1. Python3
2. exchangelib
```
***
## Dependency</br></br>      
- Exchange.ExchangeHandler.py - `used for interacting with BigQuery`</br></br>
    - Slack.SlackHandler.py - `used for interacting with Slack API`</br></br>     
        - Key_Management.KeyManagement.py - `used for key management`</br></br>     
            - .keys/key.txt - ```used for recording all keys```</br></br>

## Parameters</br></br>
>1. slack_proxy        : proxy setting for slack chosen from ('AWS' / 'GCP' / 'CORP' / 'LOCAL')
>2. slack_channel      : slack channel for recording log 

## Functions</br></br>
>SendMail - `used for sending mails with html body, embeded images and attachments`      
- parameters :
    - recipient_lst : recipient list of this mail (required)
    - subject : subject of this mail (required)
    - body : body of email for delivery (required)
    - cc_recipient_lst : carbon copy recipient list of this mail with default = empty list
    - is_html_body : if body is html format or not with default = True
    - embeded_image_lst : image path lst of images embeded in html body (must be in the same directory as python script)
    - attachment_dic : attachment_dic = {attach_name_1: attach_file_path_1, attach_name_2: attach_file_path_2}
- return : is_send_mail_succeed (boolean)

## Sample Code</br></br>
```
# SEND MAIL
exchange = ExchangeMailHandler()

if exchange.is_connect_succeed:
    exchange.SendMail(recipient_lst = recipient_list, 
                      subject = email_subject, 
                      body = email_body, 
                      cc_recipient_lst = cc_recipient_list,
                      is_html_body = True, 
                      embeded_image_lst = embeded_image_path_list))
```