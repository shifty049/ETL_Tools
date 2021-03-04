# SlackHandler</br></br>

## Purpose
### interact with Slack API

## Install
```
1. Python3
2. slackclient
3. idna
4. idna_ssl
```
***
## Dependency</br></br>      
- Slack.SlackHandler.py - `used for interacting with Slack API`</br></br>   
    - Key_Management.KeyManagement.py - `used for key management`</br></br>     
      - .keys/key.txt - ```used for recording all keys```</br></br>

## Parameters</br></br>
>1. slack_proxy : proxy setting for slack chosen from ('AWS' / 'GCP' / 'CORP' / 'LOCAL')
>2. slack_channel    : slack channel for recording log
>3. slack_timeout    : timeout for slack connection
>4. slack_connect_by : connection method chosen from request / library

## Functions</br></br>

>PostMessage - `used for posting message to the assigned channel`
- parameter:
    - channel  : assigned channel for sending message
    - message  : message sent to channel
    - username : assigned bot name for sending message. if not specific, use original bot name

>DeleteMessage - `used for deleteing all messages in the assigned channel {day_bound} days before current timestamp`
- parameter :
    - channel   : assigned channel for deleteing all messages
    - day_bound : delete messages created how many days ago with default = 3 which means delete messages 3 days ago before current timestamp
- return : is_delete_succeed (boolean)