import sys
sys.path.append('..')
import pytz
import time
import requests
from Key_Management.KeyManagement import KeyManagement
from slack import WebClient
from datetime import datetime, timedelta
from urllib.parse import urlencode, quote

tw = pytz.timezone('Asia/Taipei')

class SlackHandler(KeyManagement):
    '''
    purpose                 : used for interacting with Slack API
    param slack_proxy       : proxy setting for slack chosen from ('AWS' / 'GCP' / 'CORP' / 'LOCAL')
    param slack_channel     : slack channel for recording log
    param slack_timeout     : timeout for slack connection
    '''
    
    def __init__(self, slack_proxy = 'CORP', slack_channel = 'log-test', slack_timeout = 10):
        '''
        param slack_proxy       : proxy setting for slack client chosen from 'AWS' / 'GCP' / 'CORP' / 'LOCAL' (use local settnig) with 'CORP' as default
        param slack_channel     : slack channel selected for recording lo
        param slack_channel     : slack channel for recording log with default = 'log-test'
        param slack_timeout     : timeout for slack connection with default = 10 seconds
        '''
        # inherit from KeyManagement
        super().__init__()  
          
        # slack token
        self.slack_token = self.key_dict['slack']['slack_token']
        
        # set slack channel
        self.slack_channel = slack_channel
   
        # default proxy is using local setting
        self.proxies = {}
        
        if slack_proxy != 'LOCAL':
            proxy = 'http://{}:{}'.format(self.key_dict['proxy'][slack_proxy]['host'], self.key_dict['proxy'][slack_proxy]['port'])
            self.proxies['http'] = proxy
            self.proxies['https'] = proxy
        
        # original bot name
        self.bot_name = requests.get('https://slack.com/api/bots.info?token={}&bot={}'.format(self.slack_token, self.key_dict['slack']['bot']), 
                                        proxies = self.proxies).json()['bot']['name']
    
    def PostMessage(self, channel, message, username = None, stopword = 'Correct'):
        '''
        purpose        : used for posting message to the assigned channel
        param channel  : assigned channel for sending message
        param message  : message sent to channel
        param username : assigned bot name for sending message. if not specific, use original bot name
        param stopword : if username contains stopword  then => do not post message
        '''    

        if (not stopword) or (stopword and stopword not in (self.bot_name if not username else username)):
            is_post_succeed = False
            
            try:
                endcode_condition = urlencode({'token': self.slack_token, 'channel': channel, 'text': message, 'username': self.bot_name if not username else username})
                result = requests.get('https://slack.com/api/chat.postMessage?{}'.format(endcode_condition), proxies = self.proxies).json()
                if result['ok']:
                    is_post_succeed = True
                else:
                    error_log = result['error']
            except Exception as E:
                error_log = str(E)
        
            if not is_post_succeed:
                text = 'error arise form posting message to {} as username: {} due to {}'.format(channel, username, error_log)
                endcode_condition = urlencode({'token': self.slack_token, 'channel': 'log-test', 'text': text, 'username': 'Error_Log_Slack'})
                requests.get('https://slack.com/api/chat.postMessage?{}'.format(endcode_condition), proxies = self.proxies)
    
    def DeleteMessage(self, channel, day_bound = 3):
        '''
        purpose                  : used for deleteing all messages in the assigned channel {day_bound} days before current timestamp
        param channel            : assigned channel for deleteing all messages
        param day_bound          : delete messages created how many days ago with default = 3 which means delete messages 3 days ago before current timestamp
                                   the minimal day_bound = 0 which means delete all messages before current timestamp
        return is_delete_succeed : (boolean)
        '''

        is_delete_succeed = True
        cant_delete_count = 0
        delete_starting_time = datetime.now()
        
        try:
            # for public channel
            channel_public = requests.get('https://slack.com/api/conversations.list?token={}&limit=1000&types=public_channel'.format(self.slack_token), proxies = self.proxies).json()['channels']
            
            # for private channel
            channel_private = requests.get('https://slack.com/api/conversations.list?token={}&limit=10000&types=private_channel'.format(self.slack_token), proxies = self.proxies).json()['channels']
            
            channel_info_dict = {channel['name']: channel['id'] for channel in channel_public + channel_private}
            
            message_list =  requests.get('https://slack.com/api/conversations.history?token={}&channel={}&limit=1000'.format(self.slack_token, channel_info_dict[channel]), proxies = self.proxies).json()['messages']
            
            
            # filter for deleted messages using Taiwan timezone as standard
            delete_upper_bound_time = delete_starting_time.astimezone(tw) - timedelta(days = day_bound)
            delete_upper_bound_str = str(time.mktime(delete_upper_bound_time.timetuple()))
            message_timestamp_list = list(map(lambda x: x['ts'],list(filter(lambda x:x['ts'] < delete_upper_bound_str, message_list))))
            
            # number of request
            count = 0
            for message_timestamp in message_timestamp_list:
                delete_result = requests.get('https://slack.com/api/chat.delete?token={}&channel={}&ts={}'.format(self.slack_token, channel_info_dict[channel], message_timestamp), proxies = self.proxies).json()
                       
                if not delete_result['ok']:
                    # cant_delete_message is involved in authorization
                    if delete_result['error'] != 'cant_delete_message':
                        is_delete_succeed = delete_result['ok']                  
                        error_log = delete_result['error']
                        break
                    else:
                        cant_delete_count += 1
                
                count += 1
                # for every 50 times of request => sleep 40 seconds
                if not count % 50:
                    time.sleep(60)
       
        except Exception as E:
            error_log = str(E)
            is_delete_succeed = False

        delete_ending_time = datetime.now()
        message = '{} to delete all messages in channel {} with amount: {}{}{}{} using time: {}'.format('Succeeded' if is_delete_succeed else 'Failed', channel, len(message_timestamp_list) - cant_delete_count, 
                                            '' if not cant_delete_count else ' and {} message{} did not get authorized for deleting'.format(cant_delete_count, '' if cant_delete_count == 1 else 's'), 
                                            '' if day_bound == 0 else ' {} day{} before current timestamp (i.e. deleting time upper bound = {})'.format(day_bound, '' if day_bound == 1 else 's', delete_upper_bound_time.strftime('%Y-%m-%d %H:%M:%S %Z%z')), 
                                            '' if is_delete_succeed else ' due to {}'.format(error_log),delete_ending_time - delete_starting_time)
        
        print(message +'\n\n')

        self.PostMessage(self.slack_channel, message, '{}_Log_Slack'.format('Correct' if is_delete_succeed else 'Error'), stopword = None)

        return is_delete_succeed
    
    def ScheduleMessage(self, text_content, posted_channel, unix_timestamp):
        '''
        purpose             : used for schedule message posting at assigned channel
        param text_content  : text for posting in assigned channel
        param posted_channel: assigned channel for posting
        param unix_timestamp: unix timestamp for posting messages
        return is_schedule_succeed (boolean)
        '''
        
        is_schedule_succeed = False
        starting_time = datetime.now()
        
        headers = {'content': 'application/json', 
                'Authorization': 'Bearer {}'.format(self.slack_token)
                }
        
        try:
            endcode_condition = urlencode({'channel': posted_channel, 'post_at': unix_timestamp, 'text': text_content})
            result= requests.post('https://slack.com/api/chat.scheduleMessage?{}'.format(endcode_condition), headers = headers, proxies = self.proxies).json()
            
            if result['ok']:
                is_schedule_succeed = True
            else:
                error_log = result['error']
        except Exception as E:
            error_log = str(E)
        
        ending_time = datetime.now() 
        message = '{} to schedule posting message: {} in channel: {} at unix timestamp: {}{} using time: {}'.format('Succeeded' if is_schedule_succeed else 'Failed', 
                                                                                                                    text_content, 
                                                                                                                    posted_channel, 
                                                                                                                    unix_timestamp, 
                                                                                                                    '' if is_schedule_succeed else ' due to {}'.format(error_log), 
                                                                                                                    ending_time - starting_time)
        
        print(message +'\n\n')

        self.PostMessage(self.slack_channel, message, '{}_Log_Slack'.format('Correct' if is_schedule_succeed else 'Error'))

        return is_schedule_succeed




