from Slack.SlackHandler import SlackHandler
from exchangelib import DELEGATE, Account, Credentials, Message, Mailbox, FileAttachment,HTMLBody
from datetime import datetime

class ExchangeMailHandler(SlackHandler):
    '''
    purpose                  : used for automatically handling emails on exchange mail server
    param slack_proxy        : slack proxy chosen from AWS /GCP / CORP or LOCAL (not setting proxy) with default = 'GCP'
    param slack_channel      : slack channel for recording log with default = 'log-test'
    '''
    def __init__(self, slack_proxy = 'GCP', slack_channel = 'log-test'):
        '''
        purpose                  : automatically handle emails on exchange mail server
        param slack_proxy        : slack proxy chosen from AWS /GCP / CORP or LOCAL (not setting proxy) with default = 'GCP'
        param slack_channel      : slack channel for recording log with default = 'log-test'
        '''
        # change initial slack proxy setting and channel of SlackHandler    
        super().__init__(slack_proxy, slack_channel)    
        
        self.is_connect_succeed = False
        starting_time = datetime.now()
        
        for account_info in self.key_dict['exchange_account']:
            
            try:

                self.account_username = self.key_dict['exchange_account'][account_info]['username']
                account_password = self.key_dict['exchange_account'][account_info]['password']

                credentials = Credentials(
                    username = self.account_username,
                    password = account_password
                )
                self.account = Account(
                    primary_smtp_address = self.account_username, 
                    credentials = credentials, 
                    autodiscover = True, 
                    access_type = DELEGATE
                )

                self.is_connect_succeed = True

            except Exception as E:
                print(E)
                error_log = str(E)
        
            if  self.is_connect_succeed:          
                break
        
        ending_time = datetime.now()
        
        message = '{} to connect to exchange account: {} with login information: {}{} using time: {}'.format('Succeeded' if self.is_connect_succeed else 'Failed', self.account_username, 
                                                                                  account_info, '' if self.is_connect_succeed else ' due to {}'.format(error_log), 
                                                                                  ending_time - starting_time)
        print(message + '\n\n')
        
        self.PostMessage(self.slack_channel, message, '{}_Log_Exchange'.format('Correct' if self.is_connect_succeed else 'Error'))
    
    def SendMail(self, recipient_lst, subject, body, cc_recipient_lst = [], is_html_body = True, embeded_image_lst = [], attachment_dic = {}):
        '''
        purpose                  : used for sending mails with html body, embeded images and attachments
        param recipient_lst      : recipient list of this mail (required)
        param subject            : subject of this mail (required)
        param body               : body of email for delivery (required)
        param cc_recipient_lst   : carbon copy recipient list of this mail with default = empty list
        param is_html_body       : if body is html format or not with default = True
        param embeded_image_lst  : image path lst of images embeded in html body (must be in the same directory as python script)
        param attachment_dic     : attachement path dictionary for attachments setting of this email e.g. attachment_dic = {attach_name_1: attach_file_path_1, attach_name_2: attach_file_path_2}
        return is_send_mail_succeed(boolean)
        '''

        starting_time = datetime.now()

        is_send_mail_succeed = False

        try:

            if is_html_body:
                body = HTMLBody(body) 

            m = Message(account = self.account,
                folder = self.account.sent,      
                subject= subject,      
                body = body,
                to_recipients = [Mailbox(email_address = recipient) for recipient in recipient_lst],
                cc_recipients = [Mailbox(email_address = recipient) for recipient in cc_recipient_lst])

            # embeded images setting
            for image_path in embeded_image_lst:

                m.attach(FileAttachment(name = image_path, 
                               content = open(image_path, 'rb').read(), 
                               is_inline = True,
                               content_type = 'GIF/Image',
                               content_id = image_path))

            # attachments setting
            for attachment_name, attachment_path in attachment_dic.items():

                m.attach(FileAttachment(name = attachment_name, 
                                        content = open(attachment_path, 'rb').read()))
            
            m.send_and_save()

            is_send_mail_succeed = True

        except Exception as E:

            error_log = str(E)

        ending_time = datetime.now()

        message = '{} to send mail from {} to recipient list: {}{}{} using time: {}'.format('Succeeded' if is_send_mail_succeed else 'Failed', 
                                                                                                 self.account_username, 
                                                                                                 recipient_lst, 
                                                                                                 '' if not cc_recipient_lst else ' with carbon copy to {}'.format(cc_recipient_lst), 
                                                                                                 '' if is_send_mail_succeed else ' due to {}'.format(error_log), 
                                                                                                 ending_time - starting_time)

        print(message +'\n\n')
        self.PostMessage(self.slack_channel, message, '{}_Log_Exchange'.format('Correct' if self.is_connect_succeed else 'Error'))
        
        return is_send_mail_succeed