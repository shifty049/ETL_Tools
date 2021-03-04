import sys
sys.path.append('..')
from selenium import webdriver
from selenium.webdriver.support.ui import Select
from selenium.webdriver.common.action_chains import ActionChains
from selenium.webdriver.common.keys import Keys
from selenium.webdriver.firefox.options import Options
from selenium.webdriver.support.wait import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.common.by import By
from selenium.common.exceptions import TimeoutException
from S3.S3Handler import S3Handler
from datetime import datetime
import os
import json
import time

class PhpMyAdminHandler(S3Handler):
    '''
    purpose             : automate sql query process on PhpMyAdmin  using information given in download_information_dictionary
    param php_proxy     : proxy setting for boto3 chosen from ('AWS' / 'GCP' / 'CORP' / 'LOCAL' / None)
    param s3_proxy      : proxy setting for boto3 chosen from ('AWS' / 'GCP' / 'CORP' / 'LOCAL')
    param slack_proxy   : proxy setting for slack chosen from ('AWS' / 'GCP' / 'CORP' / 'LOCAL')
    param slack_channel : slack channel for recording log 
    '''
    
    def __init__(self, path_of_task, php_proxy = 'CORP', s3_proxy = 'CORP', slack_proxy = 'CORP', slack_channel = 'log-test'):
        '''
        param path_of_task  : path of task file for execution
        param php_proxy     : proxy setting for logging in PhpMyAdmin chosen from 'AWS' / 'GCP' / 'CORP' / 'LOCAL' (use local settnig) or None (No Proxy) with default = 'CORP'
        param s3_proxy      : S3 proxy chosen from AWS /GCP / CORP or None (not setting proxy) with default = 'CORP'
        param slack_proxy   : Slack proxy chosen from AWS /GCP / CORP or None (not setting proxy) with default = 'CORP'
        param slack_channel : slack channel for recording log with default = 'log-test'
        '''
        
        starting_time = datetime.now()
        self.is_driver_launch_succeed = False
        
        try:
            # change initial slack proxy setting of SlackHandler & s3 proxy setting of S3Handler       
            super().__init__(s3_proxy, slack_proxy, slack_channel)
        
            # read task file
            with open(path_of_task,'r') as file:
                self.task_information_dictionary = json.load(file)

            # modify option
            options = Options()
            
            # set headless mode
            options.headless = True
            
            # modify profile
            profile = webdriver.FirefoxProfile()

            # not use proxy
            if not php_proxy:
                profile.set_preference("network.proxy.type", 0)
            
            elif php_proxy != 'LOCAL':
                proxy = self.key_dict['proxy'][php_proxy]
                profile.set_preference('network.proxy.type', 1)
                profile.set_preference('network.proxy.http', proxy['host'])
                profile.set_preference('network.proxy.http_port', proxy['port'])
                profile.set_preference('network.proxy.ssl',proxy['host'])
                profile.set_preference('network.proxy.ssl_port', proxy['port'])
            
            # modify download setting
            profile.set_preference("browser.download.folderList", 2)
            profile.set_preference("browser.download.manager.showWhenStarting", False)
            # set download directory
            profile.set_preference("browser.download.dir", self.task_information_dictionary['download_directory'])
            
            # types of file for directly executing downloading task without asking
            profile.set_preference("browser.helperApps.neverAsk.saveToDisk",
                                '''
                                text/plain,text/x-csv,text/csv,
                                application/vnd.ms-excel,
                                application/csv,application/x-csv,
                                text/csv,text/comma-separated-values,
                                text/x-comma-separated-values,
                                text/tab-separated-values,
                                application/pdf
                                ''')
            
            # evoke driver with geckodriver path and options
            self.driver = webdriver.Firefox(options = options,firefox_profile = profile, \
                                            executable_path = self.task_information_dictionary['geckodriver_path'])
            self.driver.maximize_window()
            # setting static waiting time
            self.driver.implicitly_wait(20)

            self.is_driver_launch_succeed = True
        
        except Exception as E:
            error_log = str(E)
        
        ending_time = datetime.now()
        message = '{} to launch driver using time: {}{}'.format('Succeeded' if self.is_driver_launch_succeed else 'Failed',
                                                                     ending_time - starting_time, 
                                                                     '' if self.is_driver_launch_succeed else ' due to {}'.format(error_log))
        print(message + '\n\n')
        
        self.PostMessage(self.slack_channel, message, '{}_Log_PhpMyAdmin'.format('Correct' if self.is_driver_launch_succeed else 'Error'))

            
    def FileHandler(self,task_name):
        '''
        purpose                   :  integrate Login(self) and SQLOperater(self)      
        param task_name           :  task name for execution
        return is_process_succeed :  boolean 
        '''
        is_process_succeed = False                
        
        #===========================================
        # downloaded file name
        # downloaded file type
        # database name for executing sql query
        # sql query to execute
        # timeout setting for sql result downloading
        #===========================================
       
        self.file_name, self.file_type, self.database, self.link, self.sql_query, self.timeout_second  = \
        [self.task_information_dictionary['task'][task_name][item] for item in \
        ['file_name', 'file_type', 'database', 'link', 'sql_query', 'dwonload_waiting_second']]
        
        # local file path of downloaded file. If exists, then remove
        self.download_file_path = '{}/{}'.format(self.task_information_dictionary['download_directory'], '{}.{}'.format(self.file_name, self.file_type))
        
        if os.path.exists(self.download_file_path):
            os.remove(self.download_file_path)
        
        is_login_succeed = self.Login()
        
        if is_login_succeed:
            print('================================================= Finish Login =================================================\n\n')
            
            is_operate_succeed = self.SQLOperater()
            if is_operate_succeed:
                is_process_succeed = True
                print('================================================ Finish Download ===============================================\n\n')               
        return is_process_succeed

#=======================================================  Login PhpMyAdmin  ============================================================================== 

    def Login(self, timeout = 35):
        '''
        purpose                   : automatically login PhpMyAdmin with basic authencation
        param timeout             : setting timeout seconds with 35 seconds as default
        return is_login_succeed   : boolean  
        '''
        login_starting_time=datetime.now()
        is_login_succeed = False
       
        # phpmyadmin url address of this database
        self.url_address = self.key_dict['phpmyadmin'][self.link]['url_address']
               
        try:
            self.is_need_input = self.key_dict['phpmyadmin'][self.link]['is_need_input']

            # no login input interface for most cases
            if not self.is_need_input:

                login_url=self.url_address.split('://')[0]+'://{}:{}@'.format(self.key_dict['phpmyadmin'][self.link]['username'],\
                                                                            self.key_dict['phpmyadmin'][self.link]['password'])+self.url_address.split('://')[-1]

                self.driver.get(login_url)
                
            # has login input interface
            else:
                self.driver.get(self.url_address)
                locator = (By.ID, 'input_go')
                WebDriverWait(self.driver, timeout, 0.5).until(EC.element_to_be_clickable(locator))
                self.driver.find_element_by_id('input_username').send_keys(self.key_dict['phpmyadmin'][self.link]['username'])
                self.driver.find_element_by_id('input_password').send_keys(self.key_dict['phpmyadmin'][self.link]['password'])
                self.driver.find_element_by_id('input_go').click()
            
            # dynamically wait till information_schema is clickable for 10 seconds
            locator = (By.LINK_TEXT,'information_schema')
            WebDriverWait(self.driver, timeout, 0.5).until(EC.element_to_be_clickable(locator))
            time.sleep(1)     
            is_login_succeed = True
        
        except TimeoutException as E:
            # make sure information_schema is clickable => which means 
            error_log = '{}-second non-clickable login timeout'.format(timeout)   
        
        except Exception as E: 
            error_log = str(E)        
        login_ending_time=datetime.now()
        login_message='{} to login {} using time {}{}'.format('Succeeded' if is_login_succeed else 'Failed',\
                                                             self.url_address, login_ending_time - login_starting_time,\
                                                            '' if is_login_succeed else ' with error log: {}'.format(error_log))
        print(login_message+'\n\n')
        self.PostMessage(self.slack_channel,login_message, 'Correct_Log_PhpMyAdmin' if is_login_succeed else 'Error_Log_PhpMyAdmin')    
        return is_login_succeed
    
#=======================================================  SQL Operater  ==============================================================================   
    
    def SQLOperater(self, timeout = 15):
        '''
        purpose                   :  automatically operate on PhpMyAdmin and download file to local directory
        param timeout             : setting timeout seconds with 15 seconds as default
        return is_operate_succeed : boolean 
        '''
        phpmyadmin_starting_time = datetime.now()
        is_operate_succeed = True
        try:
            # select database
            locator = (By.PARTIAL_LINK_TEXT, self.database)
            WebDriverWait(self.driver, timeout, 0.5).until(EC.element_to_be_clickable(locator))
            self.driver.find_element_by_partial_link_text(self.database).click()
            print('============================================= Select Database {} ============================================\n\n'.format(self.database))
            time.sleep(3)
            
            # enter SQL query container
            locator = (By.PARTIAL_LINK_TEXT, "SQL")
            WebDriverWait(self.driver, timeout, 0.5).until(EC.element_to_be_clickable(locator))
            self.driver.find_element_by_partial_link_text("SQL").click()    
            time.sleep(3)
            print('========================================== Enter SQL Input Interface ===========================================\n\n')
            
            # the last textarea is allowed for inputing sql query and send sql query to this textarea
            textarea_time_start = datetime.now()
            textarea_xpath = '/html/body/div[{}]/form/div/fieldset/div[1]/div[1]/div[1]/div[1]/textarea'.format(4 if self.is_need_input else 5)
            while (datetime.now() - textarea_time_start).total_seconds() < timeout:
                
                if self.driver.find_elements_by_xpath(textarea_xpath):
                    break
                
                time.sleep(1)
            
            self.driver.find_element_by_xpath(textarea_xpath).send_keys(self.sql_query)   
            time.sleep(1)
            
            # change window location to (0, 0)
            self.driver.execute_script("window.scrollTo(0, 0)") 
            # click for executing sql query
            self.driver.find_element_by_id('button_submit_query').click()
            print('=============================================== Execute SQL Query ===============================================\n\n')
    
            time.sleep(15)
            
            # export sql result => xpath has two different xpath
            if self.driver.find_elements_by_xpath('//*[@id="sqlqueryresultsouter"]/div/fieldset/form[1]/a/span/img'):
                export_xpath = '//*[@id="sqlqueryresultsouter"]/div/fieldset/form[1]/a/span/img'
            else:
                export_xpath = '//*[@id="sqlqueryresultsouter"]/div/fieldset/a[3]/span/img'
             
            # move to the bottom of the page
            export_icon = self.driver.find_element_by_xpath(export_xpath)
            
            # move location of page to coordinate x = 0 and y equals to the export icon
            self.driver.execute_script("window.scrollTo({},{})".format(0, export_icon.location['y'])) 
                       
            print('=============================================== Move Page Location To Export Icon ===============================================\n\n')
            time.sleep(1)
            
            locator = (By.XPATH, export_xpath)
            WebDriverWait(self.driver, timeout, 0.5).until(EC.element_to_be_clickable(locator))
            time.sleep(1)
            export_icon.click()
           
            print('=============================================== Enter Export Page ===============================================\n\n')
            time.sleep(3)
            
            print('============================================ Customize Export Setting ===========================================\n\n')
            # select csv format
            select = Select(self.driver.find_element_by_id('plugins'))
            select.select_by_value(self.file_type)
            time.sleep(1.5)
            
            # select customized option
            self.driver.find_element_by_id('radio_custom_export').click()
            time.sleep(1.5)
            
            # move to the bottom of the page
            download_button =  self.driver.find_element_by_id('buttonGo')
            
            # move to location of download button
            self.driver.execute_script("window.scrollTo({},{})".format(download_button.location['x'], download_button.location['y'])) 
            print('=============================================== Move Page Location To Download Button ===============================================\n\n')
            time.sleep(1.5)
            
            # clear default file name and input our setting file name to downloading file
            self.driver.find_element_by_name('filename_template').clear()
            self.driver.find_element_by_name('filename_template').send_keys(self.file_name)
            time.sleep(3)
            print('=============================================== Change File Name ===============================================\n\n')
            # set for keeping column names of query => works for csv downloading file
            if self.file_type == 'csv':
                keep_column = self.driver.find_element_by_id("checkbox_csv_columns")
                # move to location of checkbox_csv_columns
                self.driver.execute_script("window.scrollTo({},{})".format(keep_column.location['x'], keep_column.location['y']))
                self.driver.find_element_by_id("checkbox_csv_columns").click()
                print('=============================================== Keep Column Name ===============================================\n\n')
                time.sleep(1)
            
            # click for downloading
            self.driver.find_element_by_id('buttonGo').click()
            print('========================================== Click To Execute Download ==========================================\n\n')
            
            # monitor for timeout
            is_operate_succeed = self.Timeout()
            if not is_operate_succeed:
                error_log = 'exceeded {} seconds timeout limitation'.format(self.timeout_second)
        
        except TimeoutException as E:
            # make sure information_schema is clickable => which means successfully log in PhpMyAdmin
            error_log = '{}-second operation timeout'.format(timeout)  
            is_operate_succeed = False

        except Exception as E:
            error_log = str(E)
            is_operate_succeed = False
        
        phpmyadmin_ending_time = datetime.now()
        operate_message='{} to automatically execute SQL query and download {} file from {} to {} using time: {}{}'.\
                                                        format('Succeeded' if is_operate_succeed else 'Failed',self.file_type,\
                                                        self.url_address,self.download_file_path,\
                                                        phpmyadmin_ending_time - phpmyadmin_starting_time,\
                                                        '' if is_operate_succeed else ' with error log: {}'.format(error_log))                                                                       
        
        print(operate_message+'\n\n')
        self.PostMessage(self.slack_channel,operate_message, 'Correct_Log_PhpMyAdmin' if is_operate_succeed else 'Error_Log_PhpMyAdmin')
        return is_operate_succeed
            
#=======================================================  Timeout  ==============================================================================
    
    def Timeout(self):
        '''
        purpose  :  check if timeout situation happens
        '''
        is_download_succeed = False
        initial_time = datetime.now()
         
        while (datetime.now() - initial_time).total_seconds() < self.timeout_second:

            if os.path.exists(self.download_file_path) and not os.path.exists('{}.part'.format(self.download_file_path)):
                is_download_succeed = True     
                break
            # check for every three seconds
            print('====================== File Downloading Process Is Ongoing...Time Spent(Sec.) So Far: {} ======================\n\n'\
            .format(round((datetime.now() - initial_time).total_seconds(),2)))
            time.sleep(1)
        print('==================== {} To Finish Downloading Process With Time Spent: {} ===================\n\n'\
        .format('Succeeded' if is_download_succeed else 'Failed',datetime.now() - initial_time))
        return is_download_succeed