import json
import os

# path of key.txt
key_file_path='/home/shifty/data/.keys/key.txt'

class KeyManagement:
    '''
    purpose    : Manage all keys recorded in key.txt
    '''
    def __init__(self):
        '''
        purpose       : set up a module for key management
        key_file_path : path to the key text file 
        '''
        
        with open(key_file_path) as json_file:
            self.key_dict = json.load(json_file)

        os.environ['GOOGLE_APPLICATION_CREDENTIALS'] = self.key_dict['bigquery']['key_file_path']