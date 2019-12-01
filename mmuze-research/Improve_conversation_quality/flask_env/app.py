from flask import Flask
app = Flask(__name__)
from flask import request
import json
from werkzeug.exceptions import BadRequest
#FLASK_APP=flask_env/app.py flask run


import random


''' 

naive app that returns a random value from the given options

expected keys



conversation_id  : Str
sender_id  : str
unix_ts : int of length 10 (seconds)
options : list of strings or string with commas


example curl

curl -X POST -H 'Content-Type: application/json' http://127.0.0.1:5000/ -d '{"sender_id": "Alice","key":"value","conversation_id":"dsa","sender_id":"dsa","unix_ts":1234567891,"options":["1","2","3"]}'

FLASK_APP=flask_env/app.py flask run

source flask_env/bin/activate

'''



#TODO : add authentication and logging




@app.route('/',methods = ['POST'])
def return_random():
    '''
    
    returns a random option from the list of "options".
    
    loggs recieved json an response.
    
    '''


    jsondata = request.get_json()
    
    # check if we have all keys
    received_keys = list(jsondata.keys())
    needed_keys = ['conversation_id', 'sender_id', 'unix_ts', 'options']
    missing_keys = list(set(needed_keys)-set(received_keys))

    if len(missing_keys)>0:
        
        raise BadRequest('you are missing the following keys: {}'.format(missing_keys))
        
    # check types

    if type(jsondata["conversation_id"])!=str:
        
       raise BadRequest('conversation_id should be string')  
       
       
       
    if type(jsondata["sender_id"])!=str:
        
       raise BadRequest('conversation_id should be string') 
       
       
        
    if len(str(jsondata["unix_ts"]))!=10 or type(jsondata["unix_ts"])!=int:
        
       raise BadRequest('unix ts should be an int having len of 10')
       
       
       
    if type(jsondata["options"])!=list:
        
        if type(jsondata["options"])!=str:
            
        
            raise BadRequest('options should be list of str or str with commas')
       
       
      
    # return random choice from options
    
    if type(jsondata["options"])==list:
        
        if len(jsondata["options"])==0:
            
            raise BadRequest('options should have at least one value')
            
        else:

            jsondata["random_choice"] = random.choice(jsondata["options"])  
        
            return jsondata   
    
    else:
        
        
        options = jsondata["options"].split(',')

        if len(options)==0:
            
            raise BadRequest('options should have at least one value')

        else:
        
            jsondata["random_choice"] = random.choice(options)
            
            return jsondata 
    
           
        
        
        
