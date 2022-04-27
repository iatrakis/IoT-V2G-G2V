from time import sleep
import sys
import os
import logging
from logging.handlers import TimedRotatingFileHandler
from pathlib import Path

import sys

from csv import writer
from csv import DictWriter
import ast

import pandas as pd
from datetime import datetime,timedelta
import matplotlib.pyplot as plt

import ssl
import requests
import json

import winsound



plt.close("all")

def fetchcolvalue(data_file, dateTime):

    import csv

    file = open(data_file, 'r')
    ct = 0

    for row in csv.reader(file):
        if row[ct] == dateTime:
            print(row[1])

def append_list_as_row(file_name, list_of_elem):
    # Open file in append mode
    with open(file_name, 'a+', newline='') as write_obj:
        # Create a writer object from csv module
        csv_writer = writer(write_obj)
        # Add contents of list as last row in the csv file
        csv_writer.writerow(list_of_elem)
def append_dict_as_row(file_name, dict_of_elem, field_names):
    # Open file in append mode
    with open(file_name, 'a+', newline='') as write_obj:
        # Create a writer object from csv module
        dict_writer = DictWriter(write_obj, fieldnames=field_names)
        # Add dictionary as wor in the csv
        dict_writer.writerow(dict_of_elem)

def get_size(obj, seen=None):
    """Recursively finds size of objects"""
    size = sys.getsizeof(obj)
    if seen is None:
        seen = set()
    obj_id = id(obj)
    if obj_id in seen:
        return 0
    # Important mark as seen *before* entering recursion to gracefully handle
    # self-referential objects
    seen.add(obj_id)
    if isinstance(obj, dict):
        size += sum([get_size(v, seen) for v in obj.values()])
        size += sum([get_size(k, seen) for k in obj.keys()])
    elif hasattr(obj, '__dict__'):
        size += get_size(obj.__dict__, seen)
    elif hasattr(obj, '__iter__') and not isinstance(obj, (str, bytes, bytearray)):
        size += sum([get_size(i, seen) for i in obj])
    return size


##
##  Do not change!
##  \/\/\/\/\/\/\/\/
def setup_logger(name, log_file, level=logging.INFO):
    formatter = logging.Formatter('%(asctime)s %(levelname)s %(message)s')
    handler = TimedRotatingFileHandler(log_file)        
    handler.setFormatter(formatter)
    logger = logging.getLogger(name)
    logger.setLevel(level)
    logger.addHandler(handler)
    return logger
#start services


x = os.popen("curl -k -X POST --data \"username=<username>&password=<password>\" localhost/login").read()
print("XXXX access_token XXXXXXXXX",x)
token = x.split("\"access_token\": \"")[1].split("\", \"refresh_token\"")[0]
print("XXXX access_token XXXXXXXXX",token)
try:
    _create_unverified_https_context = ssl._create_unverified_context
except AttributeError:
    # Legacy Python that doesn't verify HTTPS certificates by default
    pass
else:
    # Handle target environment that doesn't support HTTPS verification
    ssl._create_default_https_context = _create_unverified_https_context
r = requests.post('localhost/login', data={"username":"<username>","password":"<password>"}, verify=False)

headers = {"Authorization":"Bearer eyJ0eXAiOiJKV1QiLCJhbGciOiJIUzI1NiJ9.eyJleHAiOjE2MzY0NDc2MjUsImlhdCI6MTYzNjQ0NjcyNSwibmJmIjoxNjM2NDQ2NzI1LCJqdGkiOiI3MmVkOTgyMC1iZTM3LTQzMTMtOGY2Yi05YzFmNzUzMGNmYWQiLCJpZGVudGl0eSI6MSwiZnJlc2giOnRydWUsInR5cGUiOiJhY2Nlc3MiLCJ1c2VyX2NsYWltcyI6eyJ1c2VybmFtZSI6ImlhdHJha2lzIiwiZW1haWwiOiJnZW9yZ2lhdHIxM0BnbWFpbC5jb20iLCJhZG1pbiI6ZmFsc2V9fQ.as2z7AWR0WiTK_Cy_yCYwUCrIrATZ8mAXq4akTcWoRo"}

for line in r:
    print("r.headers.get('Authorization')222",line)
headers = {"Authorization":f"Bearer {token}"}

dd={"ev_tens":10,"time_mult":5,"linked_hours":24,"cs_num":1,"cs_slots":100,"sched_algo":2,"nrgcoin_algo":"False"}
data = {"password":"<password>","run_params":json.dumps(dd) }
notes ="palio CS me 1 client gia kathe CS kai evs xoris sleep nrg tests gia no of msg"

seira=[7,6,2,5,4,3,1]


time = datetime.now()
for i in seira:
    r = requests.post(f'localhost/users/1/services/{i}/status', data=data, headers=headers,verify=False)
    print(r.content)

print(datetime.now()-time)





BASE_NAME = Path(__file__).resolve()
DIR_NAME = BASE_NAME.parent
SERVICE_FILE_NAME = BASE_NAME.name

logger_name = SERVICE_FILE_NAME.split('.')[0] + '.log'
logger_fqn = (DIR_NAME/'log'/logger_name).absolute().as_posix()
logger = setup_logger(SERVICE_FILE_NAME, logger_fqn, logging.DEBUG)
logger.info('Synaisthisi ok Service just started for service: {0}.'.format(SERVICE_FILE_NAME))
now_t = datetime.now()
now_str = now_t.strftime("%Y_%m_%d-%H_%M_%S")
now_txt = now_t.strftime("%Y-%m-%d %H:%M:%S")
path1 = os.path.join(DIR_NAME/'csv_log', 'test_'+now_str)
os.mkdir(path1)

f=open(f"{path1}/report_{now_str}.txt", "a+")
f.write(f"Report {now_txt}\n{json.dumps(dd)}\n\n{notes}")
f.close()
try:
    import paho.mqtt.client as paho
except Exception as ex:
    logger.error('Paho library is not present')
    sys.exit('Paho library is not present')

paho.logging
BROKER = "localhost"
CLIENT_NAME = "0"
username = "<username>"
password = "<password>" 



##
## Define callbacks
##
## You can describe topic-specific behaviour here:
msg_count = 0
total_size = 0

start_date= datetime(2020,1,1,0,0,0)

time_step = timedelta(hours=1)
dates=[(start_date+i*time_step).strftime("%Y-%m-%dT%H:%M:%S") for i in range(24*10) ]
col = "price2"

df3 = pd.DataFrame(
{},
index = dates,
columns=['buy_price', 'sell_price', 'production','consumption','schedule','buy','sell','EC0_mean','EC0_std'])

f1 = lambda x: "EV"+str(x)+"_sched"
f2 = lambda x: "EV"+str(x)+"_buy"
f3 = lambda x: "EV"+str(x)+"_sell"
f4 = lambda x: "EV"+str(x)+"_cost"
f5 = lambda x: "EV"+str(x)+"_cum_cost"
ev_id = [f(x) for x in range(10001,10101) for f in (f1,f2,f3,f4,f5)]
columns_cum_cost = [f5(x) for x in range(10001,10101) ]
ev_id.append("avg_cum_cost")
df4 = pd.DataFrame(
{},
index = dates,
columns=ev_id)
count=0
msg_counter = 0 
print_count = True
def on_message(client, userdata, message):
    global msg_count, total_size,count,msg_counter,print_count
    msg_counter +=1
    
    r_msg = str(message.payload.decode("utf-8"))

    if message.topic.endswith("new/subscribes") :
        print(r_msg)
        res = json.loads(r_msg)
        aa="EV/+/ChargingRecommendations"
       
        
        print(res["topic"]==aa)
        if res["topic"]==aa:
            count+=1
            print("count",count)
            if count==1:
                sleep(10)
                client.publish("timer","start")
    

    if message.topic == "MD/ElectricityPrices":
        list1 = ast.literal_eval(r_msg)
        field_names = ['dateTime','price','tt']
       
        for p0,p1 in zip(list1[0], list1[1]):
            
            di0 = ast.literal_eval(p0)
            di1 = ast.literal_eval(p1)
            if di0["dateTime"] == datetime(2020,1,12,0,0,0).strftime("%Y-%m-%dT%H:%M:%S"):
                print("sxolase")
                winsound.PlaySound("SystemExclamation", winsound.SND_ALIAS)
                if print_count:
                    print("messages sent ",msg_counter )
                    f=open(f"{path1}/report_{now_str}.txt", "a+")
                    f.write(f"\n\nmessages sent {msg_counter}")
                    f.close()
                    
                    f=open(f"{path1}/report_{now_str}.txt", "a+")
                    day10=datetime(2020,1,10,23,0,0).strftime("%Y-%m-%dT%H:%M:%S")
                    avg_cost = df4.loc[day10]["avg_cum_cost"]
                    print("hours",dd["linked_hours"])
                    print("sched_algo",dd["sched_algo"])
                    print("avg cost",avg_cost )
                    f.write(f"\n\navg cost {avg_cost}")
                    f.close()

                    print_count = False

           
            df3.at[di0["dateTime"],"buy_price"]=di0["price"]
            df3.at[di1["dateTime"],"sell_price"]=di1["price"]
            

    if message.topic == "EC/EC0/UpdateConfidence":
        list1 = ast.literal_eval(r_msg)
        
        for p0 in list1:
            di0 = ast.literal_eval(p0)
            df3.at[di0["dateTime"],"EC0_mean"]=di0["confidence"]["mean"]
            df3.at[di0["dateTime"],"EC0_std"]=di0["confidence"]["std"]
            
        
      
    if message.topic == "EI/ElectricityImbalance":
        list1 = ast.literal_eval(r_msg)
        
        
        for p0,p1 in zip(list1[0], list1[1]):
            
            di0 = ast.literal_eval(p0)
            di1 = ast.literal_eval(p1)
            df3.at[di0["dateTime"],"production"]=di0["kwh"]
            df3.at[di1["dateTime"],"consumption"]=di1["kwh"]
            

        df3.to_csv(f"{path1}/prices_{now_str}.csv")
        df4.to_csv(f"{path1}/EV_schedules_{now_str}.csv")

    if message.topic == "EV/EV10001/ReservationOutcome":
        
        list1 = ast.literal_eval(r_msg)
        
        k1 = ast.literal_eval(list1[1])
        k2 = ast.literal_eval(list1[2])
        k3 = ast.literal_eval(list1[3])

        for p1,p2,p3 in zip(k1, k2, k3):
            
            di1 = ast.literal_eval(p1)
            di2 = ast.literal_eval(p2)
            di3 = ast.literal_eval(p3)
            
            
            df3.at[di1["dateTime"],"schedule"]=di1["kwh"]
            df3.at[di2["dateTime"],"buy"]=di2["price"]
            df3.at[di3["dateTime"],"sell"]=di3["price"]

        
    
    if len(message.topic.split("/"))==3 and message.topic.split("/")[2] == "ReservationOutcome":
        
        list1 = ast.literal_eval(r_msg)
        
        
        k1 = ast.literal_eval(list1[1])
        k2 = ast.literal_eval(list1[2])
        k3 = ast.literal_eval(list1[3])


        for p1,p2,p3 in zip(k1, k2, k3):
            
            di1 = ast.literal_eval(p1)
            di2 = ast.literal_eval(p2)
            di3 = ast.literal_eval(p3)
            
            df4.at[di1["dateTime"],message.topic.split("/")[1]+"_sched"]=di1["kwh"]
            df4.at[di2["dateTime"],message.topic.split("/")[1]+"_buy"]=di2["price"]
            df4.at[di3["dateTime"],message.topic.split("/")[1]+"_sell"]=di3["price"]
            if di1["kwh"]>=0:
                df4.at[di1["dateTime"],message.topic.split("/")[1]+"_cost"]=di1["kwh"]*di2["price"]
            else:
                df4.at[di1["dateTime"],message.topic.split("/")[1]+"_cost"]=di1["kwh"]*di3["price"]
        
        df4[message.topic.split("/")[1]+"_cost"] = df4[message.topic.split("/")[1]+"_cost"].fillna(0)      
        df4[message.topic.split("/")[1]+"_cum_cost"] = df4[message.topic.split("/")[1]+"_cost"].cumsum() 
        df4["avg_cum_cost"] = df4[columns_cum_cost].mean(axis=1)
        

        
##
## Write logs
##
def on_log(client, userdata, level, buf):
    logger.info('Client {0}, sending userdata: {1}, buf: {2}'.format(client._client_id.decode('ascii'), userdata, buf))

##
## What to do when the service is disconnected from the platform's broker
##
def on_disconnect(client, userdata, flags, rc=0):
    logger.info('Disconnected, client: {0}'.format(client._client_id))
    sys.exit('Disconnected, client: {0}'.format(client._client_id))

##
## What to do when the service is connected to the platform's broker
##
def on_connect(client, userdata, flags, rc):
    logger.info(f'Conencting ... with : {username}:{password}')

    if(rc==0):
        logger.info("connecting to broker {0}".format(BROKER))
        logger.info("Subscribing to (input) topics")
        ##
        ##  Add subscription to your TOPICS here
        ##  \/\/\/\/\/\/\/\/
        client.subscribe("#")
        
        # client.subscribe("disconnect")
        ## 
        ##  /\ /\ /\ /\ /\ /\ /\ /\
        ## 
    elif(rc==3):
        logger.error("server unavailable")
        client.loop_stop()
        sys.exit("Server is unavailable, please try later")
    elif(rc==5):
        logger.error("Invalid Credentials")
        client.loop_stop()
        sys.exit('Invalid Credentials')
    else:
        logger.error("Bad connection, returned code={0}".format(rc))
        client.loop_stop()
        sys.exit("Bad connection, returned code={0}".format(rc))



client= paho.Client(CLIENT_NAME) 
client.username_pw_set(username, password)

###### CALLBACKS
# Callback to handle subscription  topics incoming messages
client.on_message=on_message
# Log callback
client.on_log=on_log
# Callback to define what to happen when connecting, e.g. usually subscribe to topics
client.on_connect=on_connect
# Callback to define what to happen when disconnecting,
client.on_disconnect=on_disconnect
# Callback for publish actions
# client.on_publish = on_publish

# Now we try to connect
try:
    logger.info(f'Initiating connection ....')
    client.connect(BROKER, 1883,keepalive=13*60)#connect
    
except Exception as ex:
    logger.error('Error connecting to broker, error: {0}'.format(ex))
    sys.exit('Error connecting to broker')

client.loop_forever()


