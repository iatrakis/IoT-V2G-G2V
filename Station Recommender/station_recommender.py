import time
import sys
import os
import logging
import random
from logging.handlers import TimedRotatingFileHandler
from pathlib import Path
import json
from ischedule import schedule, run_loop
import ast
try:
    import my_common.my_util_pydantic as my_util_pydantic
except:
    import my_util_pydantic

def find_ID(in_topic,msg_topic):
    x=in_topic.split("/")
    y=msg_topic.split("/")
    for a,b in zip(x,y): 
        print(a)
        print(b)
        if (a!=b and a=="+"):
            ID=b
        if (a!=b and b=="+"):
            ID=a
        if (a!=b and b!="+" and a!="+"):
            return   
    return ID

def cs_available_msg_handler(dict_list, ID, msg):
    
    for i,cs_dict in enumerate(dict_list):
        if cs_dict["ID"] == ID:
            dict_list[i].update({**{"ID":ID},**json.loads(msg)})
            return
    dict_list.append({**{"ID":ID},**json.loads(msg)})

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

BASE_NAME = Path(__file__).resolve()
DIR_NAME = BASE_NAME.parent
SERVICE_FILE_NAME = BASE_NAME.name

logger_name = SERVICE_FILE_NAME.split('.')[0] + '.log'
logger_fqn = (DIR_NAME/'log'/logger_name).absolute().as_posix()
logger = setup_logger(SERVICE_FILE_NAME, logger_fqn, logging.DEBUG)
logger.info('Synaisthisi ok Service just started for service: {0}.'.format(SERVICE_FILE_NAME))

sr = my_util_pydantic.StationRecommender()

try:
    import paho.mqtt.client as paho
except Exception as ex:
    logger.error('Paho library is not present')
    sys.exit('Paho library is not present')

paho.logging

BROKER = os.environ.get('BROKER', 'localhost')
CLIENT_NAME = os.environ.get('CLIENT_NAME', '')
username = os.environ.get('username', '')
password = os.environ.get('password', '')
run_param = os.environ.get('run_params', '')

try:
    params = ast.literal_eval(run_param)
    time_mult = params["time_mult"]
except:
    time_mult = 1


## 
##  /\ /\ /\ /\ /\ /\ /\ /\
## 



##
##  Add your TOPICS here
##  \/\/\/\/\/\/\/\/


input_no = 7
output_no = 6

in_topic = [os.environ.get(f'in_topic_{i}', '') for i in range(1,input_no+1) ]
out_topic = [os.environ.get(f'out_topic_{i}', '') for i in range(1,output_no+1) ]


logger.info(f'Credentials: {username}:{password}')
logger.info(f'BROKER: {BROKER}')
for i,t in enumerate(in_topic):
    logger.info(f'in_topic_{i+1}: {t}')
for i,t in enumerate(out_topic):
    logger.info(f'out_topic_{i+1}: {t}')


IMBALANCE=0
cs_set = set(())
cs_list = []
started = False
##
## Define callbacks
##
## You can describe topic-specific behaviour here:
def on_message(client, userdata, message):
    # global FREQUENCY
    # global RANGE
    global cs_list
    global started
    r_msg = str(message.payload.decode("utf-8"))
    logger.info("message topic: {0}".format(message.topic))
    logger.info("received message ={0}".format(r_msg))
    
    
    #recieves available signal from cs and adds its ID
    # ID = find_ID(in_topic[0],message.topic)
    # if ID is not None:
    #     # cs_set.add(ID)
    #     cs_available_msg_handler(cs_list, ID, r_msg)
        



    # ID = find_ID(in_topic[2],message.topic)
    # if ID is not None:
    #     cs_list = sorted(cs_list, key = lambda i: i['price'])
    #     client.publish(out_topic[1].replace('+', ID),str(cs_list))
    extra=1

    if(message.topic==in_topic[0]):#in_topic[1]
        if(r_msg=="start"):
            started=True
            return

    ID = my_util_pydantic.find_ID(in_topic[0+extra],message.topic)
    if ID is not None:
        (topic, msg) = sr.ReceiveChargingStationRequest(r_msg,message.topic)
        client.publish(topic,msg)

    ID = my_util_pydantic.find_ID(in_topic[5+extra],message.topic)
    if ID is not None:
        (topic, msg) = sr.ReceiveRequestUpdateAvailability(r_msg,message.topic)
        client.publish(topic,msg)

    ID = my_util_pydantic.find_ID(in_topic[3+extra],message.topic)
    if ID is not None:
        sr.ReceiveElectricityPrices( r_msg,message.topic)
        logger.info("sr.prices: {0}".format(sr._buy_prices))
        logger.info("sr.prices: {0}".format(sr._sell_prices))
    
    ID = my_util_pydantic.find_ID(in_topic[4+extra],message.topic)
    if ID is not None and ID == "SR":
        sr.RecieveElectricityImbalance( r_msg,message.topic)
        logger.info("sr.imbalance: {0}".format(sr.imbalance))
    
    ID = my_util_pydantic.find_ID(in_topic[1+extra],message.topic)
    if ID is not None:
        (topic, msg) = sr.ReceiveRecommendationRequest( r_msg,message.topic)
        # logger.info("sr.station_dict: {0}".format(sr.station_dict))
        client.publish(topic,msg)

    ID = my_util_pydantic.find_ID(in_topic[2+extra],message.topic)
    if ID is not None:
        (topic, msg) = sr.ReceiveRecommendationAuthenticationQuery( r_msg,message.topic)
        # logger.info("sr.station_dict: {0}".format(sr.station_dict))
        client.publish(topic,msg)    
    # if(message.topic==in_topic_1):
    #     IMBALANCE += float(r_msg)
    # if(message.topic==in_topic_3):
    #     IMBALANCE -= float(r_msg)
    # if(message.topic==in_topic_5):    
    #     client.publish(out_topic_1,float(IMBALANCE)) 
    
    # if(message.topic=="disconnect"):
    #     client.disconnect()
    #     client.loop_stop()

##
## Write logs
##
def on_log(client, userdata, level, buf):
    logger.info('Client {0}, sending userdata: {1}, buf: {2}'.format(client._client_id.decode('ascii'), userdata, buf))

##
## What to do when the service is disconnected from the platform's broker
##
def on_disconnect(client, userdata, flags, rc=0):
    logger.info('Disconnected, client: {0} and flags: {1}'.format(client._client_id, flags))
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
        
        for t in in_topic:
            client.subscribe(t)
        ## 
        ##  /\ /\ /\ /\ /\ /\ /\ /\
        ## 
        # client.subscribe("disconnect")
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
    # client.connect(BROKER)#connect
    client.connect(BROKER)
except Exception as ex:
    logger.error('Error connecting to broker, error: {0}'.format(ex))
    sys.exit('Error connecting to broker')

#client.loop_forever()
client.loop_start()
day=0    
start_time = time.time()

def daily_routine():
    global day
    dt = time.time() - start_time
    print(f"Started a *slow* day task at t={dt:.5f}")
    day +=1
    print("DAYYYY sim_now",day)
    logger.info(f'day: {day}')
    # my_util_pydantic.sim_now+=  my_util_pydantic.time_step*24
    # logger.info(f'my_util_pydantic.sim_now: {my_util_pydantic.sim_now}')

    
    # msg0 = md.SendElectricityImbalanceRequest()
    # client.publish(out_topic[4].replace('+', "MD"),msg0)
    # time.sleep(2)
    # (topic, msg) = md.ReceiveElectiricityPricesRequest("lala","MD/CS0/ElectricityPricesRequest")
    # client.publish(topic,msg)

def hourly_routine():
    
    my_util_pydantic.sim_now+=  my_util_pydantic.time_step
    print(my_util_pydantic.sim_now)
    logger.info(f'{my_util_pydantic.sim_now}')    

# my_util_pydantic.sim_now+=  my_util_pydantic.time_step*24
while True:
    pass
    if started:
        break
schedule(daily_routine, interval=24*time_mult)
schedule(hourly_routine, interval=1*time_mult)
run_loop()
""" 
schedule(daily_routine, interval=24)
schedule(hourly_routine, interval=1)
# run_loop(return_after=30)
time.sleep(24)
time.sleep(24)
# my_util_pydantic.sim_now-=  my_util_pydantic.time_step*24*2
run_loop()
 """
# while True:
#     time.sleep(2)
    # msg0 = sr.SendElectricityPricesRequest()
    # client.publish(out_topic[3].replace('+', "SR"),msg0)
    # msg0 = sr.SendElectricityImbalanceRequest()
    # client.publish(out_topic[4].replace('+', "SR"),msg0)

   
