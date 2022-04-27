import time
import sys
import os
import logging
import random
from logging.handlers import TimedRotatingFileHandler
from pathlib import Path
from ischedule import schedule, run_loop
import ast
try:
    import my_common.my_util_pydantic as my_util_pydantic
except:
    import my_util_pydantic

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

try:
    import paho.mqtt.client as paho
except Exception as ex:
    logger.error('Paho library is not present')
    sys.exit('Paho library is not present')

paho.logging

BROKER =os.environ.get('BROKER', 'localhost')
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

input_no = 11
output_no = 5

in_topic = [os.environ.get(f'in_topic_{i}', '') for i in range(1,input_no+1) ]
out_topic = [os.environ.get(f'out_topic_{i}', '') for i in range(1,output_no+1) ]

logger.info(f'Credentials: {username}:{password}')
logger.info(f'BROKER: {BROKER}')
for i,t in enumerate(in_topic):
    logger.info(f'in_topic_{i+1}: {t}')
for i,t in enumerate(out_topic):
    logger.info(f'out_topic_{i+1}: {t}')

supply = [0 for i in range(24)]
demand = [0 for i in range(24)]
imba = my_util_pydantic.ElectricityImbalance()
started = False
reg=["MD","CS1"]
##
## Define callbacks
##
## You can describe topic-specific behaviour here:
def on_message(client, userdata, message):
    global supply
    global demand
    global started
    r_msg = str(message.payload.decode("utf-8"))
    logger.info("message topic: {0}".format(message.topic))
    logger.info("received message ={0}".format(r_msg))
    

    # ID = my_util_pydantic.find_ID(in_topic[0],message.topic)
    # if ID is not None:
    #     my_util_pydantic.hourly_update_power(supply,r_msg)

    # ID = my_util_pydantic.find_ID(in_topic[2],message.topic)
    # if ID is not None:
    #     my_util_pydantic.hourly_update_power(demand,r_msg)
        
    # ID = my_util_pydantic.find_ID(in_topic[4],message.topic)
    # if ID is not None:
    #     msg = f"{{\"total_supply\":{supply}\n\"total_demand\":{demand}}}"
    #     client.publish(out_topic[0].replace('+', ID),msg)
    extra=1

    if(message.topic==in_topic[1]):#in_topic[1]
        if(r_msg=="start"):
            started=True
            return

    ID = my_util_pydantic.find_ID(in_topic[8+extra],message.topic)#in_topic[8+extra]
    if ID is not None:
        (topic, msg) = imba.ReceiveElectricityImbalanceRequest(r_msg,message.topic)
        client.publish(topic,msg)
        

    ID = my_util_pydantic.find_ID(in_topic[1+extra],message.topic)
    if ID is not None:
        (topic, msg) = imba.ReceiveRegistrationRequest(r_msg,message.topic)
        client.publish(topic,msg)

    ID = my_util_pydantic.find_ID(in_topic[2+extra],message.topic)
    if ID is not None:
        (topic, msg) = imba.ReceiveRegistrationRequest(r_msg,message.topic)
        client.publish(topic,msg)
        
    ID = my_util_pydantic.find_ID(in_topic[3+extra],message.topic)
    if ID is not None:
        (topic, msg) = imba.ReceiveChargingStationRequest(r_msg,message.topic)
        client.publish(topic,msg)

    ID = my_util_pydantic.find_ID(in_topic[4+extra],message.topic)
    if ID is not None:
        (topic, msg) = imba.ReceiveUpdateExpectedProfileRequest(r_msg,message.topic)
        client.publish(topic,msg)

    ID = my_util_pydantic.find_ID(in_topic[5+extra],message.topic)
    if ID is not None:
        (topic, msg) = imba.ReceiveUpdateExpectedProfileRequest(r_msg,message.topic)
        client.publish(topic,msg)

    ID = my_util_pydantic.find_ID(in_topic[6+extra],message.topic)
    if ID is not None:
        (topic, msg) = imba.ReceiveUpdateConfidenceRequest(r_msg,message.topic)
        client.publish(topic,msg)

    ID = my_util_pydantic.find_ID(in_topic[7+extra],message.topic)
    if ID is not None:
        (topic, msg) = imba.ReceiveUpdateConfidenceRequest(r_msg,message.topic)
        client.publish(topic,msg)

    ID = my_util_pydantic.find_ID(in_topic[0],message.topic)
    if ID is not None:
        (topic, msg) = imba.ReceiveUpdateConfidenceRequest(r_msg,message.topic)
        client.publish(topic,msg)

    ID = my_util_pydantic.find_ID(in_topic[9+extra],message.topic)
    if ID is not None:
        (topic, msg) = imba.ReceiveRequestUpdatedStationSchedule(r_msg,message.topic)
        client.publish(topic,msg)
        #otan erxetai ston imbalancer neo update schedule tha prepei na steilei neo imbalance (gia tin trexousa mera) se osoun ton akoun
        #                                                                                     gia tin mera/tis meres pou allakse to schedule
        # x = list(imba.station_dict.keys())
        # x.append("MD")   
        # for id in x:
        # for id in reg:
        topic111 , msg111 = imba.ReceiveElectricityImbalanceRequest("0","EI/+/ElectricityImbalanceRequest".replace("+","CS10000000"))
        client.publish(topic111 , msg111)
        topic111 , msg111 = imba.ReceiveElectricityImbalanceRequest("1","EI/+/ElectricityImbalanceRequest".replace("+","CS10000000"))
        client.publish(topic111 , msg111)

    

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

# client.loop_forever()

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
    logger.info(f'my_util_pydantic.sim_now: {my_util_pydantic.sim_now}')
    
    # time.sleep(0.2)
    # (topic, msg) = imba.ReceiveElectricityImbalanceRequest("mlpa","EI/MD/ElectricityImbalanceRequest")
    # client.publish(topic,msg)
def hourly_routine():
    global day
    my_util_pydantic.sim_now+=  my_util_pydantic.time_step
    print(my_util_pydantic.sim_now)
    logger.info(f'{my_util_pydantic.sim_now}')
    
    #otan einai 1 i ora "pairnei request apo ton CS10000" kai stelnei o MD ta prices
    if  my_util_pydantic.sim_now.hour==1:
        if day == 0:
            topic111 , msg111 = imba.ReceiveElectricityImbalanceRequest("0","EI/+/ElectricityImbalanceRequest".replace("+","CS10000000"))
            client.publish(topic111 , msg111)
        topic111 , msg111 = imba.ReceiveElectricityImbalanceRequest("1","EI/+/ElectricityImbalanceRequest".replace("+","CS10000000"))
        client.publish(topic111 , msg111)

    
    
    
    


while True:
    pass
    if started:
        break
schedule(daily_routine, interval=24*time_mult)
schedule(hourly_routine, interval=1*time_mult)
run_loop()
""" schedule(daily_routine, interval=24)

# run_loop(return_after=30)
time.sleep(24)
time.sleep(24)
run_loop()
 """

# FREQUENCY=24
# i=0
# time.sleep(10)
# while True:
#     time.sleep(FREQUENCY)
#     i+=1
#     logger.info(f'i: {i}')
#     my_util_pydantic.sim_now+=  my_util_pydantic.time_step*24
#     logger.info(f'my_util_pydantic.sim_now: {my_util_pydantic.sim_now}')
    

#     (topic, msg) = imba.ReceiveElectricityImbalanceRequest("mlpa","EI/MD/ElectricityImbalanceRequest")
#     client.publish(topic,msg)

