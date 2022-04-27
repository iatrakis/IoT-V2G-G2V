import time
import sys
import os
import logging
import random
from logging.handlers import TimedRotatingFileHandler
from pathlib import Path
import json
from ischedule import schedule, run_loop
import datetime
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

"""http://www.steves-internet-guide.com/multiple-client-connections-python-mqtt/"""
clients=[]
consumers=[]
nclients=1
## 
##  /\ /\ /\ /\ /\ /\ /\ /\
## 



##
##  Add your TOPICS here
##  \/\/\/\/\/\/\/\/

input_no = 7
output_no = 3

in_topic = [os.environ.get(f'in_topic_{i}', '') for i in range(1,input_no+1) ]
out_topic = [os.environ.get(f'out_topic_{i}', '') for i in range(1,output_no+1) ]


logger.info(f'Credentials: {username}:{password}')
logger.info(f'BROKER: {BROKER}')
for i,t in enumerate(in_topic):
    logger.info(f'in_topic_{i+1}: {t}')
for i,t in enumerate(out_topic):
    logger.info(f'out_topic_{i+1}: {t}')

p1 = my_util_pydantic.PowerSlot(kwh=1)
consumer0 = my_util_pydantic.ElectricityConsumer(id="EC10")

FREQUENCY = 24
RANGE = 100

CONFIDENCE = 1

TIME = 0
PRICE = 2
PRODUCTION = 0
started = False
history = []
log ={"time":0,"consuption":0,"price":2}

detailed_history = []
detailed_log ={"time":0,"consuption":0,"price":2}
##
## Define callbacks
##
## You can describe topic-specific behaviour here:
def on_message(client, userdata, message):
    global FREQUENCY
    global RANGE
    global started
    r_msg = str(message.payload.decode("utf-8"))
    logger.info("message topic: {0}".format(message.topic))
    logger.info("received message ={0}".format(r_msg))

    extra=1
    pid = int(client._client_id.decode('ascii').split("client")[1])
    if(message.topic==in_topic[0]):
        if(r_msg=="start"):
            # schedule(daily_routine, interval=24)
            # run_loop()
            started=True
            # logger.info("on_message on_message on_message on_message")
            
        

    """"na tsekaro aneksarita tou id px find_ID oxi =="""
    ID = my_util_pydantic.find_ID(in_topic[0+extra],message.topic)
    if ID is not None and ID==consumers[pid].id:
        consumers[pid].ReceiveRegistrationOutcome( r_msg,message.topic)
        logger.info("consumers[{0}].reg: {1}".format(pid,consumers[pid].reg))
        if consumers[pid].reg == {'EI': 'SUCCESS', 'MD': 'SUCCESS'}:
            msg0 = consumers[pid].SendUpdateExpectedEnergyProfile(day=1,column= "Total_Load(MW)")
            client.publish(out_topic[1].replace('+', consumers[pid].id),msg0)
            msg1 = consumer.SendUpdateEnergyProfileConfidence(day=day,column= "Total_Load(MW)")
            client.publish(out_topic[2].replace('+', consumer.id),msg1)

    ID = my_util_pydantic.find_ID(in_topic[1+extra],message.topic)
    if ID is not None and ID==consumers[pid].id:
        consumers[pid].ReceiveRegistrationOutcome( r_msg,message.topic)
        logger.info("consumers[{0}].reg: {1}".format(pid,consumers[pid].reg))
        if consumers[pid].reg == {'EI': 'SUCCESS', 'MD': 'SUCCESS'}:
            msg0 = consumers[pid].SendUpdateExpectedEnergyProfile(day=1,column= "Total_Load(MW)")
            client.publish(out_topic[1].replace('+', consumers[pid].id),msg0)
            msg1 = consumer.SendUpdateEnergyProfileConfidence(day=day,column= "Total_Load(MW)")
            client.publish(out_topic[2].replace('+', consumer.id),msg1)

    ID = my_util_pydantic.find_ID(in_topic[2+extra],message.topic)
    if ID is not None:
        consumer0.ReceiveUpdateProfileOutcome( r_msg,message.topic)
        logger.info("consumer0.reg: {0}".format(consumer0.reg))
    
    ID = my_util_pydantic.find_ID(in_topic[3+extra],message.topic)
    if ID is not None:
        consumer0.ReceiveUpdateProfileOutcome( r_msg,message.topic)
        logger.info("consumer0.reg: {0}".format(consumer0.reg))

    ID = my_util_pydantic.find_ID(in_topic[4+extra],message.topic)
    if ID is not None:
        consumer0.ReceiveUpdateConfidenceOutcome( r_msg,message.topic)
        logger.info("consumer0.reg: {0}".format(consumer0.reg))
    
    ID = my_util_pydantic.find_ID(in_topic[5+extra],message.topic)
    if ID is not None:
        consumer0.ReceiveUpdateConfidenceOutcome( r_msg,message.topic)
        logger.info("consumer0.reg: {0}".format(consumer0.reg))
    
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
        #init
        # msg0 = consumer0.SendRegistrationRequest()
        # client.publish(out_topic[0].replace('+', consumer0.id),msg0)
        pid = int(client._client_id.decode('ascii').split("client")[1])
        success=False
        msg0 = consumers[pid].SendRegistrationRequest(column= "Total_Load(MW)")
        success=client.publish(out_topic[0].replace('+', consumers[pid].id),msg0)
        # time.sleep(1)
        # if success:
        #     time.sleep(1)
        #     msg0 = consumers[pid].SendUpdateExpectedEnergyProfile(day=1,column= "Total_Load(MW)")
        #     client.publish(out_topic[1].replace('+', consumers[pid].id),msg0)
        

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



# client= paho.Client(CLIENT_NAME) 
# client.username_pw_set(username, password)

for i  in range(nclients):
   cname=CLIENT_NAME+str(i)
   client= paho.Client(cname)
   client.username_pw_set(username, password)
   clients.append(client)

   consumer = my_util_pydantic.ElectricityConsumer(id="EC"+str(i))
   consumers.append(consumer)
   


for client in clients:
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

    client.loop_start()

day=1    
start_time = time.time()

def daily_routine():
    global day
    dt = time.time() - start_time
    print(f"Started a *slow* day task at t={dt:.5f}")
    day +=1
    print("DAYYYY sim_now",day)
    logger.info(f'day: {day}')
    print("calculate consumption")
    print("send expected consumption")
    my_util_pydantic.sim_now+=  my_util_pydantic.time_step*24
    for (client,consumer) in zip(clients,consumers): 
        
        msg0 = consumer.SendUpdateExpectedEnergyProfile(day=day,column= "Total_Load(MW)")
        client.publish(out_topic[1].replace('+', consumer.id),msg0)
        msg1 = consumer.SendUpdateEnergyProfileConfidence(day=day,column= "Total_Load(MW)")
        client.publish(out_topic[2].replace('+', consumer.id),msg1)
        

while True:
    pass
    if started:
        break
schedule(daily_routine, interval=24*time_mult)
run_loop()


# run_loop(return_after=30)
# run_loop()
""" 
while True:
     print(datetime.datetime.now())
     time.sleep(0.01)
     if (datetime.datetime.now().second == 0 or datetime.datetime.now().second == 30)  and started:
        break
run_loop() """

# while(not started):
#     pass

# logger.info("while loop stopped")   
# run_loop()



# i=0
# while True:
#     time.sleep(FREQUENCY)
#     i+=1
#     # random_int = random.randrange(RANGE)   #was negative
 
#     # msg0 = consumer0.SendUpdateExpectedEnergyProfile()
#     # client.publish(out_topic[1].replace('+', consumer0.id),msg0)
#     for (client,consumer) in zip(clients,consumers): 
        
#         msg0 = consumer.SendUpdateExpectedEnergyProfile(day=i,column= "Total_Load(MW)")
#         client.publish(out_topic[1].replace('+', consumer.id),msg0)

#         # msg0 = consumer.SendUpdateEnergyProfileConfidence(day=i,column= "Total_Load(MW)")
#         # client.publish(out_topic[2].replace('+', consumer.id),msg0)
   