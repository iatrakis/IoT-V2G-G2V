import time
import sys
import os
import logging
import random
from logging.handlers import TimedRotatingFileHandler
from pathlib import Path
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

BROKER = os.environ.get('BROKER', 'localhost')
CLIENT_NAME = os.environ.get('CLIENT_NAME', '')
username = os.environ.get('username', '')
password = os.environ.get('password', '')
run_param = os.environ.get('run_params', '')

clients=[]
producers=[]
producer_type = ["Wind_Gen(MW)","Solar_Gen(MW)"]
nclients=len(producer_type)


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
output_no = 3

in_topic = [os.environ.get(f'in_topic_{i}', '') for i in range(1,input_no+1) ]
out_topic = [os.environ.get(f'out_topic_{i}', '') for i in range(1,output_no+1) ]

logger.info(f'Credentials: {username}:{password}')
logger.info(f'BROKER: {BROKER}')
print(f'Credentials: {username}:{password}')
print(f'BROKER: {BROKER}')
for i,t in enumerate(in_topic):
    logger.info(f'in_topic_{i+1}: {t}')
for i,t in enumerate(out_topic):
    logger.info(f'out_topic_{i+1}: {t}')

FREQUENCY = 24
RANGE = 100
PAY_AMOUNT = 200
sum = 0
# p1 = my_util_pydantic.PowerSlot(kwh=1)
started = False
producer0 = my_util_pydantic.ElectricityProducer(id="EP10000")
CONFIDENCE = 1

##
## Define callbacks
##
## You can describe topic-specific behaviour here:
def on_message(client, userdata, message):
    global FREQUENCY
    global RANGE
    global sum
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
            # print("on_message on_message on_message on_message")
            
        

    ID = my_util_pydantic.find_ID(in_topic[0+extra],message.topic)
    if ID is not None and ID==producers[pid].id:
        
        producers[pid].ReceiveRegistrationOutcome( r_msg,message.topic)
        logger.info("producers[pid].reg: {0}".format(producers[pid].reg))
        
        if producers[pid].reg == {'EI': 'SUCCESS', 'MD': 'SUCCESS'}:
            msg0 = producers[pid].SendUpdateExpectedEnergyProfile(day=1,column= producer_type[pid])
            client.publish(out_topic[1].replace('+', producers[pid].id),msg0)
            msg1 = producers[pid].SendUpdateEnergyProfileConfidence(day=1,column= producer_type[pid])
            client.publish(out_topic[2].replace('+', producers[pid].id),msg1)
    
    ID = my_util_pydantic.find_ID(in_topic[1+extra],message.topic)
    if ID is not None and ID==producers[pid].id:
        producers[pid].ReceiveRegistrationOutcome( r_msg,message.topic)
        logger.info("producers[pid].reg: {0}".format(producers[pid].reg))
        
        if producers[pid].reg == {'EI': 'SUCCESS', 'MD': 'SUCCESS'}:
            msg0 = producers[pid].SendUpdateExpectedEnergyProfile(day=1,column= producer_type[pid])
            client.publish(out_topic[1].replace('+', producers[pid].id),msg0)
            msg1 = producers[pid].SendUpdateEnergyProfileConfidence(day=1,column= producer_type[pid])
            client.publish(out_topic[2].replace('+', producers[pid].id),msg1)
            # client.publish("EP/EP123/UpdateExpectedProduction","UPDATE day 1 test")

            
    ID = my_util_pydantic.find_ID(in_topic[2+extra],message.topic)
    if ID is not None:
        producers[pid].ReceiveUpdateProfileOutcome( r_msg,message.topic)
        logger.info("producers[pid].reg: {0}".format(producers[pid].reg))
    
    ID = my_util_pydantic.find_ID(in_topic[3+extra],message.topic)
    if ID is not None:
        producers[pid].ReceiveUpdateProfileOutcome( r_msg,message.topic)
        logger.info("producers[pid].reg: {0}".format(producers[pid].reg))

    ID = my_util_pydantic.find_ID(in_topic[4+extra],message.topic)
    if ID is not None:
        producers[pid].ReceiveUpdateConfidenceOutcome( r_msg,message.topic)
        logger.info("producers[pid].reg: {0}".format(producers[pid].reg))
    
    ID = my_util_pydantic.find_ID(in_topic[5+extra],message.topic)
    if ID is not None:
        producers[pid].ReceiveUpdateConfidenceOutcome( r_msg,message.topic)
        logger.info("producers[pid].reg: {0}".format(producers[pid].reg))

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
        pid = int(client._client_id.decode('ascii').split("client")[1]) 
        
        success=False
        msg0 = producers[pid].SendRegistrationRequest(column= producer_type[pid])
        success = client.publish(out_topic[0].replace('+', producers[pid].id),msg0)
        # time.sleep(1)
        # if success:
        #     time.sleep(1)
        #     msg0 = producers[pid].SendUpdateExpectedEnergyProfile(day=1,column= producer_type[pid])
        #     client.publish(out_topic[1].replace('+', producers[pid].id),msg0)

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

   producer = my_util_pydantic.ElectricityProducer(id="EP"+str(i))
   producers.append(producer)
   


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
        sys.exit('Error connecting to broker, error: {0}'.format(ex))

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
    print("calculate production")
    print("send expected production")
    my_util_pydantic.sim_now+=  my_util_pydantic.time_step*24
    for (client,producer) in zip(clients,producers): 
        pid = int(client._client_id.decode('ascii').split("client")[1]) 
        msg0 = producer.SendUpdateExpectedEnergyProfile(day=day,column= producer_type[pid])
        client.publish(out_topic[1].replace('+', producer.id),msg0)
        msg1 = producer.SendUpdateEnergyProfileConfidence(day=day,column= producer_type[pid])
        client.publish(out_topic[2].replace('+', producer.id),msg1)
    
while True:
    pass
    if started:
        break

schedule(daily_routine, interval=24*time_mult)
run_loop()
""" schedule(daily_routine, interval=24)

# run_loop(return_after=30)


while True:
     print(datetime.datetime.now())
     time.sleep(0.01)
     if (datetime.datetime.now().second == 0 or datetime.datetime.now().second == 30)  and started:
        break
run_loop()
print("start loop !!!!")



# while(not started):
#     pass

logger.info("while loop stopped")   
 """
# i=0
# while True:
#     time.sleep(FREQUENCY)
#     i+=1
#     logger.info(f'i: {i}')
#     # random_int = random.randrange(RANGE)
#     # p1.set_kwh(float(secret_param))
#     # logger.info(f'secret_param: {secret_param}')

#     for (client,producer) in zip(clients,producers): 
        
#         msg0 = producer.SendUpdateExpectedEnergyProfile(day=i,column= "Wind_Gen(MW)")
#         client.publish(out_topic[1].replace('+', producer.id),msg0)

#         # msg0 = producer.SendUpdateEnergyProfileConfidence(day=i,column= "Wind_Gen(MW)")
#         # client.publish(out_topic[2].replace('+', producer.id),msg0)
       
        

