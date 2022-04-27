import time
import sys
import os
import logging
import random
from logging.handlers import TimedRotatingFileHandler
from pathlib import Path
import math
import ast
from ischedule import schedule, run_loop
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

md = my_util_pydantic.MechanismDesign()

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

logger.info(run_param)
try:
    params = ast.literal_eval(run_param)
    time_mult = params["time_mult"]
except Exception as e: 
    print(e)
    time_mult = 1

try:
    my_util_pydantic.nrgcoin_algo = ast.literal_eval(params["nrgcoin_algo"])
    
    print(my_util_pydantic.nrgcoin_algo)
    print(type(my_util_pydantic.nrgcoin_algo))
    logger.info(my_util_pydantic.nrgcoin_algo)
    logger.info(type(my_util_pydantic.nrgcoin_algo))
except Exception as e: 
    logger.info(e)
    print(e)
    my_util_pydantic.nrgcoin_algo = True
## 
##  /\ /\ /\ /\ /\ /\ /\ /\
## 



##
##  Add your TOPICS here
##  \/\/\/\/\/\/\/\/

input_no = 12
output_no = 6

in_topic = [os.environ.get(f'in_topic_{i}', '') for i in range(1,input_no+1) ]
out_topic = [os.environ.get(f'out_topic_{i}', '') for i in range(1,output_no+1) ]


imbalance = {"total_supply":0,"total_demand":0}

PRICE = 2
TIME = 0
PRODUCTION = 0
started = False
broadcast_new_prices = False
reg=["CS1"]
##
## Define callbacks
##
## You can describe topic-specific behaviour here:
def on_message(client, userdata, message):
    global imbalance
    global PRICE
    global TIME
    global PRODUCTION
    global started,broadcast_new_prices
    r_msg = str(message.payload.decode("utf-8"))
    logger.info("message topic: {0}".format(message.topic))
    logger.info("received message ={0}".format(r_msg))
    

    extra=1

    if(message.topic==in_topic[1]):#in_topic[1]
        if(r_msg=="start"):
            started=True
            return

    ID = my_util_pydantic.find_ID(in_topic[9+extra],message.topic)#in_topic[9+extra]
    # if ID is not None and ID == "MD":
    if ID is not None :
    # if in_topic[9+extra]==message.topic:
        
        logger.info(" md.RecieveElectricityImbalance  md.RecieveElectricityImbalance  md.RecieveElectricityImbalance!!!!!!!!!!")
        print(" md.RecieveElectricityImbalance  md.RecieveElectricityImbalance  md.RecieveElectricityImbalance!!!!!!!!!!")

        md.RecieveElectricityImbalance( r_msg,message.topic)
        # logger.info("md.imbalance: {0}".format(md.imbalance))
        if broadcast_new_prices:
            # for id in reg:
            x = list(md.station_dict.keys())
            broadcast_new_prices = False
            
            tpc,msg33 = md.ReceiveElectiricityPricesRequest("0","MD/+/ElectricityPricesRequest".replace("+","CS10000000"))
            client.publish(tpc,msg33)
            tpc,msg33 = md.ReceiveElectiricityPricesRequest("1","MD/+/ElectricityPricesRequest".replace("+","CS10000000"))
            client.publish(tpc,msg33)
            # for id in x:
            #     tpc,msg33 = md.ReceiveElectiricityPricesRequest("0","MD/+/ElectricityPricesRequest".replace("+",id))
            #     client.publish(tpc,msg33)
            #     tpc,msg33 = md.ReceiveElectiricityPricesRequest("1","MD/+/ElectricityPricesRequest".replace("+",id))
            #     client.publish(tpc,msg33)


    ID = my_util_pydantic.find_ID(in_topic[1+extra],message.topic)
    if ID is not None:
        (topic, msg) = md.ReceiveRegistrationRequest(r_msg,message.topic)
        client.publish(topic,msg)
        # time.sleep(5)
        # #SendElectricityImbalanceRequest gia tin currunt day
        # client.publish(out_topic[4].replace('+', "MD"),"0")

    ID = my_util_pydantic.find_ID(in_topic[2+extra],message.topic)
    if ID is not None:
        (topic, msg) = md.ReceiveRegistrationRequest(r_msg,message.topic)
        client.publish(topic,msg)
        # time.sleep(5)
        # #SendElectricityImbalanceRequest gia tin currunt day
        # client.publish(out_topic[4].replace('+', "MD"),"0")

    ID = my_util_pydantic.find_ID(in_topic[3+extra],message.topic)
    if ID is not None:
        (topic, msg) = md.ReceiveChargingStationRequest(r_msg,message.topic)
        client.publish(topic,msg)

    ID = my_util_pydantic.find_ID(in_topic[4+extra],message.topic)
    if ID is not None:
        (topic, msg) = md.ReceiveUpdateExpectedProfileRequest(r_msg,message.topic)
        client.publish(topic,msg)

    ID = my_util_pydantic.find_ID(in_topic[5+extra],message.topic)
    if ID is not None:
        (topic, msg) = md.ReceiveUpdateExpectedProfileRequest(r_msg,message.topic)
        client.publish(topic,msg)

    ID = my_util_pydantic.find_ID(in_topic[6+extra],message.topic)
    if ID is not None:
        (topic, msg) = md.ReceiveUpdateConfidenceRequest(r_msg,message.topic)
        client.publish(topic,msg)

    ID = my_util_pydantic.find_ID(in_topic[7+extra],message.topic)
    if ID is not None:
        (topic, msg) = md.ReceiveUpdateConfidenceRequest(r_msg,message.topic)
        client.publish(topic,msg)

    ID = my_util_pydantic.find_ID(in_topic[0],message.topic)
    if ID is not None:
        (topic, msg) = md.ReceiveUpdateConfidenceRequest(r_msg,message.topic)
        client.publish(topic,msg)

    ID = my_util_pydantic.find_ID(in_topic[10+extra],message.topic)
    if ID is not None:
        
        (topic, msg) = md.ReceiveRequestUpdatedStationSchedule(r_msg,message.topic)
        client.publish(topic,msg)
        broadcast_new_prices = True

    ID = my_util_pydantic.find_ID(in_topic[8+extra],message.topic)
    if ID is not None:
        
        (topic, msg) = md.ReceiveElectiricityPricesRequest(r_msg,message.topic)
        client.publish(topic,msg)

    # ID = my_util_pydantic.find_ID("EI/+/ElectricityImbalance",message.topic)#in_topic[9+extra]
    # if ID is not None and ID == "MD":
    # # if ID is not None :
    #     client.publish("MD/EP100/UpdateConfidenceOutcome","MPAINEI STO 9")
    #     logger.info(" md.RecieveElectricityImbalance  md.RecieveElectricityImbalance  md.RecieveElectricityImbalance!!!!!!!!!!")
    #     print(" md.RecieveElectricityImbalance  md.RecieveElectricityImbalance  md.RecieveElectricityImbalance!!!!!!!!!!")

    #     md.RecieveElectricityImbalance( r_msg,message.topic)
    #     # logger.info("md.imbalance: {0}".format(md.imbalance))
        
    # if(message.topic==in_topic_11):
    # if(message.topic=="EP/EP1/request_payment"):
    # if(in_topic_11.split("/")[0]==message.topic.split("/")[0] and in_topic_11.split("/")[2]==message.topic.split("/")[2]):
    #     EPID=message.topic.split("/")[1]
    #     print("Recived a EP/EP1/request_payment msg !!!!")
    #     client.publish("EP/"+EPID+"/accept_payment","OK") 
    #     print("Send a EP/EP1/accept_payment OK msg !!!!")

    # ID = find_ID(in_topic[0],message.topic)
    # if ID is not None:
    #     # client.publish(out_topic_5.replace('+', ID),"OK")
    #     TIME+=1
    #     PRODUCTION += int(r_msg)
    #     if TIME >=6 : #24
            
    #         sell_production_price = Bt_sell (PRODUCTION,imbalance["total_supply"],imbalance["total_demand"])
    #         client.publish(out_topic[3].replace('+', ID),"TIME: "+str(TIME)+" PRODUCTION : "+str(PRODUCTION)+" PAY AMOUNT : "+str(sell_production_price))
            
    #         logger.info("TIME: "+str(TIME)+" PRODUCTION : "+str(PRODUCTION)+" PAY AMOUNT : "+str(sell_production_price))
    #         logger.info("imbalance[\"total_supply\"]: "+str(imbalance["total_supply"])+" imbalance[\"total_demand\"] : "+str(imbalance["total_demand"]))
    #         TIME = 0
    #         PRODUCTION = 0
    #         client.publish(out_topic[0],float(1)) 

    # if(message.topic==in_topic[5]):
    #     imbalance = ast.literal_eval(r_msg)
    #     # client.publish(out_topic_1,float(PRICE))

    # # if(message.topic==in_topic_3):
    # #     IMBALANCE -= float(r_msg)
    # if(message.topic==in_topic[4]):    
    #     client.publish(out_topic[1],float(PRICE)) 
    
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

        # time.sleep(10)
        # #SendElectricityImbalanceRequest gia tin currunt day
        # client.publish(out_topic[4].replace('+', "MD"),"0")

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

    """ time.sleep(0.1)
    if day == 1:
        client.publish(out_topic[4].replace('+', "MD"),"0")
    msg0 = md.SendElectricityImbalanceRequest()
    client.publish(out_topic[4].replace('+', "MD"),"1") """
    
    # time.sleep(2)
    # (topic, msg) = md.ReceiveElectiricityPricesRequest("lala","MD/CS0/ElectricityPricesRequest")
    # client.publish(topic,msg)
    # client.publish("MD/EP0/RegistrationOutcome","TATATATA")
def hourly_routine():
    global day
    my_util_pydantic.sim_now+=  my_util_pydantic.time_step
    print(my_util_pydantic.sim_now)
    logger.info(f'{my_util_pydantic.sim_now}')
      
    # if  my_util_pydantic.sim_now.hour==1:
    #     if day == 0:
    #         client.publish(out_topic[4].replace('+', "MD"),"0")
    #     msg0 = md.SendElectricityImbalanceRequest()
    #     client.publish(out_topic[4].replace('+', "MD"),"1")

    #otan einai 2 i ora "pairnei request apo ton CS10000" kai stelnei o MD ta prices
    if  my_util_pydantic.sim_now.hour==2:
        if day == 0: 
            tpc,msg33 = md.ReceiveElectiricityPricesRequest("0","MD/+/ElectricityPricesRequest".replace("+","CS10000000"))
            client.publish(tpc,msg33)
        tpc,msg33 = md.ReceiveElectiricityPricesRequest("1","MD/+/ElectricityPricesRequest".replace("+","CS10000000"))
        client.publish(tpc,msg33)
    
    

while True:
    pass
    if started:
        break
schedule(daily_routine, interval=24*time_mult)
schedule(hourly_routine, interval=1*time_mult)
run_loop()
""" 
schedule(daily_routine, interval=24)

# run_loop(return_after=30)
time.sleep(24)
time.sleep(24)
run_loop() """
# while True:
#     time.sleep(2)
   
#     # msg0 = producer0.SendUpdateEnergyProfileConfidence()
#     # client.publish(out_topic[2].replace('+', producer0.id),msg0)
#     msg0 = md.SendElectricityImbalanceRequest()
#     client.publish(out_topic[4].replace('+', "MD"),msg0)


