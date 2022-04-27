###station o palios me pollous clients gia polla CSs
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
    # pass
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
print(run_param)
try:
    params = ast.literal_eval(run_param)
    time_mult = params["time_mult"]
except Exception as e: 
    print(e)
    time_mult = 1


try:
    nclients = params["cs_num"]
except Exception as e: 
    print(e)
    nclients = 8

try:
    cs_slots = params["cs_slots"]
except:
    cs_slots = 10

try:
    my_util_pydantic.sched_algo = params["sched_algo"]
except:
    my_util_pydantic.sched_algo = 2

clients=[]
stations=[]
# cs_type = [1,2,3,4,5,6,7,8,9,10]
cs_type = [1,2,3,4,5,6,7,8]
# cs_type = [x for x in range(1,11)]*2
# cs_type.sort()
# cs_type = [x for x in range(1,3)]*3
# nclients=len(cs_type)

## 
##  /\ /\ /\ /\ /\ /\ /\ /\
## 

input_no = 15
output_no = 10

in_topic = [os.environ.get(f'in_topic_{i}', '') for i in range(1,input_no+1) ]
out_topic = [os.environ.get(f'out_topic_{i}', '') for i in range(1,output_no+1) ]

logger.info(f'Credentials: {username}:{password}')
logger.info(f'BROKER: {BROKER}')
for i,t in enumerate(in_topic):
    logger.info(f'in_topic_{i+1}: {t}')
for i,t in enumerate(out_topic):
    logger.info(f'out_topic_{i+1}: {t}')

IMBALANCE=0

PRICE=1
started = False

# station0 = my_util_pydantic.ChargingStation(id="CS1")
# station0.location=my_util_pydantic.Location(latitude=40,longitude=40)
# station0.add_charging_slots(11,ratedPower=120.0, connectorType='3 phase-DC', category='level 3')
# station0.add_charging_slots(11,ratedPower=50.0, connectorType='3 phase-DC', category='level 3')
# station0.add_charging_slots(11,ratedPower=43.0, connectorType='3 phase-60A per phase', category='level 3')
# station0.add_charging_slots(11,ratedPower=22.0, connectorType='3 phase-32A per phase', category='level 2')
# station0.add_charging_slots(11,ratedPower=11.0, connectorType='3 phase-16A per phase', category='level 2')
# station0.add_charging_slots(11,ratedPower=7.4, connectorType='Single phase 32A', category='level 2')
# station0.add_charging_slots(11,ratedPower=3.7, connectorType='Single phase 16A', category='level 1')
# station0.add_charging_slots(11,ratedPower=3.0, connectorType='Regular electricity socket 13A', category='level 1')

FREQUENCY = 2
##
## Define callbacks
##
## You can describe topic-specific behaviour here:
def on_message(client, userdata, message):
    # global FREQUENCY
    # global RANGE
    global IMBALANCE
    global PRICE
    global started
    r_msg = str(message.payload.decode("utf-8"))
    logger.info("message topic: {0}".format(message.topic))
    logger.info("received message ={0}".format(r_msg))


    extra=1

    csid = int(client._client_id.decode('ascii').split("client")[1])
    
    print("csid {1} message topic: {0}".format(message.topic,stations[csid].id))
    print("csid {1} received message ={0}".format(r_msg,stations[csid].id))


    if(message.topic==in_topic[2]):#in_topic[1]
        if(r_msg=="start"):
            started=True
            return
    ID = my_util_pydantic.find_ID(in_topic[2+extra],message.topic)
    if ID is not None:
        stations[csid].ReceiveRegistrationOutcome( r_msg,message.topic)
        logger.info("stations[csid].reg: {0}".format(stations[csid].reg))

    ID = my_util_pydantic.find_ID(in_topic[3+extra],message.topic)
    if ID is not None:
        stations[csid].ReceiveRegistrationOutcome( r_msg,message.topic)
        logger.info("stations[csid].reg: {0}".format(stations[csid].reg))

    ID = my_util_pydantic.find_ID(in_topic[4+extra],message.topic)
    if ID is not None:
        stations[csid].ReceiveRegistrationOutcome( r_msg,message.topic)
        logger.info("stations[csid].reg: {0}".format(stations[csid].reg))

    ID = my_util_pydantic.find_ID(in_topic[0],message.topic)
    if ID is not None:
        stations[csid].ReceiveUpdateConfidenceOutcome( r_msg,message.topic)
        logger.info("stations[csid].reg: {0}".format(stations[csid].reg))
    
    ID = my_util_pydantic.find_ID(in_topic[1],message.topic)
    if ID is not None:
        stations[csid].ReceiveUpdateConfidenceOutcome( r_msg,message.topic)
        logger.info("stations[csid].reg: {0}".format(stations[csid].reg))
    
    ID = my_util_pydantic.find_ID(in_topic[11+extra],message.topic)
    if ID is not None:
        stations[csid].ReceiveUpdateScheduleOutcome( r_msg,message.topic)
        logger.info("stations[csid].reg: {0}".format(stations[csid].reg))
    
    ID = my_util_pydantic.find_ID(in_topic[12+extra],message.topic)
    if ID is not None:
        stations[csid].ReceiveUpdateScheduleOutcome( r_msg,message.topic)
        logger.info("stations[csid].reg: {0}".format(stations[csid].reg))
    
    ID = my_util_pydantic.find_ID(in_topic[13+extra],message.topic)
    if ID is not None:
        stations[csid].ReceiveUpdateAvailabilityOutcome( r_msg,message.topic)
        logger.info("stations[csid].reg: {0}".format(stations[csid].reg))
    
    ID = my_util_pydantic.find_ID(in_topic[9+extra],message.topic)
    if ID is not None:
    # if in_topic[9+extra]==message.topic:
        stations[csid].ReceiveElectricityPrices( r_msg,message.topic)
        logger.info("stations[csid]._buy_prices: {0}".format(stations[csid]._buy_prices))
        logger.info("stations[csid]._sell_prices: {0}".format(stations[csid]._sell_prices))
    
    ID = my_util_pydantic.find_ID(in_topic[10+extra],message.topic)
    # if in_topic[10+extra]==message.topic:
    if ID is not None :
    # if ID is not None and ID.startswith("CS"):
        stations[csid].RecieveElectricityImbalance( r_msg,message.topic)
        logger.info("stations[csid].imbalance: {0}".format(stations[csid].imbalance))
    
    ID = my_util_pydantic.find_ID(in_topic[5+extra],message.topic)
    if ID is not None:
        (tpc0,msg0) = stations[csid].ReceiveChargingReservationRequest( r_msg,message.topic)
        client.publish(tpc0,msg0)
        #if to reservarion einai true tote stations[csid].scheduling(reservation)
        # stations[csid].scheduling()
        # print("stations[csid].scheduling()",stations[csid].schedules)
        # logger.info("stations[csid].scheduling()",stations[csid].schedules)

    ID = my_util_pydantic.find_ID(in_topic[8+extra],message.topic)
    if ID is not None:
        (tpc0,msg0) = stations[csid].ReceiveAuthenticationResponse( r_msg,message.topic)
        client.publish(tpc0,msg0)
        #an einai success
        (tpc1,msg1)=stations[csid].SendUpdatedChargingSchedule(msg0,tpc0)
        client.publish(tpc1,msg1)
        
        (tpc2,msg2)=stations[csid].SendUpdatedStationAvailability(msg0,tpc0)
        client.publish(tpc2,msg2)
        
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
black_list =   ["CS/+/EV/+/EV_start/start_negotiation",
                "CS/+/EV/+/CS_start/accept_negotiation",
                "timer",
                "MD/ElectricityPrices",
                "EI/ElectricityImbalance"]
def on_connect(client, userdata, flags, rc):
    logger.info(f'Conencting ... with : {username}:{password}')
    if(rc==0):
        logger.info("connecting to broker {0}".format(BROKER))
        logger.info("Subscribing to (input) topics")
        ##
        ##  Add subscription to your TOPICS here
        ##  \/\/\/\/\/\/\/\/
        
        csid = int(client._client_id.decode('ascii').split("client")[1])

        for t in in_topic:
            if not t in black_list:
                client.subscribe(t.replace('+',stations[csid].id))
            else:
                client.subscribe(t)
         #init
        msg0 = stations[csid].SendChargingStationRegistration()
        client.publish(out_topic[1].replace('+', stations[csid].id),msg0)
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

    # ev = my_util_pydantic.ElectricVehicle(ev_type=ev_type[i])
    station = my_util_pydantic.ChargingStation()
    # station = my_util_pydantic.ChargingStation(id="CS"+str(cs_type[i]))
    # station.location=my_util_pydantic.Location(latitude=40,longitude=40)
    station.add_charging_slots(cs_slots,ratedPower=120.0, connectorType='3 phase-DC', category='level 3')
    station.add_charging_slots(cs_slots,ratedPower=50.0, connectorType='3 phase-DC', category='level 3')
    station.add_charging_slots(cs_slots,ratedPower=43.0, connectorType='3 phase-60A per phase', category='level 3')
    station.add_charging_slots(cs_slots,ratedPower=22.0, connectorType='3 phase-32A per phase', category='level 2')
    station.add_charging_slots(cs_slots,ratedPower=11.0, connectorType='3 phase-16A per phase', category='level 2')
    station.add_charging_slots(cs_slots,ratedPower=7.4, connectorType='Single phase 32A', category='level 2')
    station.add_charging_slots(cs_slots,ratedPower=3.7, connectorType='Single phase 16A', category='level 1')
    station.add_charging_slots(cs_slots,ratedPower=3.0, connectorType='Regular electricity socket 13A', category='level 1')

    # producer = my_util_pydantic.ElectricityProducer(id="EP"+str(i))
    stations.append(station)
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
    # logger.info(f'my_util_pydantic.sim_now: {my_util_pydantic.sim_now}')

    """ time.sleep(0.1)
    if day == 1:
        client.publish(out_topic[7].replace('+', station0.id),"0")
    msg0 = station0.SendElectricityImbalanceRequest()
    client.publish(out_topic[7].replace('+', station0.id),"1")
    time.sleep(0.1)
    if day == 1:
        client.publish(out_topic[6].replace('+', station0.id),"0")
    msg0 = station0.SendElectricityPricesRequest()
    client.publish(out_topic[6].replace('+', station0.id),"1") """
    

    # msg0 = station0.SendElectricityImbalanceRequest()
    # client.publish(out_topic[4].replace('+', station0.id),msg0)
    # time.sleep(2)
    # (topic, msg) = md.ReceiveElectiricityPricesRequest("lala","MD/CS0/ElectricityPricesRequest")
    # client.publish(topic,msg)

    # (topic, msg) = imba.ReceiveElectricityImbalanceRequest("mlpa","EI/MD/ElectricityImbalanceRequest")
    # client.publish(topic,msg)
def hourly_routine():
    global day
    my_util_pydantic.sim_now+=  my_util_pydantic.time_step
    print(my_util_pydantic.sim_now)
    logger.info(f'{my_util_pydantic.sim_now}')
    """ for i,(client,station) in enumerate(zip(clients,stations)):#UPDATE ME MD/Prices
        station.payment_calculation()  
        if  my_util_pydantic.sim_now.hour==1:
            if day == 0:
                client.publish(out_topic[7].replace('+', station.id),"0")
            msg0 = station.SendElectricityImbalanceRequest()
            client.publish(out_topic[7].replace('+', station.id),"1")
        
        if  my_util_pydantic.sim_now.hour==2:
            if day == 0:
                client.publish(out_topic[6].replace('+', station.id),"0")
            msg0 = station.SendElectricityPricesRequest()
            client.publish(out_topic[6].replace('+', station.id),"1") """
    #     msg0 = station.SendElectricityPricesRequest()
    #     client.publish(out_topic[6].replace('+', station.id),"1")
    #     if day == 1:
    #         client.publish(out_topic[6].replace('+', station.id),"0")

# my_util_pydantic.sim_now-=  my_util_pydantic.time_step*24
while True:
    pass
    if started:
        break
schedule(daily_routine, interval=24*time_mult)
schedule(hourly_routine, interval=1*time_mult)
run_loop()
