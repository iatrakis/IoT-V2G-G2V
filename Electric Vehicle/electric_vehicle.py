#gia test 1000 ev se 1 client

import time
import sys
import os
import logging
import random
from logging.handlers import TimedRotatingFileHandler
from pathlib import Path
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

try:
    import paho.mqtt.client as paho
except Exception as ex:
    logger.error('Paho library is not present')
    sys.exit('Paho library is not present')

paho.logging

BROKER =  os.environ.get('BROKER', 'localhost')
CLIENT_NAME = os.environ.get('CLIENT_NAME', '')
username = os.environ.get('username', '')
password = os.environ.get('password', '')
run_param = os.environ.get('run_params', '')

clients=[]
evs=[]
# ev_type = [1,2]
try:
    params = ast.literal_eval(run_param)
    ev_tens = params["ev_tens"]
    if ev_tens>300:
        ev_tens=100
except :
    ev_tens = 1


try:
    time_mult = params["time_mult"]
except:
    time_mult = 1

try:
    my_util_pydantic.linked_hours = params["linked_hours"]
except:
    my_util_pydantic.linked_hours = 2
ev_type = [x for x in range(1,11)]*ev_tens
# ev_type = [x for x in range(1,6)]*2
ev_type.sort()
# ev_type = [x for x in range(1,3)]*3
nclients=len(ev_type)
## 
##  /\ /\ /\ /\ /\ /\ /\ /\
## 



##
##  Add your TOPICS here
##  \/\/\/\/\/\/\/\/

input_no = 5
output_no = 4

in_topic = [os.environ.get(f'in_topic_{i}', '') for i in range(1,input_no+1) ]
out_topic = [os.environ.get(f'out_topic_{i}', '') for i in range(1,output_no+1) ]

logger.info(f'Credentials: {username}:{password}')
logger.info(f'BROKER: {BROKER}')
for i,t in enumerate(in_topic):
    logger.info(f'in_topic_{i+1}: {t}')
for i,t in enumerate(out_topic):
    logger.info(f'out_topic_{i+1}: {t}')

# ev0 = my_util_pydantic.ElectricVehicle()
started = False
def find_evid(topic):
  print(topic)
  topic_arr = topic.split("/")
  
  for i, t in enumerate(topic_arr):
    if t == "EV": #after EV follows evid
      try:
        ev_no = int(topic_arr[i+1].split("EV")[1]) -10001
        return ev_no
      except:
        return
  return
##
## Define callbacks
##
## You can describe topic-specific behaviour here:
def on_message(client, userdata, message):
    
    global started,last_departure,rand,day_range
    r_msg = str(message.payload.decode("utf-8"))
    """ logger.info("message topic: {0}".format(message.topic))
    logger.info("received message ={0}".format(r_msg)) """
    


    # id = find_ID(in_topic_1,message.topic)
    # if id is not None:
    # if(message.topic==in_topic[0]):
    #     # client.publish(out_topic_2.replace('+', ID),str(cs_list))
    #     cs_recommend_list = ast.literal_eval(r_msg)
    #     if len(cs_recommend_list) > 0:
    #         best_CSID = choose_best_cs(cs_recommend_list)
    #         client.publish(out_topic[0].replace('+', best_CSID),"reservation details")
    extra=1

    
    if(message.topic==in_topic[0]):#in_topic[1]
        if(r_msg=="start"):
            started=True
            return

    evid = find_evid(message.topic)
    if evid is not None and (evid < 0 or evid > nclients):
        print("evid {0} ".format(evid))
        logger.info("evid {0} ".format(evid))
        print("mnm gia ton allo agent")
        logger.info("mnm gia ton allo agent")
        return
        

    print("evid {0} ".format(evid))
    print("evid {1} message topic: {0}".format(message.topic,evs[evid].id))
    print("evid {1} received message ={0}".format(r_msg,evs[evid].id))

    ID = my_util_pydantic.find_ID(in_topic[0+extra],message.topic)
    if ID is not None and ID == evs[evid].id:
        evs[evid].ReceiveRecommendations( r_msg,message.topic)
        """ logger.info("evs[{1}].recommendations: {0}".format(evs[evid].recommendations,evs[evid].id)) """
        print("evs[{1}].recommendations: {0}".format(evs[evid].recommendations,evs[evid].id))
        # if len(evs[evid].recommendations)>0:
        (tpc0,msg0 )= evs[evid].SendStationReservationRequest()
        client.publish(tpc0,msg0)
        # else:
        #     logger.info("EMPTY RECOMMENTATION")
    try:
        ID = my_util_pydantic.find_ID(in_topic[1+extra],message.topic)
        if ID is not None and ID == evs[evid].id:
            evs[evid].ReceiveReservationOutcome( r_msg,message.topic)
            """ logger.info("evs[{1}].reservations: {0}".format(evs[evid].reservations,evs[evid].id)) """
            print("evs[{1}].reservations: {0}".format(evs[evid].reservations,evs[evid].id))
            #an to reservation egine sosta vriskei to epomeno charging session
            last_departure[evid] = evs[evid].find_linked_session(last_departure[evid],last_departure[evid]+day_range*my_util_pydantic.time_interval)
            # last_departure[evid] = evs[evid].find_linked_session(last_departure[evid],last_departure[evid]+7*my_util_pydantic.time_interval)
            
            # last_departure[evid] = evs[evid].find_linked_session(last_departure[evid],last_departure[evid]+7*my_util_pydantic.time_interval)
            
            rand[evid]=random.randint(1,12)
            while my_util_pydantic.sim_now >= evs[evid].preferences.arrival-rand[evid]*my_util_pydantic.time_step:
                rand[evid]=random.randint(1,12)
            #allios prepei na ksanastelnei recommentation
    except Exception as e:
            print(f'ERROR on msg: {e}')
            logger.info(f'ERRORon msg: {e}')
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
        # evid = int(client._client_id.decode('ascii').split("client")[1])

       
        
        for t in in_topic:
            # if t == "EV/+/ChargingRecommendations":
            #     t="EV/+/ChargingRecommendations".replace('+', evs[evid].id)

            # if t =="EV/+/ReservationOutcome":
            #     t="EV/+/ReservationOutcome".replace('+', evs[evid].id)

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

for i  in range(nclients):
    # cname=CLIENT_NAME+str(i)
    # client= paho.Client(cname)
    # client.username_pw_set(username, password)
    # clients.append(client)
    # time.sleep(0.1)
    ev = my_util_pydantic.ElectricVehicle(ev_type=ev_type[i])
    # producer = my_util_pydantic.ElectricityProducer(id="EP"+str(i))
    evs.append(ev)


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
    client.connect(BROKER)

    
    # client.connect(BROKER,keepalive=1*60)#connect
    
except Exception as ex:
    logger.error('Error connecting to broker, error: {0}'.format(ex))
    sys.exit('Error connecting to broker')

client.loop_start()
# client.loop_forever()
last_departure = [0 for _ in range(nclients)]
day_range = 7


for i in range(nclients):
    try:
        last_departure[i] = evs[i].find_linked_session(last_departure[i],last_departure[i]+day_range*my_util_pydantic.time_interval)
    except Exception as e:
        print(f'ERROR: {e}')
        logger.info(f'ERROR: {e}')
    # last_departure[i] = evs[i].find_linked_session(last_departure[i],last_departure[i]+day_range*my_util_pydantic.time_interval)
day=0    
start_time = time.time()
rand=[2 for _ in range(nclients)]

def daily_routine():
    global day,last_departure
    dt = time.time() - start_time
    print(f"Started a *slow* day task at t={dt:.5f}")
    day +=1
    print("DAYYYY sim_now",day)
    logger.info(f'day: {day}')
    # my_util_pydantic.sim_now+=  my_util_pydantic.time_step*24
    logger.info(f'my_util_pydantic.sim_now: {my_util_pydantic.sim_now}')
    print(f'my_util_pydantic.sim_now: {my_util_pydantic.sim_now}')
    # for ev in evs:
    #     client.publish(out_topic[0].replace('+', ev.id),"test"+ev.id)

    
    # last_departure = ev0.find_linked_session(last_departure,last_departure+day_range*my_util_pydantic.time_interval)
    
    
    # msg0 = md.SendElectricityImbalanceRequest()
    # client.publish(out_topic[4].replace('+', "MD"),msg0)
    # time.sleep(2)
    # (topic, msg) = md.ReceiveElectiricityPricesRequest("lala","MD/CS0/ElectricityPricesRequest")
    # client.publish(topic,msg)
def hourly_routine():
    global last_departure,rand
    my_util_pydantic.sim_now+=  my_util_pydantic.time_step*1
    # print(str(my_util_pydantic.sim_now)+" "+str(my_util_pydantic.sim_now == ev0.preferences.arrival-4*my_util_pydantic.time_step))
    # logger.info(str(my_util_pydantic.sim_now)+" "+str(my_util_pydantic.sim_now == ev0.preferences.arrival-4*my_util_pydantic.time_step))
    for i,ev in enumerate(evs):
        if my_util_pydantic.sim_now == ev.preferences.arrival-rand[i]*my_util_pydantic.time_step:
            logger.info(f'rand:                 {rand[i]}')
            print(f'rand:                        {rand[i]}')
            logger.info(f'GINETAI RECOMMENTATION: {my_util_pydantic.sim_now}')
            print(f'GINETAI RECOMMENTATION: {my_util_pydantic.sim_now}')
            logger.info(f'ev.preferences.arrival: {ev.preferences.arrival}')
            print(f'ev.preferences.arrival: {ev.preferences.arrival}')
            # time.sleep(0.3)
            msg0 = ev.SendChargingRecommendationsRequest()
            client.publish(out_topic[0].replace('+', ev.id),msg0)
            print("EDWW",out_topic[0].replace('+', ev.id),msg0)
        
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
my_util_pydantic.sim_now+=  my_util_pydantic.time_step*24
run_loop()
 """
# while True:
#     time.sleep(10)
#     last_departure = ev0.find_linked_session(last_departure,last_departure+day_range*my_util_pydantic.time_interval)
    
#     msg0 = ev0.SendChargingRecommendationsRequest()
#     client.publish(out_topic[0].replace('+', ev0.id),msg0)
#     time.sleep(10)
#     (tpc0,msg0 )= ev0.SendStationReservationRequest()
#     client.publish(tpc0,msg0)
# while True:
#     time.sleep(10)
#     msg0 = ev0.SendChargingRecommendationsRequest()
#     client.publish(out_topic[0].replace('+', ev0.id),msg0)
#     time.sleep(10)
#     (tpc0,msg0 )= ev0.SendStationReservationRequest()
#     client.publish(tpc0,msg0)
   

