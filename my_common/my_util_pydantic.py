
import re
from time import sleep
import math
from datetime import date, datetime, time, timedelta,timezone
from typing import List, Optional
import ast
import numpy as np
import pandas as pd
from geopy import distance
import statistics
import pulp
import random

# import matplotlib.pyplot as plt

from pydantic import (
    BaseModel,
    PrivateAttr,
    # NegativeInt,
    # PositiveInt,
    # conint,
    # conlist,
    # constr,
    confloat,
    # ValidationError,
    # validator
)


global_cost = 0
tests = 0
cum_global_cost =[]
days = 1
time_step = timedelta(hours=1)#0.1666666666
time_intervals_per_hour = int((3600/time_step.seconds))
time_interval = 24*days*time_intervals_per_hour

sim_now = datetime(2017,1,1,0,0)#datetime.now()
sim_now+=timedelta(days=3*365)
start_sim = sim_now
start = 0


duration = 45#365
start_day = 0#3*365
end_day = start_day+duration

linked_hours = 4
offset = 0 #extra ores gia na min ginetai infinite recursion 10000000

linked_id = 3
charging_id = 1
driving_id = 2
idle_id = 0 
arrival_id = 7
departure_id = 8

max_recom = 3
sched_algo = 2
nrgcoin_algo = True

min2=1000000
max2=-1000000
def find_ID(in_topic,msg_topic):
    x=in_topic.split("/")
    y=msg_topic.split("/")

    if in_topic == msg_topic:
        return 1
    if not len(x)==len(y):
        return
    for a,b in zip(x,y): 
        # print(a)
        # print(b)
        if (a!=b and a=="+"):
            ID0=b
        if (a!=b and b=="+"):
            ID0=a
        if (a!=b and b!="+" and a!="+"):
            return   
    return ID0
def find_ID2(in_topic,msg_topic,sel):
    x=in_topic.split("/")
    y=msg_topic.split("/")

    if in_topic == msg_topic:
        return 1
    if not len(x)==len(y):
        return
    ID0=None
    for i in range(len(x)):
        # print(x[i])
        # print(y[i])
        if (x[i]!=y[i] and x[i]=="+"):
            if i>0 and x[i-1] == sel:
                ID0=y[i]
        if (x[i]!=y[i] and y[i]=="+"):
            if  i>0 and x[i-1] == sel:
                ID0=x[i]
        if (x[i]!=y[i] and y[i]!="+" and x[i]!="+"):
            return
    return ID0
def hourly_update_power(supply,msg):
    x = PowerSlot.str_to_PowerSlot(msg)
    # prosthontai oi expected KWh stin epomeni ora  
    supply[(x.dateTime.hour+1)%24] += x.kwh
     # tha prepei na midenizete i methepomeni ora mono mia fora
     #kai oxi kathe fora pou kanei kapoios add 
     # alla douleuei
    supply[(x.dateTime.hour+2)%24] = 0

def Bt_sell(qt_plus, Qt_plus, Qt_minus):
    try:
        denominator = math.exp(((Qt_plus-Qt_minus)/Qt_minus)**2)
        fraction = (0.2*qt_plus)/denominator
    except:
        fraction = 0
    total = 0.1*qt_plus + fraction
    return total

def Bt_buy(qt_minus, Qt_plus, Qt_minus):
    x = (0.65*Qt_minus)*qt_minus/(Qt_minus+Qt_plus)
    return x

def a_hat_calcutator(P_prime,c_prime):
    # print(c_prime)
    N=len(P_prime)
    # sum1 = sum([(P_prime[0]-P_prime[t])/(2*(c_prime[0]-c_prime[t])) for t in range(1,N) if not c_prime[0] == c_prime[t]])
    # a_hat = sum1/(N-1)
    # print("array1",array1)
    # print(a_hat)
    array1 = [(P_prime[0]-P_prime[t])/(2*(c_prime[0]-c_prime[t])) for t in range(1,N) if not c_prime[0] == c_prime[t] and not c_prime[t]==0]
    if len(array1)-1>0:
        a_hat2 = sum(array1)/(len(array1)-1)
    else:
        a_hat2 = -0.13

    if a_hat2>0:
        a_hat2=-a_hat2
    return a_hat2

def update_or_append(my_dict,my_ID2,list_obj):
    # if my_ID2 in my_dict:
    for a in list_obj:
        for i,y in enumerate(my_dict[my_ID2]):
            if a.dateTime == y.dateTime: #update kwh in known date
                my_dict[my_ID2][i] = a

        if a not in my_dict[my_ID2] and sim_now<=a.dateTime<sim_now + 2*time_interval*time_step:
            my_dict[my_ID2].append(a)        

    my_dict[my_ID2].sort(key=lambda y: y.dateTime)
    my_dict[my_ID2] = my_dict[my_ID2][-2*time_interval:]    

def update_or_append_list(my_list,list_obj):
    # if my_ID2 in my_dict:
    for a in list_obj:
        for i,y in enumerate(my_list):
            if a.dateTime == y.dateTime: #update kwh in known date
                my_list[i] = a

        if a not in my_list and sim_now<=a.dateTime<sim_now + 2*time_interval*time_step:
            my_list.append(a)        

    my_list.sort(key=lambda y: y.dateTime)
    # my_list = my_list[-2*time_interval:]    

def add_or_append_list(my_list,list_obj):
    
    for a in list_obj:
        added=False
        for i,y in enumerate(my_list):
            if a.dateTime == y.dateTime: #update kwh in known date
                if my_list[i].kwh>0 and a.kwh>0:
                    my_list[i].kwh += a.kwh
                    added=True
                if my_list[i].kwh<=0 and a.kwh<=0:
                    my_list[i].kwh += a.kwh
                    added=True


        if  not added and sim_now<=a.dateTime<sim_now + 2*time_interval*time_step:
            my_list.append(a)        
    my_list.sort(key=lambda y: y.dateTime)
    

class PowerSlot(BaseModel):
    dateTime: datetime = datetime.now()
    kwh: float = 0
    
    def next_timestep(self):
        self.dateTime += time_step

    def set_kwh(self,kwh):
        self.kwh = kwh
        self.next_timestep()

class TimePrice(BaseModel):
    dateTime: datetime = datetime.now()
    price: float = 0

    def next_timestep(self):
        self.dateTime += time_step

    def set_price(self,price):
        self.price = price
        self.next_timestep()

class Confidence(BaseModel):
    mean: float = 1
    std: float = 0.1
  
class ConfidenceSlot(BaseModel):
    dateTime:datetime = datetime.now()
    confidence:Confidence = Confidence()

    def next_timestep(self):
        self.dateTime += time_step

    def set_mean_std(self, mean, std):
        # self.confidence.mean = mean
        # self.confidence.std = std
        self.confidence = Confidence(mean=mean,std=std)
        self.next_timestep()
    

class ElectricProsumer(BaseModel):
    
    def SendRegistrationRequest(self,column):
        print("Send Registration Request")
        """
        print(self.id)
        p1=PowerSlot()
        print(p1)
        df2 = pd.read_csv('my_common\simple_producer2.csv',index_col=False)
        # print(df2.iloc[[0]])
        my_id= "EP0"

        print(df2.loc[df2['id'] == self.id])
        prod2 = df2.loc[df2['id'] == self.id].to_numpy()
        print(prod2)
        energy = list(prod2[0][1:])
        x = []
        for i in range(time_interval):
            p1.set_kwh(energy[i])
            x.append(p1.copy())
        msg = str([a.json() for a in x[-time_interval:]])
        """
        
        try:
            self._df = pd.read_csv('my_common\Generated_Data_EnergySourcesLoad-KDE-4year.csv',index_col=False, usecols= [column,'Day_of_Week','Time_of_Day'])
        except :
            self._df = pd.read_csv('Generated_Data_EnergySourcesLoad-KDE-4year.csv',index_col=False, usecols= [column,'Day_of_Week','Time_of_Day'])
        
        if column == "Total_Load(MW)":
            self._df['Total_Load(MW)'] = self._df['Total_Load(MW)']/10
            print(self._df)
            



        self._conf = self._df.groupby(['Day_of_Week','Time_of_Day']).agg(['mean', 'std'])
        self._conf.columns = ['_'.join(c) for c in self._conf.columns.values]
        print(self._conf)

        # x = np.array([a for a in range(1,24*7+1)])
        # y = self._conf[column+'_mean']
        # e = self._conf[column+'_std']

        # plt.errorbar(x, y,e,  linestyle='None', marker='.')
        # plt.plot(x,y)

        # plt.show()
        p1 = PowerSlot(dateTime= sim_now - time_step)
        
        day = 0
        print("DAY",day)
        consumption = self._df[day*time_interval:(day+1)*time_interval].to_numpy()
        print(consumption)
        x= []

        for cons in consumption:
            if column == "Total_Load(MW)":
                p1.set_kwh(cons[2]/1)
                x.append(p1.copy())
            else:
                p1.set_kwh(cons[2])
                x.append(p1.copy())

        msg = str([a.json() for a in x[-time_interval:]])
        print(msg)
        return msg
    def ReceiveRegistrationOutcome(self, msg, topic):
        print("Receive Registration Outcome")
        a = topic.split("/")
        print(a[0]+" "+self.id+" "+msg)
        self.reg[a[0]] = msg

    def SendChargingStationRegistration(self):
        print("Send Charging Station Registration Request")
        # p1=PowerSlot()
        # print(p1)
        # x = []
        # for i in range(time_interval):
        #     p1.set_kwh(5)
        #     x.append(p1.copy())
        # msg = str([a.json() for a in x[-time_interval:]])
        # msg = ChargingStation(id= self.id,stationNetwork=self.stationNetwork,location=self.location,chargingSlots=self.chargingSlots).json()
        msg = self.json()
        print(msg)
        return msg

    def SendUpdateExpectedEnergyProfile(self,day,column):
        print("Send Update Expected Energy Profile")
        """      
        p1=PowerSlot()
        print(p1)

        df2 = pd.read_csv('my_common\simple_producer2.csv',index_col=False)
        my_id= "EP0"

        print(df2.loc[df2['id'] == self.id])
        prod2 = df2.loc[df2['id'] == self.id].to_numpy()
        print(prod2)
        energy_profile=1 #rand
        energy = list(prod2[energy_profile][1:])

        x = []
        for i in range(time_interval):
            p1.set_kwh(energy[i])
            x.append(p1.copy())
        msg = str([a.json() for a in x[-time_interval:]])
        """
        # column = 'Total_Load(MW)'
        # try:
        #     df = pd.read_csv('my_common\Generated_Data_EnergySourcesLoad-KDE-4year.csv',index_col=False, usecols= [column])
        # except:
        #     df = pd.read_csv('Generated_Data_EnergySourcesLoad-KDE-4year.csv',index_col=False, usecols= [column])
        
        # p1 = PowerSlot(dateTime= sim_now - time_step+ timedelta(days=day) )
        p1 = PowerSlot(dateTime= start_sim - time_step+ timedelta(days=day))#tin auriani mera
        # day =0
        print("DAY",day)

        print("p1",p1)
        consumption = self._df[day*time_interval:(day+1)*time_interval].to_numpy()
        print("consumption",consumption)
        x= []

        for cons in consumption:
            if column == "Total_Load(MW)":
                p1.set_kwh(cons[2]/1)
                x.append(p1.copy())
            else:
                p1.set_kwh(cons[2])
                x.append(p1.copy())

        msg = str([a.json() for a in x[-time_interval:]])

        return msg
    def ReceiveUpdateProfileOutcome(self, msg, topic):
        print("Receive Update Profile Outcome")
        a = topic.split("/")
        print(a[0]+" "+self.id+" "+msg)
        self.reg[a[0]] = msg

    def SendUpdateEnergyProfileConfidence(self,day,column):
        print("Send Update Energy Profile Confidence")
        # p1=PowerSlot()
        

        # column = 'Total_Load(MW)'
        # try:
        #     df = pd.read_csv('my_common\Generated_Data_EnergySourcesLoad-KDE-4year.csv',index_col=False, usecols= ['Day_of_Week', 'Time_of_Day',column])
        # except:
        #     df = pd.read_csv('Generated_Data_EnergySourcesLoad-KDE-4year.csv',index_col=False, usecols= ['Day_of_Week', 'Time_of_Day',column])
        
        # con = ConfidenceSlot(dateTime= sim_now - time_step+ timedelta(days=day) )
        #  p1 = PowerSlot(dateTime= start_sim - time_step+ timedelta(days=day))
        con = ConfidenceSlot(dateTime= start_sim - time_step+ timedelta(days=day))
        # con = ConfidenceSlot(dateTime= sim_now - time_step)
        # day =0
        print("DAY",day)

        print("con",con)
        # print(self._df)
        # self._conf = self._df.groupby(['Day_of_Week','Time_of_Day']).agg(['mean', 'std'])
        # self._conf.columns = ['_'.join(c) for c in self._conf.columns.values]
        # print(self._conf)

        # x = np.array([a for a in range(1,24*7+1)])
        # y = self._conf['Total_Load(MW)_mean']
        # e = self._conf['Total_Load(MW)_std']

        # plt.errorbar(x, y,e,  linestyle='None', marker='.')
        # plt.plot(x,y)

        # plt.show()

        # tmp = df.loc[:, ['Arrival','Departure','Soc','Day_of_Week','Time_of_Day']]
        # self._conf = tmp.groupby(['Day_of_Week','Time_of_Day']).agg(['mean', 'std'])
        # self._conf.columns = ['_'.join(c) for c in self._conf.columns.values]

        # print( self._conf)
        x= []
        print(self._df)
        for i in range(1,time_interval+1):
            day_of_week = self._df['Day_of_Week'][day*time_interval]
            
            s = self._conf.loc[day_of_week, i]
            print(s[column+"_mean"],s[column+"_std"])
            con.set_mean_std(s[column+"_mean"],s[column+"_std"])
            x.append(con.copy())
            

        msg = str([a.json() for a in x[-time_interval:]])

        print(msg)
        return msg
    def ReceiveUpdateConfidenceOutcome(self, msg, topic):
        print("Receive Update Confidence Outcome")
        a = topic.split("/")
        print(a[0]+" "+self.id+" "+msg)
        self.reg[a[0]] = msg

    
    

class ElectricityConsumer(BaseModel): #ElectricProsumer
    id: str
    consumptionUnit: str = "KWh"
    consumptionType: str = "Residential"
    reg = {}
    _df=PrivateAttr(0)
    _conf=PrivateAttr(0)
    

    def consumption(self):
        return PowerSlot(5)
    def confidence(self):
        return ConfidenceSlot(1,0.05)


    """    Producer Consumer Registration Protocol 6.10 """
    def SendRegistrationRequest(self,column):
        return ElectricProsumer.SendRegistrationRequest(self,column)
        

    def ReceiveRegistrationOutcome(self, msg, topic):
        ElectricProsumer.ReceiveRegistrationOutcome(self, msg, topic)
        print(self.reg)
        # print("Receive Registration Outcome")
        # a = topic.split("/")
        # print("LALALALALALA"+a[0]+" "+self.id+" "+msg)

    
    """"    Expected Production Consumption Update Protocol 6.4 """
    def SendUpdateExpectedEnergyProfile(self,day,column):
        print("Send Update Expected Energy Profile")
        return ElectricProsumer.SendUpdateExpectedEnergyProfile(self,day,column)

    def ReceiveUpdateProfileOutcome(self, msg, topic):
        print("Receive Update Profile Outcome")
        ElectricProsumer.ReceiveUpdateProfileOutcome(self, msg, topic)
        print(self.reg)

    """"    Update Energy Profile Confidence Protocol 6.11"""
    def SendUpdateEnergyProfileConfidence(self,day,column):
        print("Send Update Energy Profile Confidence")
        return ElectricProsumer.SendUpdateEnergyProfileConfidence(self,day,column)
    def ReceiveUpdateConfidenceOutcome(self, msg, topic):
        print("Receive Update Confidence Outcome")
        ElectricProsumer.ReceiveUpdateConfidenceOutcome(self, msg, topic)
        print(self.reg)

        
class ElectricityProducer(BaseModel): #ElectricProsumer
    id: str
    productionUnit: str = "KWh"
    productionType: str = "PV Panel"
    reg = {}
    _df=PrivateAttr(0)
    _conf=PrivateAttr(0)

    def production(self):
        return PowerSlot(5)
    def confidence(self):
        return ConfidenceSlot(1,0.05)
    
    """    Producer Consumer Registration Protocol 6.10 """
    def SendRegistrationRequest(self,column):
        return ElectricProsumer.SendRegistrationRequest(self,column)
        # p1=PowerSlot()
        # print(p1)
        # x = []
        # for i in range(time_interval):
        #     p1.set_kwh(10)
        #     x.append(p1.copy())
        # msg = str([a.json() for a in x[-time_interval:]])

        # print("Send Registration Request")
        # print(msg)
        # return msg

    def ReceiveRegistrationOutcome(self, msg, topic):
        ElectricProsumer.ReceiveRegistrationOutcome(self, msg, topic)
        print(self.reg)
        # print("Receive Registration Outcome")
    
    # """"    Expected Production Consumption Update Protocol 6.4 """
    # def SendUpdateExpectedEnergyProfile(self):
    #     print("Send Update Expected Energy Profile")

    # def ReceiveUpdateProfileOutcome(self):
    #     print("Receive Update Profile Outcome")
    """"    Expected Production Consumption Update Protocol 6.4 """
    def SendUpdateExpectedEnergyProfile(self,day,column):
        print("Send Update Expected Energy Profile")
        return ElectricProsumer.SendUpdateExpectedEnergyProfile(self,day,column)

    def ReceiveUpdateProfileOutcome(self, msg, topic):
        print("Receive Update Profile Outcome")
        ElectricProsumer.ReceiveUpdateProfileOutcome(self, msg, topic)
        print(self.reg)

    """"    Update Energy Profile Confidence Protocol 6.11"""
    def SendUpdateEnergyProfileConfidence(self,day,column):
        print("Send Update Energy Profile Confidence")
        return ElectricProsumer.SendUpdateEnergyProfileConfidence(self,day,column)
    def ReceiveUpdateConfidenceOutcome(self, msg, topic):
        print("Receive Update Confidence Outcome")
        ElectricProsumer.ReceiveUpdateConfidenceOutcome(self, msg, topic)
        print(self.reg)


    
class NegotiationObject(BaseModel):
    price: float = 0.99
    arrival: datetime = datetime.now() 
    departure: datetime = datetime.now()
    soc: float = 0.5

class Reservation(BaseModel):
    id: str
    arrival: datetime
    departure: datetime

class ChargingType(BaseModel):
    ratedPower: float = 10
    connectorType: str = "Type2"
    category: str = "normal"

class EVPreferences(BaseModel):
    arrivalConfidence: Confidence = Confidence()
    socConfidence: Confidence = Confidence()
    departureConfidence: Confidence = Confidence()
    desiredSOC: float = 0.95
    stationNetwork: str = "net1"
    arrival: datetime = datetime.now()
    departure: datetime = datetime.now()
    chargingType: ChargingType = ChargingType()
    totalKWHs: float = 14.3
    selling_price = 0.2


class Battery(BaseModel):
    capacity: float = 33.0
    soh: float = 0.95
    soc: float = 0.50
    chargingEfficiency: float = 0.95
    consumption: float = 18.03
    chargingTypes: List[ChargingType] = []

    def __init__(self,**data) -> None:
        super().__init__(**data)
        # self._status_array = np.zeros(240)
        try:
            df = pd.read_csv('my_common\ChargingTypeList.csv',index_col=False)
        except:
            df = pd.read_csv('ChargingTypeList.csv',index_col=False)
        
        for i, row in df.iterrows():
            type = ChargingType(connectorType=df["Charger_Type"][i],ratedPower=df["Charger_Power(kW)"][i],category=df["category"][i])
            self.chargingTypes.append(type)


    def discharge(self,x):
        self.soc -= x

    def charge(self,x):
        self.soc += x
    
    def drive(self,km):
        self.soc -= ((km/100)*self.consumption)/self.capacity

class Location(BaseModel):
    """"signed Decimal degrees latitude +-90 longitude +-180"""
    latitude:confloat(gt=-90, lt=90) = 10
    longitude:confloat(gt=-180, lt=180) = 10      

    def distance(self,loc2):
        coords_1 = (self.latitude, self.longitude)
        coords_2 = (loc2.latitude, loc2.longitude)
        # coords_2 = ( 35.51958017109308 , 24.01675932211589)

        print(distance.distance(coords_1, coords_2).km)
        return distance.distance(coords_1, coords_2).km
class ChargingRecommendation(BaseModel):
    id: str = "rec1000"
    arrival: datetime = datetime.now()
    departure: datetime = datetime.now()
    stationNetwork: str = "net1"
    connectorType: str = "Type2"
    stationID: str = "CS1000"
    issueDate: datetime = datetime.now()
    slotID: int = 0
    pricePerKWH: float = 0.2
    totalKWHs: float = 20
    chargingPower: float = 10###
    rank: float = 4.5
    location: Location = Location()
    evid : str = "EV9999"

    

    """"    Charging Recommendation Protocol 6.1 """
    def ReceiveRecommendationRequest(self):
        print("Receive Recommendation Request")
    def CalculateChargingRecommendations(self):
        print("Calculate Charging Recommendations")
    def SendChargingRecommendations(self):
        print("Send Charging Recommendations")

    """"    Charging Station Registration Protocol 6.5 cp4"""
    def ReceiveChargingStationRequest(self, msg, topic):
        print("Receive Charging Station Request")
        return ServiceProvider.ReceiveChargingStationRequest(self, msg, topic)
    def HandleChargingStationRegistration(self):
        print("Handle Charging Station Registration")
    def SendStationRegistrationOutcome(self, id, outcome):
        print("Send Station Registration Outcome")
        print(out_topic_3.replace('+', id),outcome)
        return (out_topic_3.replace('+', id),outcome)

    """"    Authenticate Recommendation Protocol 6.6"""
    def ReceiveRecommendationAuthenticationQuery(self):
        print("Receive Recommendation Authentication Query")
    def SendAuthenticationOutcome(self):
        print("Send Authentication Outcome")

    """"    Electricity Imbalance Request Protocol 6.8"""
    def SendElectricityImbalanceRequest(self):
        print("Send Electricity Imbalance Request")
    def RecieveElectricityImbalance(self):
        print("Recieve Electricity Imbalance")

    """"    Update Station Availability Protocol 6.12"""
    def ReceiveRequestUpdateAvailability(self):
        print("Receive Request Update Availability")
    def HandleStationAvailabilityUpdate(self):
        print("Handle Station Availability Update")
    def SendUpdateAvailabilityOutcome(self):
        print("Send Update Availability Outcome")

ev_counter = 10000
class ElectricVehicle(BaseModel):
    id: str ="EV"
    ev_type:int = 5
    # id_number : int = 1000
    model: str = "model1"
    preferences: EVPreferences = EVPreferences()
    battery: Battery = Battery()
    location: Location = Location()

    recommendations = []
    reservations = NegotiationObject()
    
    _status_array = PrivateAttr()
    _driverBehavior = PrivateAttr()
    _conf=PrivateAttr(0)

    def __init__(self,ev_type,**data) -> None:
        super().__init__(**data)
        global ev_counter
        # self._status_array = np.zeros(240)
        try:
            df = pd.read_csv('my_common\GeneratedData_DriverBehavior_arr_dep_soc.csv',index_col=False)
        except:
            df = pd.read_csv('GeneratedData_DriverBehavior_arr_dep_soc.csv',index_col=False)
        self.ev_type = ev_type
        df2 = df.loc[df['DriverID']=="DR"+str(ev_type)]
        print(df2)
        tmp = df2.loc[:, ['Arrival','Departure','Soc','Day_of_Week','Time_of_Day']]
        self._conf = tmp.groupby(['Day_of_Week','Time_of_Day']).agg(['mean', 'std'])
        self._conf.columns = ['_'.join(c) for c in self._conf.columns.values]
        print("ev_type",ev_type)
        # print( tmp)
        # print( self._conf)

        self._driverBehavior = df2[start_day*time_interval:end_day*time_interval]
        self._status_array = self._driverBehavior["Status"].to_numpy()
        ev_counter+=1
        self.id = "EV"+str(ev_counter)
        print("self.id",self.id)
        self.model = df2["EV_Model"].iloc[0]

        self.location = Location(latitude=ev_counter%10,longitude= 0)
        # print("self._status_array before",self._status_array[:1000])
        for i in range(duration*time_interval+offset):
            # if self.get_status(i)==charging_id and self.get_status(i-1)!=charging_id:#auto douleua
            if self.get_status(i)==charging_id and self.get_status(i+1)!=charging_id: #an einai to teleftaio slot fortisis prosthese linked_hours
                for k in range(1,linked_hours+1):
                    # self.set_linked_hour(i-k,-1)
                    # self.set_linked_hour(i+k,1) #auto douleua
                    self.set_simple_linked_hour(i+k)

        print("self._status_array after",self._status_array[:100])
        # username = input("add hours ")

    def set_selling_price(self,schedule,buy_prices,sell_prices):

        print()
        total_cost=0
        total_profit=0
        total_kwh_charged = 0
        for sched,buy,sell in zip(schedule,buy_prices,sell_prices):
            if sched.kwh>0:
                total_cost+=sched.kwh*buy.price
                total_kwh_charged+=sched.kwh
            else:
                total_profit+=-sched.kwh*sell.price
        if total_kwh_charged>0:
            new_avg_kwh_cost = total_cost/total_kwh_charged
            self.preferences.selling_price=0.5*new_avg_kwh_cost+0.5*self.preferences.selling_price
        print("self.preferences.selling_price",self.preferences.selling_price)
        print("total_cost",total_cost)
        print("total_profit",total_profit)
        print()


    def set_linked_hour(self,hour,c):
        if hour>=0 and hour<duration*time_interval+offset and self.get_status(hour) == idle_id:
            self._status_array[hour] = linked_id
            return 1
        elif hour<0:
            self.set_linked_hour(hour+1,+1)
            # return 0
        elif hour>=duration*time_interval+offset:
            self.set_linked_hour(hour-1,-1)
            # return 0
        elif self.get_status(hour) != idle_id:
            self.set_linked_hour(hour+c,c)
            # self.set_linked_hour(hour+1,c)
            

    def set_simple_linked_hour(self,hour):
        
        if not self.get_status(hour) == driving_id and not self.get_status(hour) == charging_id and hour<duration*time_interval:
            self._status_array[hour] = linked_id

        
    def get_status(self,hour):
        if hour>=0 and hour<duration*time_interval:
            return self._status_array[hour] 
        return -1

    def find_linked_session(self,start,end):
        self.preferences.totalKWHs=0
        arrival = []
        departure = []
        for i in range(start,end):

            if self.get_status(i)==charging_id :
                self.preferences.totalKWHs += self._driverBehavior["Consumption_Charging(kW)"].iloc[i]

            if self.get_status(i-1)!=linked_id and self.get_status(i-1)!=charging_id and self.get_status(i-1)!=arrival_id and (self.get_status(i)==charging_id or self.get_status(i)==linked_id):   
                # _status_array[i] = arrival_id
                my_str = str(self._driverBehavior["Date"].iloc[i])+" "+str(self._driverBehavior["Time_of_Day"].iloc[i]-1)+":00"
                # self.preferences.arrival = datetime.strptime(my_str, '%Y-%m-%d %H:%M')
                arr_date = datetime.strptime(my_str, '%d-%m-%y %H:%M')
                self.preferences.arrival = arr_date
                day = arr_date.weekday()+1
                hour = arr_date.hour+1
                self.preferences.arrivalConfidence = Confidence(mean = self._conf.loc[day,hour]['Arrival_mean'],
                                                                std = self._conf.loc[day,hour]['Arrival_std'])
                self.preferences.socConfidence = Confidence(mean = self._conf.loc[day,hour]['Soc_mean'],
                                                                std = self._conf.loc[day,hour]['Soc_std'])
                arrival.append(1)
            else:
                arrival.append(0)

            if self.get_status(i)==charging_id and self.get_status(i-1)!=charging_id :    
                self.preferences.chargingType = ChargingType(ratedPower=self._driverBehavior["Charger_Power(kW)"].iloc[i], 
                                                            connectorType=self._driverBehavior["Charger_Type"].iloc[i])
                self.battery.soc = self._driverBehavior["Starting_SoC"].iloc[i]
                self.battery.consumption = self._driverBehavior["Mean_Energy_Consumption(kWh/100km)"].iloc[i]
                self.battery.capacity = self._driverBehavior["Battery_Capacity(kWh)"].iloc[i]

            if self.get_status(i)==charging_id and self.get_status(i+1)!=charging_id :
                self.preferences.desiredSOC = self._driverBehavior["Ending_SoC"].iloc[i]

            if self.get_status(i+1)!=linked_id and self.get_status(i+1)!=charging_id  and (self.get_status(i)==charging_id or self.get_status(i)==linked_id or self.get_status(i)==arrival_id):
                # _status_array.iloc[i] = departure_id
                my_str = str(self._driverBehavior["Date"].iloc[i])+" "+str(self._driverBehavior["Time_of_Day"].iloc[i]-1)+":00"
                # self.preferences.departure = datetime.strptime(my_str, '%Y-%m-%d %H:%M')+timedelta(seconds= 3600)#timedelta(seconds= self._driverBehavior["Charging_Time(mins)"].iloc[i]*60)
                dep_date = datetime.strptime(my_str, '%d-%m-%y %H:%M')+timedelta(seconds= 3600)
                self.preferences.departure = dep_date
                day = dep_date.weekday()+1
                hour = dep_date.hour+1

                self.preferences.departureConfidence = Confidence(mean = self._conf.loc[day,hour]['Departure_mean'],
                                                                    std = self._conf.loc[day,hour]['Departure_std'])
                print("i",i)
                # print(self)
                departure.append(1)
                return i+1
                # return (i+1,arrival,departure)
            departure.append(0)
        print("DEN VRETHIKE CHARGING")
        # print(self)
        return end
    def drive(self,km):
        self.battery.drive(km)
        self.location = Location()

    

    """"    Charging Recommendation Protocol 6.1 """
    def SendChargingRecommendationsRequest(self):
        print("Send Charging Recommendations Request")
        # return  self.json(exclude={'recommendations', 'reservations'})
        return str((self.preferences.json(),self.location.json()))

    def ReceiveRecommendations(self, msg, topic):
        print("Receive Recommendations")
        x = ast.literal_eval(msg)
        self.recommendations = [ChargingRecommendation.parse_raw(y) for y in x]
        print(self.id,self.recommendations)
        # self.recommendations = ast.literal_eval(msg)
        # self.recommendations = ChargingRecommendation.parse_raw(msg)

    """"    Charging Station Reservation Protocol 6.2"""
    def SendStationReservationRequest(self):
        print("Send Station Reservation Request")
        rec_len = len(self.recommendations)
        if rec_len>0:
            choice = random.randint(0,rec_len-1)
            # choice = 0
            msg1 = self.recommendations[choice].json()
            stationID = self.recommendations[choice].stationID

            # msg2 = self.json(exclude={'recommendations', 'reservations'})
            msg2 = self.battery.json()
            msg3 = self.preferences.json()
            
            msg = str([msg1,msg2,msg3])
            topic = in_topic_14.replace('+',stationID)
            return (topic,msg)
        else:
            print("ERROR!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!")
            print("no recomendastion send")
            topic = in_topic_14.replace('+',"CS3000000")
            return (topic,"ERROR!!!!!!!!!!! no recommendation sent to "+self.id)
    def ReceiveReservationOutcome(self, msg, topic):
        print("Receive Reservation Outcome")
        # x = ast.literal_eval(msg)
        # self.recommendations = [ChargingRecommendation.parse_raw(y) for y in x]
        x ,sched,buy,sell, valid = ast.literal_eval(msg)
        
        if valid:
            self.reservations = ChargingRecommendation.parse_raw(x)
            self.recommendations = []
            print(type(sched))
            schedule = [PowerSlot.parse_raw(y) for y in ast.literal_eval(sched)]
            buy_prices = [TimePrice.parse_raw(y) for y in ast.literal_eval(buy)]
            sell_prices = [TimePrice.parse_raw(y) for y in ast.literal_eval(sell)]
            print("sched,buy,sell",sched,buy,sell)
            self.set_selling_price(schedule,buy_prices,sell_prices)
        # TODO else 
        # afaireis apo tin lista to invalid kai kaneis reuquest gia allo reservation apo ta recommended 

    """"    Negotiation Protocol 6.3 """
    
    def ReceiveNegotiationMessage(self, msg, topic):
        print("Receive Negotiation Message")
        x = NegotiationObject.parse_raw(msg)
        csid = find_ID2(topic,in_topic_17,"CS")
        print("lala",x,csid)
        return self.NegotiationDecisionMaking(x,csid)

    def NegotiationDecisionMaking(self,x,id):
        print("Negotiation Decision Making")
        # accept="YES!!!!"
        # accept = "no"
        accept = random.choices(["yes", "no"], weights=(25, 75), k=1)[0]
        print(accept)
        return self.SendDecisionMakingOutcome(accept,id)

    def SendDecisionMakingOutcome(self,accept,id):
        print("Send Decision Making Outcome")
        print("To decision einai",accept)
        msg=accept
        #thelei na vazei sto + gia to cs kai sto deutero gia to EV
        
        topic = out_topic_17.replace('CS/+','CS/'+id).replace('EV/+','EV/'+self.id)
        # topic = topic.replace('EV/+','EV/'+self.id)
        return (topic,msg)

    #EV_starts
    def SendNegotiationMessage(self):
        print("Send Negotiation Message")  
        msg = NegotiationObject().json()
        id=self.reservations.stationID
        topic = out_topic_16.replace('CS/+','CS/'+id).replace('EV/+','EV/'+self.id)
        return (topic,msg)
    def ReceiveDecisionMakingOutcome(self, msg, topic):
        print("Receive Decision Making Outcome")
        csid = find_ID2(topic,in_topic_16,"CS")
        if msg == "no":
            print("eisai pipas ?")
            return self.SendNegotiationMessage()
        else:
            return
        

        



class ChargingSlot(ChargingType,BaseModel):
    slotID: int
    netProfitPerKwh: float
    reservations: List[Reservation] = []
cs_counter=10000
class ChargingStation(BaseModel):
    id: str = "CS"
    stationNetwork: str = "net1"
    location: Location = Location()
    chargingSlots: List[ChargingSlot] = []

    reg = {}
    # _prices = PrivateAttr([])
    
    _reservations = PrivateAttr([])
    reservation_id = 100000
    schedules = {}
    _station_schedule = PrivateAttr([])
    _EV_balance = PrivateAttr({})
    linked_evs = []
    _buy_prices =  PrivateAttr([])
    _sell_prices =  PrivateAttr([])

    imbalance_plus = []
    imbalance_minus =[]

    def __init__(self,**data) -> None:
        super().__init__(**data)
        global cs_counter
        # super().__init__(**data)
        cs_counter+=1
        self.id = "CS"+str(cs_counter)
        print("self.id",self.id)
        self.location = Location(latitude=cs_counter%10,longitude= 0)
        # p1 = TimePrice(dateTime=datetime(2020,1,1,0,0)-time_step)
        
        # try:
        #     df = pd.read_csv('my_common\_prices.csv',index_col=False)
        # except:
        #     df = pd.read_csv('_prices.csv',index_col=False)
        
        # for i, row in df.iterrows():
        #     # type = ChargingType(connectorType=df["Charger_Type"][i],ratedPower=df["Charger_Power(kW)"][i],category=df["category"][i])
        #     # self.chargingTypes.append(type)
        #     # my_str = str(self.df["Date"][i])+" "+str(self.df["Time_of_Day"][i]-1)+":00"
        #     # self.preferences.arrival = datetime.strptime(my_str, '%Y-%m-%d %H:%M')
        #     p1.set_price(df["Price"][i])
        #     self._prices.append(p1.copy())
        

    def add_charging_slots(self,n,ratedPower, connectorType, category):
        for i in range(n):
            slot_id=len(self.chargingSlots)
            self.chargingSlots.append(ChargingSlot(slotID=slot_id,netProfitPerKwh=0.02,
                ratedPower=ratedPower, connectorType=connectorType, category=category))

    def payment_calculation(self):
        #kathe ora tsekarei ta cs
        #gia kathe id(car) sto schedule
        #an to sim_now einai == me to PowerSlot.dateTime tote 
        #   an to PowerSlot.kwh>0 tote balance-=PowerSlot.kwh*self._buy_price
        #   an to PowerSlot.kwh<0 tote balance+=PowerSlot.kwh*self._sell_price
        print("payment_calculation payment_calculation payment_calculation")
        for key,value in self.schedules.items():    
            print("key",key)
            
            if not key in self._EV_balance:
                self._EV_balance[key] = 0

            # the_slot = [slot for slot in value if sim_now==slot.dateTime]
            # the_i = [i for i,_ in enumerate(self._buy_price) if sim_now==self._buy_price[i].dateTime]
            for slot in value:
                if sim_now==slot.dateTime:
                    for i,_ in enumerate(self._buy_prices):
                        if sim_now==self._buy_prices[i].dateTime:


                            if slot.kwh>0:
                                print(slot.kwh)
                                self._EV_balance[key]-=slot.kwh*self._buy_prices[i].price#
                            else:
                                self._EV_balance[key]+=slot.kwh*self._sell_prices[i].price
            print("balance",self._EV_balance[key])


    def scheduling(self,recommentation_id):
        charging_time = 0
        reserved_time = 0
        energy_need = 0
        for index,res in enumerate(self._reservations):
            if res[0].id == recommentation_id :
            
                # sim_now = datetime.now()
                # time_step = timedelta(hours=0.25)
                arrival = res[0].arrival
                departure = res[0].departure

                # kwh_need = (res[1].preferences.desiredSOC-res[1].battery.soc)*res[1].battery.capacity
                kwh_need = res[2].totalKWHs
                kw_max_power_per_timestep = res[0].chargingPower*time_step/timedelta(hours=1)

                prices_in_schedule =[]
                for x in self._buy_prices:
                    if arrival <= x.dateTime < departure :
                        prices_in_schedule.append(x)
                
                msg_buy_price = str([a.json() for a in prices_in_schedule])

                sell_prices_in_schedule =[]
                for x in self._sell_prices:
                    if arrival <= x.dateTime < departure :
                        sell_prices_in_schedule.append(x)
                
                msg_sell_price = str([a.json() for a in sell_prices_in_schedule])
            
                schedule = []
            
                t = arrival
                while t < departure :
                    energy_per_timestep = kw_max_power_per_timestep
                    # if t<arrival+time_step:
                    #     energy_per_timestep=0
                    if  kwh_need < kw_max_power_per_timestep:
                        energy_per_timestep = kwh_need
                        kwh_need = 0
                    else:
                        kwh_need -= energy_per_timestep    

                    p1=PowerSlot(kwh=energy_per_timestep,dateTime=t)
                    schedule.append(p1.copy())
                    
                    t += time_step
                
                # self.schedules.append({res[1].id:schedule})
                self.schedules[res[0].evid]= schedule
                print("self.schedules",self.schedules)
                msg_sched = str([a.json() for a in schedule])
                print()
                print(msg_sched)
                return (msg_sched , msg_buy_price, msg_sell_price)
    def scheduling_lowest_prices(self,recommentation_id):
        charging_time = 0
        reserved_time = 0
        energy_need = 0
        for index,res in enumerate(self._reservations):
            if res[0].id == recommentation_id :
            
                # sim_now = datetime.now()
                # time_step = timedelta(hours=0.25)
                arrival = res[0].arrival
                departure = res[0].departure

                # kwh_need = (res[1].preferences.desiredSOC-res[1].battery.soc)*res[1].battery.capacity
                
                kw_max_power_per_timestep = res[0].chargingPower*time_step/timedelta(hours=1)
                
                kwh_need = res[2].totalKWHs
                

                prices_in_schedule =[]
                for x in self._buy_prices:
                    if arrival <= x.dateTime < departure :
                        prices_in_schedule.append(x)
                
                msg_buy_price = str([a.json() for a in prices_in_schedule])

                sell_prices_in_schedule =[]
                for x in self._sell_prices:
                    if arrival <= x.dateTime < departure :
                        sell_prices_in_schedule.append(x)
                
                msg_sell_price = str([a.json() for a in sell_prices_in_schedule])

                print("prices_in_schedule",prices_in_schedule)
                prices_in_schedule.sort(key=lambda y: y.price)
                print("SORTED prices_in_schedule",prices_in_schedule)
            
                schedule = []

                # t = arrival
                # while t < departure :
                for price_time in prices_in_schedule:
                    energy_per_timestep = kw_max_power_per_timestep
                    # if t<arrival+time_step:
                    #     energy_per_timestep=0
                    if  kwh_need < kw_max_power_per_timestep:
                        energy_per_timestep = kwh_need
                        kwh_need = 0
                    else:
                        kwh_need -= energy_per_timestep    

                    p1=PowerSlot(kwh=energy_per_timestep,dateTime=price_time.dateTime)
                    schedule.append(p1.copy())
                    
                    # t += time_step
                schedule.sort(key=lambda y: y.dateTime)
                # self.schedules.append({res[1].id:schedule})
                self.schedules[res[0].evid]= schedule
                print("self.schedules",self.schedules)
                msg_sched = str([a.json() for a in schedule])
                print()
                print(msg_sched)
                return (msg_sched , msg_buy_price, msg_sell_price)
        """ for index,res in enumerate(self._reservations):
            
            # sim_now = datetime.now()
            # time_step = timedelta(hours=0.25)
            arrival = res[0].arrival
            departure = res[0].departure

            kwh_need = (res[1].preferences.desiredSOC-res[1].battery.soc)*res[1].battery.capacity
            kw_max_power_per_timestep = res[0].chargingPower*time_step/timedelta(hours=1)

           
            schedule = []
          
            t = arrival
            while t < departure :
                energy_per_timestep = kw_max_power_per_timestep
                # if t<arrival+time_step:
                #     energy_per_timestep=0
                if  kwh_need < kw_max_power_per_timestep:
                    energy_per_timestep = kwh_need
                    kwh_need = 0
                else:
                    kwh_need -= energy_per_timestep    

                p1=PowerSlot(kwh=energy_per_timestep,dateTime=t)
                schedule.append(p1.copy())
                
                t += time_step
            
            # self.schedules.append({res[1].id:schedule})
            self.schedules[res[1].id]= schedule
            print("self.schedules",self.schedules)
            msg = str([a.json() for a in schedule])
            print()
            print(msg)
 """

    def scheduling_V2G(self,recommentation_id):
        charging_time = 0
        reserved_time = 0
        energy_need = 0
        for index,res in enumerate(self._reservations):
            if res[0].id == recommentation_id :
            
                # sim_now = datetime.now()
                # time_step = timedelta(hours=0.25)
                arrival = res[0].arrival
                departure = res[0].departure

                # kwh_need = (res[1].preferences.desiredSOC-res[1].battery.soc)*res[1].battery.capacity
                kwh_need = res[1].preferences.totalKWHs
                kw_max_power_per_timestep = res[0].chargingPower*time_step/timedelta(hours=1)#11# 
                kw_max_power_V2G_per_timestep =10
                EV_selling_price=10
                # EV_selling_price=avg_price_calculation()
                lowest_soc =0.1
                avg_buy_price = res[1].preferences.selling_price

                prices_in_schedule =[]
                for x in self._buy_prices:
                    if arrival <= x.dateTime < departure :
                        prices_in_schedule.append(x)
                
                msg_buy_price = str([a.json() for a in prices_in_schedule])

                sell_prices_in_schedule =[]
                # for x in self._sell_prices: GIA NA DO AN DOULEUEI TO V2G !!!!!!!!
                for x in self._buy_prices:
                    if arrival <= x.dateTime < departure :
                        sell_prices_in_schedule.append(x)
                
                msg_sell_price = str([a.json() for a in sell_prices_in_schedule])
                
                print("prices_in_schedule",prices_in_schedule)
                prices_in_schedule.sort(key=lambda y: y.price,reverse=True)
                print("SORTED prices_in_schedule",prices_in_schedule)
                print("LENGHT prices_in_schedule",len(prices_in_schedule))
                remaining_timeslots = len(prices_in_schedule)-1


                schedule = []


                v2g=[]
                for i,price_time in enumerate(prices_in_schedule):
                    energy_per_timestep = kw_max_power_per_timestep
                    energy_V2G_per_timestep = kw_max_power_V2G_per_timestep
                    # if t<arrival+time_step:
                    #     energy_per_timestep=0
                    if remaining_timeslots*kw_max_power_per_timestep<kwh_need:
                        print("LALALLALLALLALLALALLALALALLALAL",price_time.dateTime)
                        break
                    if price_time.price >avg_buy_price :
                        if res[1].battery.soc - energy_V2G_per_timestep/res[1].battery.capacity >lowest_soc:
                            # kwh_need += energy_V2G_per_timestep
                            energy_per_timestep =-energy_V2G_per_timestep
                            kwh_need+=energy_V2G_per_timestep
                            print(kwh_need,price_time.dateTime)
                            v2g.append(i)
                            p1=PowerSlot(kwh=energy_per_timestep,dateTime=price_time.dateTime)
                            schedule.append(p1.copy())
                            
 
                    remaining_timeslots-=1
                    
                prices_in_schedule=[a for i,a in enumerate(prices_in_schedule) if not i in v2g]
                prices_in_schedule.sort(key=lambda y: y.price,reverse=False)
                for price_time in prices_in_schedule:
                    energy_per_timestep = kw_max_power_per_timestep
                    energy_V2G_per_timestep = kw_max_power_V2G_per_timestep

                    if  kwh_need < kw_max_power_per_timestep:
                        energy_per_timestep = kwh_need
                        kwh_need = 0
                    else:
                        kwh_need -= energy_per_timestep 

                    print(kwh_need,price_time.dateTime)
                    p1=PowerSlot(kwh=energy_per_timestep,dateTime=price_time.dateTime)
                    schedule.append(p1.copy())
                    
                    # t += my_util_pydantic.time_step
                schedule.sort(key=lambda y: y.dateTime)
                
                """ self.schedules[res[1].id]= schedule """
                """ print("self.schedules",self.schedules) """
                print("schedule",schedule)
                msg_sched = str([a.json() for a in schedule])
                print("kwh_need",kwh_need)
                print()
                print(msg_sched)
                return (msg_sched , msg_buy_price, msg_sell_price)


    def scheduling_V2G_time(self,recommentation_id):
        charging_time = 0
        reserved_time = 0
        energy_need = 0
        for index,res in enumerate(self._reservations):
            if res[0].id == recommentation_id :
            
                # sim_now = datetime.now()
                # time_step = timedelta(hours=0.25)
                arrival = res[0].arrival
                departure = res[0].departure
                max_capacity = res[1].battery.capacity

                # kwh_need = (res[1].preferences.desiredSOC-res[1].battery.soc)*res[1].battery.capacity
                need_kwh = [res[1].preferences.totalKWHs]
                kw_max_power_per_timestep = res[0].chargingPower*time_step/timedelta(hours=1)#11# 
                kw_max_power_V2G_per_timestep =10
                EV_selling_price=10
                # EV_selling_price=avg_price_calculation()
                lowest_soc =0.1
                avg_buy_price = res[1].preferences.selling_price

                prices_in_schedule =[]
                for x in self._buy_prices:
                    if arrival <= x.dateTime < departure :
                        prices_in_schedule.append(x)
                
                msg_buy_price = str([a.json() for a in prices_in_schedule])

                sell_prices_in_schedule =[]
                # for x in self._sell_prices: GIA NA DO AN DOULEUEI TO V2G !!!!!!!!
                for x in self._buy_prices:
                    if arrival <= x.dateTime < departure :
                        sell_prices_in_schedule.append(x)
                
                msg_sell_price = str([a.json() for a in sell_prices_in_schedule])
                

                k=0
                while not need_kwh[-1]==0:
                    cost = [0] 
                    need_kwh=[res[1].preferences.totalKWHs]
                    # median_price = statistics.median(price_init)
                    
                    tmp_price_init_buy=prices_in_schedule.copy()
                    tmp_price_init_buy.extend([TimePrice(price=10000000000) for i in range(k)])
                    k+=1
                    avg_buy_price=0.0
                    median_price_buy = max(avg_buy_price, statistics.median([x.price for x in tmp_price_init_buy]))
                    avg_price_buy = sum([x.price for x in prices_in_schedule]) / len(prices_in_schedule)
                    peak=[]
                    v2g = []
                    g2v = []

                    total_non_peak_slots = 0
                    for pr in prices_in_schedule:
                        if pr.price<median_price_buy:
                            total_non_peak_slots+=1
                    remaining_not_peak_slots=total_non_peak_slots

                    schedule = []

                    for i,(pr_buy,pr_sell) in enumerate(zip(prices_in_schedule,sell_prices_in_schedule)):
                        if pr_sell.price>median_price_buy:
                            print("PEAK")
                            peak.append(1)
                            #discharge
                            
                            
                            if need_kwh[-1]+kw_max_power_V2G_per_timestep >= max_capacity:
                                p1=PowerSlot(kwh=-(max_capacity-need_kwh[-1]),dateTime=pr_sell.dateTime)
                                schedule.append(p1.copy())

                                cost.append(cost[-1]-(max_capacity-need_kwh[-1])*pr_sell.price)
                                # cost.append(cost[-1])
                            
                                need_kwh.append(max_capacity)
                                # need_kwh.append(need_kwh[-1])
                                # v2g.append(i,pr,max_capacity)
                                
                                
                            elif need_kwh[-1] +kw_max_power_V2G_per_timestep > remaining_not_peak_slots*kw_max_power_V2G_per_timestep:
                                print("BUT NO V2G")
                                p1=PowerSlot(kwh=0,dateTime=pr_sell.dateTime)
                                schedule.append(p1.copy())

                                need_kwh.append(need_kwh[-1])#idle
                                cost.append(cost[-1])
                            else:
                                # username = input("discharge !! ")
                                p1=PowerSlot(kwh=-kw_max_power_V2G_per_timestep,dateTime=pr_sell.dateTime)
                                schedule.append(p1.copy())
                                need_kwh.append(need_kwh[-1]+kw_max_power_V2G_per_timestep)
                                cost.append(cost[-1]-kw_max_power_V2G_per_timestep*pr_sell.price)
                            v2g.append([i,pr_sell.price,need_kwh[-1]])
                        else:# OOORRR if pr_buy<=median_price_buy:
                            print("NOT PEAK")
                            peak.append(0)
                            #charge
                            remaining_not_peak_slots-=1

                            if need_kwh[-1]-kw_max_power_per_timestep<0:#overcharge
                                p1=PowerSlot(kwh=need_kwh[-1],dateTime=pr_buy.dateTime)
                                schedule.append(p1.copy())

                                cost.append(cost[-1]+need_kwh[-1]*pr_buy.price)
                                need_kwh.append(0)
                                
                            else:
                                p1=PowerSlot(kwh=kw_max_power_per_timestep,dateTime=pr_buy.dateTime)
                                schedule.append(p1.copy())

                                need_kwh.append(need_kwh[-1]-kw_max_power_per_timestep)
                                cost.append(cost[-1]+kw_max_power_per_timestep*pr_buy.price)
                            g2v.append([i,pr_buy,need_kwh[-1]])

                print("need_kwh",need_kwh)
                print("g2v",g2v)
                print("v2g",v2g)
                if not need_kwh[-1]==0:
                    print("DEN FORTISE FULL",i)
                    # username = input("Enter ")
                    
                else:
                    print("FORTISE FULL")

                # return(need_kwh,cost)
                print("schedule",schedule)
                msg_sched = str([a.json() for a in schedule])
                print("need_kwh",need_kwh)
                print()
                print(msg_sched)
                return (msg_sched , msg_buy_price, msg_sell_price)



    def scheduling_V2G_optimal(self,recommentation_id):
        global global_cost,tests,cum_global_cost
        charging_time = 0
        reserved_time = 0
        energy_need = 0
        for index,res in enumerate(self._reservations):
            if res[0].id == recommentation_id :
            
               
                
                # Initalize PuLP object
               
                optimization_model = pulp.LpProblem('MinimizeCost', pulp.LpMinimize)
                # Constants
                
                arrival = res[0].arrival
                departure = res[0].departure
                max_capacity = res[1].capacity

                lowest_soc =0.1
                avg_buy_price = res[2].selling_price

                prices_in_schedule =[]
                for x in self._buy_prices:
                    if arrival <= x.dateTime < departure :
                        prices_in_schedule.append(x)
                
                msg_buy_price = str([a.json() for a in prices_in_schedule])

                sell_prices_in_schedule =[]
                # for x in self._sell_prices: GIA NA DO AN DOULEUEI TO V2G !!!!!!!!
                for x in self._sell_prices:
                    if arrival <= x.dateTime < departure :
                        sell_prices_in_schedule.append(x)
                
                msg_sell_price = str([a.json() for a in sell_prices_in_schedule])
                number_of_slots = int((departure - arrival).seconds/3600 +(departure - arrival).days*24 )
                print("number_of_slots",number_of_slots)
                print("len(prices_in_schedule)",len(prices_in_schedule))
                print("len(sell_prices_in_schedule)",len(sell_prices_in_schedule))

                #elegxos an exo prices gia olo to diastima pou einai sindedemeno to oxima
                
                
                if not number_of_slots == len(prices_in_schedule) == len(sell_prices_in_schedule):
                    print("Loipoun prices gia na ginei to scheduling")
                    print("number_of_slots",number_of_slots)
                    print("len(prices_in_schedule)",len(prices_in_schedule))
                    print("len(sell_prices_in_schedule)",len(sell_prices_in_schedule))
                    # username = input("Loipoun prices gia na ginei to scheduling ")
                    number_of_slots = len(prices_in_schedule)

                

                # need_kwh = [res[1].preferences.totalKWHs]
                # kw_max_power_per_timestep = res[0].chargingPower*time_step/timedelta(hours=1)#11# 
                # kw_max_power_V2G_per_timestep =10
               
                kw_each_hour_g2v = res[0].chargingPower*time_step/timedelta(hours=1)
                kw_each_hour_v2g = res[0].chargingPower*time_step/timedelta(hours=1)#10
                kwh_need = res[2].totalKWHs
                
                max_capacity = res[1].capacity
                min_capacity = 0.1*res[1].capacity

                init_soc = 1-kwh_need/max_capacity
                init_capacity = init_soc*max_capacity
                # Create decision variables
                decision_variables1 = []
                decision_variables2 = []
                for i in range(number_of_slots):
                    variable = 'g2v' + '_' + str(i+100000)
                    variable = pulp.LpVariable(str(variable), lowBound = 0, upBound = 1)
                    decision_variables1.append(variable)
                    variable = 'v2g' + '_' + str(i+100000)
                    variable = pulp.LpVariable(str(variable), lowBound = 0, upBound = 1)
                    decision_variables2.append(variable)

                # Optimization function
                optimization_function = ""
                for i in range(number_of_slots):
                    g2v_cost = prices_in_schedule[i].price * decision_variables1[i]*kw_each_hour_g2v
                    v2g_profit = sell_prices_in_schedule[i].price * decision_variables2[i]*kw_each_hour_v2g
                    degradetion_cost =  0.001* decision_variables2[i]*kw_each_hour_v2g # FTHORA MPATARIAS LOGO V2G
                    extra_cost_g2v = 0#100*( prices_in_schedule[i].price - avg_buy_price)* decision_variables1[i]*kw_each_hour_g2v
                    extra_cost_v2g = 0#100*( avg_buy_price - sell_prices_in_schedule[i].price)*decision_variables2[i]*kw_each_hour_v2g
                    #10000000000000000000000000
                    formula = (g2v_cost + degradetion_cost + extra_cost_g2v+extra_cost_v2g - v2g_profit )
                    # formula = ((price_each_hour[i] * decision_variables[i]) if decision_variables[i]>=0 else (sell_prices_in_schedule[i] * decision_variables[i]))
                    optimization_function += formula
                    
                optimization_model += optimization_function

                # Constraints

                # large_number = 1000000000
                # for i in range(number_of_slots):
                #     formula = ""
                #     formula += decision_variables1[i] +  decision_variables2[i]     
                #     optimization_model += (formula <= 1)

                for i in range(number_of_slots):
                    formula = ""
                    formula += decision_variables1[i] +  decision_variables2[i]     
                    optimization_model += (formula <= 1)

                formula = ""
                for i in range(number_of_slots):
                    formula += decision_variables1[i] * kw_each_hour_g2v-decision_variables2[i] * kw_each_hour_v2g
                        
                optimization_model += (formula == kwh_need)


                soc = ""

                for i in range(number_of_slots):
                    soc += (decision_variables1[i]*kw_each_hour_g2v - decision_variables2[i]*kw_each_hour_v2g)
                    optimization_model += (soc+init_capacity <= max_capacity)
                    optimization_model += (soc+init_capacity >= min_capacity)

                # formula = ""
                # for i in range(number_of_slots):
                #     formula += (price_each_hour[i] * decision_variables[i])
                # optimization_model += (formula <= 100)

                # Solve optimization
                # print("optimization_model",optimization_model)
                optimization_result = optimization_model.solve()

                print("Status:", pulp.LpStatus[optimization_model.status])
                print("Total cost: ", pulp.value(optimization_model.objective))

                # Let's see which decision variables are selected by the optimization
                optimal_schedule = []
                for var in optimization_model.variables():
                    # if (var.varValue == 1):
                        # print(var, end=" ")
                    optimal_schedule.append(var.varValue)
                # print("\n")
                # print("optimal_schedule",optimal_schedule)
                
                # total_cost = 0
                # for i,s in enumerate(optimal_schedule):
                #     if i <10:
                #         total_cost+=s*prices_in_schedule[i]*kw_each_hour_g2v
                #     else:
                #         total_cost-=s*sell_prices_in_schedule[i-10]*kw_each_hour_v2g
                # # total_cost = (pd.Series(price_each_hour)*pd.Series(optimal_schedule)).sum()
                # # print(total_cost)
                # all_cost.append(total_cost)
                # if total_cost<=-0.7951400000000001:
                d_g2v = optimal_schedule[:len(optimal_schedule)//2]
                d_v2g = optimal_schedule[len(optimal_schedule)//2:]

                actual_cost =sum([p.price*d*kw_each_hour_g2v for p,d in  zip(prices_in_schedule,d_g2v)])
                actual_cost -=sum( [p.price*d*kw_each_hour_v2g for p,d in  zip(sell_prices_in_schedule,d_v2g)])
                print("actual_cost",actual_cost,"!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!")
                global_cost+=actual_cost
                cum_global_cost.append(global_cost)
                tests+=1
                print("tests",tests)
                

                print("global_cost",global_cost,"!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!")
                print("prices_in_schedule",prices_in_schedule)
                print("sell_prices_in_schedule",sell_prices_in_schedule)
                print("optimal_schedule",optimal_schedule)
                
                print("optimal_schedule g2v",d_g2v)
                print("optimal_schedule v2g",d_v2g)
                p1 = PowerSlot(dateTime= arrival - time_step)

                schedule = []

                for i in range(number_of_slots):
                    p1.set_kwh(d_g2v[i]*kw_each_hour_g2v - d_v2g[i]*kw_each_hour_v2g)
                    schedule.append(p1.copy())
                    
                print("schedule",schedule)
                msg_sched = str([a.json() for a in schedule])
                
                print()
                print(msg_sched)
                """ if tests ==44 or tests ==5:
                    print("cum_global_cost",cum_global_cost)
                    plt.plot(cum_global_cost, label='cum_global_cost')
                    # plt.plot([x.price for x in CS1._sell_prices], label='sell price')
                    # plt.legend()
                    plt.ylabel('cost')
                    plt.show() """

                return (msg_sched , msg_buy_price, msg_sell_price)

                """  k=0
                while not need_kwh[-1]==0:
                    cost = [0] 
                    need_kwh=[res[1].preferences.totalKWHs]
                    # median_price = statistics.median(price_init)
                    
                    tmp_price_init_buy=prices_in_schedule.copy()
                    tmp_price_init_buy.extend([TimePrice(price=10000000000) for i in range(k)])
                    k+=1
                    avg_buy_price=0.0
                    median_price_buy = max(avg_buy_price, statistics.median([x.price for x in tmp_price_init_buy]))
                    avg_price_buy = sum([x.price for x in prices_in_schedule]) / len(prices_in_schedule)
                    peak=[]
                    v2g = []
                    g2v = []

                    total_non_peak_slots = 0
                    for pr in prices_in_schedule:
                        if pr.price<median_price_buy:
                            total_non_peak_slots+=1
                    remaining_not_peak_slots=total_non_peak_slots

                    schedule = []

                    for i,(pr_buy,pr_sell) in enumerate(zip(prices_in_schedule,sell_prices_in_schedule)):
                        if pr_sell.price>median_price_buy:
                            print("PEAK")
                            peak.append(1)
                            #discharge
                            
                            
                            if need_kwh[-1]+kw_max_power_V2G_per_timestep >= max_capacity:
                                p1=PowerSlot(kwh=-(max_capacity-need_kwh[-1]),dateTime=pr_sell.dateTime)
                                schedule.append(p1.copy())

                                cost.append(cost[-1]-(max_capacity-need_kwh[-1])*pr_sell.price)
                                # cost.append(cost[-1])
                            
                                need_kwh.append(max_capacity)
                                # need_kwh.append(need_kwh[-1])
                                # v2g.append(i,pr,max_capacity)
                                
                                
                            elif need_kwh[-1] +kw_max_power_V2G_per_timestep > remaining_not_peak_slots*kw_max_power_V2G_per_timestep:
                                print("BUT NO V2G")
                                p1=PowerSlot(kwh=0,dateTime=pr_sell.dateTime)
                                schedule.append(p1.copy())

                                need_kwh.append(need_kwh[-1])#idle
                                cost.append(cost[-1])
                            else:
                                # username = input("discharge !! ")
                                p1=PowerSlot(kwh=-kw_max_power_V2G_per_timestep,dateTime=pr_sell.dateTime)
                                schedule.append(p1.copy())
                                need_kwh.append(need_kwh[-1]+kw_max_power_V2G_per_timestep)
                                cost.append(cost[-1]-kw_max_power_V2G_per_timestep*pr_sell.price)
                            v2g.append([i,pr_sell.price,need_kwh[-1]])
                        else:# OOORRR if pr_buy<=median_price_buy:
                            print("NOT PEAK")
                            peak.append(0)
                            #charge
                            remaining_not_peak_slots-=1

                            if need_kwh[-1]-kw_max_power_per_timestep<0:#overcharge
                                p1=PowerSlot(kwh=need_kwh[-1],dateTime=pr_buy.dateTime)
                                schedule.append(p1.copy())

                                cost.append(cost[-1]+need_kwh[-1]*pr_buy.price)
                                need_kwh.append(0)
                                
                            else:
                                p1=PowerSlot(kwh=kw_max_power_per_timestep,dateTime=pr_buy.dateTime)
                                schedule.append(p1.copy())

                                need_kwh.append(need_kwh[-1]-kw_max_power_per_timestep)
                                cost.append(cost[-1]+kw_max_power_per_timestep*pr_buy.price)
                            g2v.append([i,pr_buy,need_kwh[-1]])

                print("need_kwh",need_kwh)
                print("g2v",g2v)
                print("v2g",v2g)
                if not need_kwh[-1]==0:
                    print("DEN FORTISE FULL",i)
                    # username = input("Enter ")
                    
                else:
                    print("FORTISE FULL")

                # return(need_kwh,cost)
                print("schedule",schedule)
                msg_sched = str([a.json() for a in schedule])
                print("need_kwh",need_kwh)
                print()
                print(msg_sched) """
                






    def charging_with_schedule(self,t):
        for id,shedule in self.schedules.items():
            print("ID",id)
            print("Schedule",shedule)
            for x in shedule:
                if t<=x.dateTime<t+time_step:
                    if x.kwh>0:
                        # EV[id].charge(x.kwh)
                        print()
                        
                        
        print()


    def charging(self,t):
        for res in self._reservations:
            if res[0].arrival<= t < res[0].departure:
                print("fortizei")
                self.fcfs(res[1])

            print()

    def fcfs(self,ev):

        print("11111111111",self._reservations)
        for index,res in enumerate(self._reservations):
            if ev in res:
        # EV= self._reservations[1]
                if ev.battery.soc < ev.preferences.desiredSOC:

                    delta_power = self._reservations[index][0].chargingPower *(time_step.seconds/3600)
                    self._reservations[index][1].battery.soc += delta_power/ev.battery.capacity


                    if self._reservations[index][1].battery.soc>ev.preferences.desiredSOC:
                        extra_soc = (self._reservations[index][1].battery.soc-ev.preferences.desiredSOC)
                        print(extra_soc)
                        self._reservations[index][1].battery.soc -= delta_power/ev.battery.capacity
                        
                        extra_time = extra_soc/(delta_power/ev.battery.capacity)
                        print("extra_time",extra_time)
                        desired_time = (1-extra_time)*time_step
                        print(desired_time)
                        delta_power = self._reservations[index][0].chargingPower *(desired_time.seconds/3600)
                        print("delta_power",delta_power)
                        self._reservations[index][1].battery.soc += delta_power/ev.battery.capacity
        
        print("222222222222",self._reservations)
    
    # def updateVehiclesFCFS():

    #     # check each chargePort
    #     for index, vehicle in enumerate( chargePorts.chargePorts ):        

    #         # add 1 minute of charge
    #         if vehicle is not None:
    #             vehicle.currentCharge +=  ( vehicle.chargeRate ) / 60.0
    #             removed = False;

    #             # check if done charging
    #             if vehicle.currentCharge >= vehicle.chargeNeeded:

    #                 # this vehicle is on the out, so wrap up its listener
    #                 chargePorts.chargePortListeners[ index ][ 0 ].terminateCharge( vehicle , common.currentTime )

    #                 # remove finished vehicle from grid and document it
    #                 csvGen.exportVehicleToCSV( vehicle, "SUCCESS" )
    #                 common.doneChargingLot.append( vehicle )
                    
    #                 # the next vehicle
    #                 if not queue.empty():

    #                     nextVehicle = queue.get()
    #                     chargePorts.chargePorts[ index ] = nextVehicle

    #                     # and then make a new listener
    #                     chargePorts.chargePortListeners[ index ].insert( 0 , chargeEvent.ChargeEvent( nextVehicle , common.currentTime ) )

    #                 else:
    #                     chargePorts.chargePorts[ index ] = None
    #                 removed = True;


    #             # check if deadline reached            
    #             if common.currentTime >= vehicle.depTime and not removed:

    #                 # this vehicle is on the out, so wrap up its listener
    #                 chargePorts.chargePortListeners[ index ][ 0 ].terminateCharge( vehicle , common.currentTime )
                    
    #                 # remove finished vehicle from grid and document it
    #                 csvGen.exportVehicleToCSV( vehicle, "FAILURE" )               
    #                 common.failedLot.append( vehicle )
                    
    #                 # the next vehicle
    #                 if not queue.empty():

    #                     nextVehicle = queue.get()
    #                     chargePorts.chargePorts[ index ] = nextVehicle

    #                     # and then make a new listener
    #                     chargePorts.chargePortListeners[ index ].insert( 0 , chargeEvent.ChargeEvent( nextVehicle , common.currentTime ) )

    #                 else:
    #                     chargePorts.chargePorts[ index ] = None
    



        

    """"    Update Energy Profile Confidence Protocol 6.11"""
    def SendUpdateEnergyProfileConfidence(self):
        print("Send Update Energy Profile Confidence")
        return ElectricProsumer.SendUpdateEnergyProfileConfidence(self)
    def ReceiveUpdateConfidenceOutcome(self, msg, topic):
        print("Receive Update Confidence Outcome")
        ElectricProsumer.ReceiveUpdateConfidenceOutcome(self, msg, topic)
        print(self.reg)

    """"    Charging Station Reservation Protocol 6.2"""
    def ReceiveChargingReservationRequest(self, msg, topic):
        print("Receive Charging Reservation Request")
        id = find_ID(in_topic_14,topic)
        # recom , ev = ast.literal_eval(msg)
        recom , bat, pref = ast.literal_eval(msg)
        print()
        print()
        # print(recom)
        # print()
        # print(ev)
        self._reservations.append([ChargingRecommendation.parse_raw(recom),Battery.parse_raw(bat),EVPreferences.parse_raw(pref)])
        # self._reservations.append([ChargingRecommendation.parse_raw(recom),ElectricVehicle.parse_raw(ev)])<--auto itan prin
        # self._reservations.append([ChargingRecommendation.parse_raw(recom),ElectricVehicle.parse_raw(ev)])
        print("self._reservations",self._reservations)
        if id is not None:
            return ChargingStation.SendAuthenticateRecommendationQuery(self,recom)
            # return ChargingStation.HandleChargingReservationRequest(self, msg, id)


    def HandleChargingReservationRequest(self, msg, id):
        print("Handle Charging Reservation Request")
        #CHEKAREI NA DEI AMA TO RECOMMENTTATION EINAI AYTHENTIKO
        x,success_reservation = ast.literal_eval(msg)
        # success_reservation = True
        reservation = ChargingRecommendation.parse_raw(x)
        recommendation = ChargingRecommendation.parse_raw(x)
        schedule=[]
        buy_prices=[]
        sell_prices=[]
        print(self.chargingSlots)
        #make Reservation
        
        if success_reservation:
            
            for i,chargingSlot in enumerate(self.chargingSlots):
                if chargingSlot.slotID == recommendation.slotID:
                    
                    self.chargingSlots[i].reservations.append(Reservation(id="reserv"+str(self.reservation_id),
                            arrival = recommendation.arrival,departure= recommendation.departure))
                    if sched_algo == 1:
                        print("self.scheduling(recommendation.id)")
                        schedule , buy_prices ,sell_prices = self.scheduling(recommendation.id)
                    elif sched_algo == 2:
                        print("self.scheduling_lowest_prices(recommendation.id)")
                        schedule , buy_prices ,sell_prices= self.scheduling_lowest_prices(recommendation.id)
                    # print("self.scheduling_V2G(recommendation.id)")
                    # schedule , buy_prices ,sell_prices= self.scheduling_V2G(recommendation.id)
                    # print("self.scheduling_V2G_time(recommendation.id)")
                    # schedule , buy_prices ,sell_prices= self.scheduling_V2G_time(recommendation.id)
                    elif sched_algo == 3:
                        print("self.scheduling_V2G_optimal(recommendation.id)")
                        schedule , buy_prices ,sell_prices= self.scheduling_V2G_optimal(recommendation.id)

                    
                    
                    self.reservation_id +=1
        else:
             # remove from self._reservations to auto me to recommendation id tou recomentation
            for index,reserv in enumerate(self._reservations):
                if reserv[0].id == recommendation.id:
                    self._reservations.pop(index)


           
                


        print(self.chargingSlots)
        
        # check if reservation can fit in schedule
        
        

        # msg1 = str((reservation,success_reservation))
        return ChargingStation.SendReservationOutcome(self, recommendation.evid,success_reservation,reservation, schedule , buy_prices, sell_prices)

    def SendReservationOutcome(self, id,success_reservation,reservation, schedule , buy_prices, sell_prices):
        print("Send Reservation Outcome")
        msg = str((reservation.json(), schedule , buy_prices, sell_prices,success_reservation))
        
        return (out_topic_14.replace('+', id),msg)
    

    """"    Negotiation Protocol 6.3 """
 
    #EV_starts
    def ReceiveNegotiationMessage(self, msg, topic):
        print("Receive Negotiation Message")
        x = NegotiationObject.parse_raw(msg)
        evid = find_ID2(topic,out_topic_16,"EV")
        print("lala",x,evid)
        return self.NegotiationDecisionMaking(x,evid)

    def NegotiationDecisionMaking(self,x,id):
        print("Negotiation Decision Making")
        # accept="YES!!!!"
        # accept="no"
        accept = random.choices(["yes", "no"], weights=(75, 25), k=1)[0]
        print(accept)
        return self.SendDecisionMakingOutcome(accept,id)

    def SendDecisionMakingOutcome(self,accept,id):
        print("Send Decision Making Outcome")
        print("To decision einai",accept)
        msg=accept
        topic = in_topic_16.replace('CS/+','CS/'+self.id).replace('EV/+','EV/'+id)
        
        return (topic,msg)

    #CS_starts
    def SendNegotiationMessage(self):
        print("Send Negotiation Message")  
        msg = NegotiationObject().json()
        id="EV10001"#self.reservations.stationID
        topic = in_topic_17.replace('CS/+','CS/'+self.id).replace('EV/+','EV/'+id)
        return (topic,msg)
    def ReceiveDecisionMakingOutcome(self, msg, topic):
        print("Receive Decision Making Outcome")
        evid = find_ID2(topic,out_topic_17,"EV")
        if msg == "no":
            print("eisai pipas ?")
            return self.SendNegotiationMessage()
        else:
            return



    """"    Charging Station Registration Protocol 6.5 cp4"""
    def SendChargingStationRegistration(self):
        print("Send Charging Station Registration")
        return ElectricProsumer.SendChargingStationRegistration(self)
    def ReceiveRegistrationOutcome(self, msg, topic):
        print("Receive Registration Outcome")
        ElectricProsumer.ReceiveRegistrationOutcome(self, msg, topic)
        print(self.reg)

    """"    Charging Station Update Schedule Protocol 6.9"""
    def SendUpdatedChargingSchedule(self,msg,topic):
        print("Send Updated Charging Schedule")
        # p1=PowerSlot()
        # print(p1)
        # x = []
        # for i in range(time_interval):
        #     p1.set_kwh(3)
        #     x.append(p1.copy())
        # msg = str([a.json() for a in x[-time_interval:]])
        a = ast.literal_eval(msg)
        schedule=a[1]
        print(in_topic_9.replace('+', self.id),schedule)
        return (in_topic_9.replace('+', self.id),schedule)
    def ReceiveUpdateScheduleOutcome(self, msg, topic):
        print("Receive Update Schedule Outcome")
        a = topic.split("/")
        print(a[0]+" "+self.id+" "+msg)
        self.reg[a[0]] = msg

    """"    Authenticate Recommendation Protocol 6.6"""
    def SendAuthenticateRecommendationQuery(self,msg):
        print("Send Authenticate Recommendation Query")

        # test_rec = ChargingRecommendation(id = "test_rec")
        test_rec = ChargingRecommendation.parse_raw(msg)
        # msg = test_rec.json()
        topic = in_topic_15.replace("+",test_rec.stationID)
        return (topic,msg)

    def ReceiveAuthenticationResponse(self, msg, topic):
        print("Receive Authentication Response")
        # outcome = bool(msg)
        id = find_ID(out_topic_15,topic)
        if id is not None:
            return ChargingStation.HandleChargingReservationRequest(self,msg,id)



    """"    Electricity Imbalance Request Protocol 6.8"""
    def SendElectricityImbalanceRequest(self):
        print("Send Electricity Imbalance Request")
        return ServiceProvider.SendElectricityImbalanceRequest(self)
    def RecieveElectricityImbalance(self, msg, topic):
        print("Recieve Electricity Imbalance")
        ServiceProvider.RecieveElectricityImbalance(self, msg, topic)
        # print(self.imbalance)
        print("CS self.imbalance_plus",self.imbalance_plus)
        print("CS self.imbalance_minus",self.imbalance_minus)

    """"    Update Station Availability Protocol 6.12"""
    def SendUpdatedStationAvailability(self,msg,topic):
        print("Send Updated Station Availability")
        a = ast.literal_eval(msg)
        recom=a[0]
        print(in_topic_10.replace('+', self.id),recom)
        return (in_topic_10.replace('+', self.id),recom)

    def ReceiveUpdateAvailabilityOutcome(self, msg, topic):
        print("Receive Update Availability Outcome")
        # a = topic.split("/")
        # print(a[0]+" "+self.id+" "+msg)
        # self.reg[a[0]] = msg
        self.reg["SR"] = msg

    """"    Electricity Prices Request Protocol 6.7"""
    def SendElectricityPricesRequest(self):
        print("Send Electricity Prices Request")
        return ServiceProvider.SendElectricityPricesRequest(self)
    def ReceiveElectricityPrices(self, msg, topic):
        print("Receive Electricity Prices")
        ServiceProvider.ReceiveElectricityPrices(self, msg, topic)
        print("self._buy_prices",self._buy_prices)
        print("self._sell_prices",self._sell_prices)



in_topic_1 = "EP/+/RegisterElectricityProducer"
in_topic_2 = "EC/+/RegisterElectricityConsumer"
in_topic_3 = "CS/+/RegisterChargingStation"
in_topic_4 = "EP/+/UpdateExpectedProduction"
in_topic_5 = "EC/+/UpdateExpectedConsumption"
in_topic_6 = "EP/+/UpdateConfidence"
in_topic_7 = "EC/+/UpdateConfidence"
in_topic_8 = "CS/+/UpdateConfidence"
in_topic_9 = "CS/+/UpdatedChargingSchedule"
in_topic_10 = "CS/+/UpdatedStationAvailability"
in_topic_11 = "MD/+/ElectricityPricesRequest"
in_topic_12 = "EI/+/ElectricityImbalanceRequest"
in_topic_13 = "EV/+/RequestChargingRecommendations"
in_topic_14 = "CS/+/ReserveChargingSlot"
in_topic_15 = "CS/+/AuthenticateRecommendation"
in_topic_16 = "CS/+/EV/+/EV_start/accept_negotiation"
in_topic_17 = "CS/+/EV/+/CS_start/start_negotiation"


out_topic_1 = "EI/+/RegistrationOutcome"
out_topic_2 = "MD/+/RegistrationOutcome"
out_topic_3 = "SR/+/RegistrationOutcome"
out_topic_4 = "EI/+/UpdateProfileOutcome"
out_topic_5 = "MD/+/UpdateProfileOutcome"
out_topic_6 = "EI/+/UpdateConfidenceOutcome"
out_topic_7 = "MD/+/UpdateConfidenceOutcome"
out_topic_8 = "EI/+/UpdateScheduleOutcome"
out_topic_9 = "MD/+/UpdateScheduleOutcome"
out_topic_10 = "CS/+/UpdateAvailabilityOutcome"
out_topic_11 = "MD/ElectricityPrices"
out_topic_12 = "EI/ElectricityImbalance"
out_topic_13 = "EV/+/ChargingRecommendations"
out_topic_14 = "EV/+/ReservationOutcome"
out_topic_15 = "CS/+/AuthenticateRecommendationOutcome"
out_topic_16 = "CS/+/EV/+/EV_start/start_negotiation"
out_topic_17 = "CS/+/EV/+/CS_start/accept_negotiation"


class ServiceProvider(BaseModel):
    def ReceiveRegistrationRequest(self, msg, topic):

        my_ID = find_ID(in_topic_1,topic)
        if my_ID is not None:
            return ServiceProvider.HandleRegistrationRequest(self, msg, my_ID, self.prod_dict, self.prod_conf_dict)

        my_ID = find_ID(in_topic_2,topic)
        if my_ID is not None:
            return ServiceProvider.HandleRegistrationRequest(self, msg, my_ID, self.cons_dict, self.cons_conf_dict)
    

    def HandleRegistrationRequest(self, msg, my_ID2, my_dict, my_conf_dict ):
        print("Handle Registration Request")
        x = ast.literal_eval(msg)
        list_obj = [PowerSlot.parse_raw(y) for y in x]
        

        if my_ID2 in my_dict:
            print("IPARXEI IDI")
            # update_or_append(my_dict,my_ID2,list_obj)
            update_or_append_list(my_dict[my_ID2],list_obj)
            my_dict[my_ID2] = my_dict[my_ID2][-2*time_interval:]
            my_conf_dict[my_ID2] = []
            print(my_dict)
            return self.__class__.SendRegistrationOutcome(self, my_ID2,"REGISTER AFTER UPDATE")
             

        my_dict[my_ID2] = list_obj
        my_conf_dict[my_ID2] = []
        print(my_dict)
        return self.__class__.SendRegistrationOutcome(self, my_ID2,"SUCCESS")
    
    def ReceiveChargingStationRequest(self, msg, topic):
        my_ID = find_ID(in_topic_3,topic)
        if my_ID is not None:
            return ServiceProvider.HandleChargingStationRegistration(self, msg, my_ID, self.station_dict)
     
    def HandleChargingStationRegistration(self, msg, my_ID2, my_dict):
        print("Handle Charging Station Registration") 
        # x = ast.literal_eval(msg)
        # list_obj = [PowerSlot.parse_raw(y) for y in x]
        list_obj = ChargingStation.parse_raw(msg)

        if my_ID2 in my_dict:
            print("IPARXEI IDI")
            # update_or_append(my_dict,my_ID2,list_obj)
            update_or_append_list(my_dict[my_ID2],list_obj)
            my_dict[my_ID2] = my_dict[my_ID2][-2*time_interval:]
            print(my_dict)
            return self.__class__.SendStationRegistrationOutcome(self, my_ID2,"REGISTER AFTER UPDATE")
             

        my_dict[my_ID2] = list_obj
        print(my_dict)
        return self.__class__.SendStationRegistrationOutcome(self, my_ID2,"SUCCESS")  

    
    
    """"    Expected Production Consumption Update Protocol 6.4 """
    def ReceiveUpdateExpectedProfileRequest(self, msg, topic):
        print("Receive Update Expected Profile Request")
        print("msg",msg)
        print("topic",topic)
        my_ID = find_ID(in_topic_4,topic)
        if my_ID is not None:
            return ServiceProvider.HandleUpdateExpectedProfileRequest(self, msg, my_ID, self.prod_dict)

        my_ID = find_ID(in_topic_5,topic)
        if my_ID is not None:
            return ServiceProvider.HandleUpdateExpectedProfileRequest(self, msg, my_ID, self.cons_dict)
        
    def HandleUpdateExpectedProfileRequest(self, msg, my_ID2, my_dict):
        print("Handle Update Expected Profile Request")
        x = ast.literal_eval(msg)
        list_obj = [PowerSlot.parse_raw(y) for y in x]
        
        # if my_ID2 in my_dict:
        #     # my_dict[my_ID2] = list_obj
        #     """ my_dict[my_ID2].extend(list_obj)
        #     my_dict[my_ID2] = my_dict[my_ID2][-2*time_interval:] """

        #     for a in list_obj:
        #         for i,y in enumerate(my_dict[my_ID2]):
        #             if a.dateTime == y.dateTime: #update kwh in known date
        #                 my_dict[my_ID2][i] = a

        #         if a not in my_dict[my_ID2] and sim_now<a.dateTime<sim_now + 2*time_interval*time_step:
        #             my_dict[my_ID2].append(a)        
        #             # else:  # new date
        #             #     my_dict[my_ID2].append(y)

        #     my_dict[my_ID2].sort(key=lambda y: y.dateTime)
        #     my_dict[my_ID2] = my_dict[my_ID2][-2*time_interval:]


            # print(my_dict)
        if my_ID2 in my_dict:
            # update_or_append(my_dict,my_ID2,list_obj)
            update_or_append_list(my_dict[my_ID2],list_obj)
            my_dict[my_ID2] = my_dict[my_ID2][-2*time_interval:]
            print(my_dict)
            return self.__class__.SendUpdateProfileOutcome(self, my_ID2,"SUCCESS UPDATE")
        
        print("UPDATE BEFORE REGISTER")
        my_dict[my_ID2] = list_obj
        print(my_dict)
        # return self.__class__.SendUpdateProfileOutcome(self, my_ID2,"FAIL UPDATE")
        return self.__class__.SendUpdateProfileOutcome(self, my_ID2,"UPDATE BEFORE REGISTER")
    def SendUpdateProfileOutcome(self):
        print("Send Update Profile Outcome")

    """"Update Energy Profile Confidence Protocol 6.11"""
    def ReceiveUpdateConfidenceRequest(self, msg, topic):
        print("Receive Update Confidence Request")
        my_ID = find_ID(in_topic_6,topic)
        if my_ID is not None:
            return ServiceProvider.HandleConfidenceUpdateRequest(self, msg, my_ID, self.prod_conf_dict)

        my_ID = find_ID(in_topic_7,topic)
        if my_ID is not None:
            return ServiceProvider.HandleConfidenceUpdateRequest(self, msg, my_ID, self.cons_conf_dict)

        my_ID = find_ID(in_topic_8,topic)
        if my_ID is not None:
            return ServiceProvider.HandleConfidenceUpdateRequest(self, msg, my_ID, self.station_conf_dict)
        
    def HandleConfidenceUpdateRequest(self, msg, my_ID2, my_dict):
        print("Handle Confidence Update Request")
        x = ast.literal_eval(msg)
        list_obj = [ConfidenceSlot.parse_raw(y) for y in x]
        
        if my_ID2 in my_dict:
            # my_dict[my_ID2] = list_obj
            update_or_append_list(my_dict[my_ID2],list_obj)
            my_dict[my_ID2] = my_dict[my_ID2][-2*time_interval:]
            
            print(my_dict)
            return self.__class__.SendConfidenceUpdateOutcome(self, my_ID2,"SUCCESS UPDATE CONFIDENCE")
     
        print("DEN IPARXEI")
        return self.__class__.SendConfidenceUpdateOutcome(self, my_ID2,"FAIL UPDATE CONFIDENCE")
    
    def SendConfidenceUpdateOutcome(self):
        print("Send Confidence Update Outcome")

    """"    Charging Station Update Schedule Protocol 6.9"""
    def ReceiveRequestUpdatedStationSchedule(self, msg, topic):
        print("Receive Request Updated Station Schedule")
        my_ID = find_ID(in_topic_9,topic)
        if my_ID is not None:
            return ServiceProvider.HandleUpdateStationSchedule(self, msg, my_ID, self.station_dict)

    def HandleUpdateStationSchedule(self, msg, id, my_dict):
        print("Handle Update Station Schedule")
        x = ast.literal_eval(msg)
        list_obj = [PowerSlot.parse_raw(y) for y in x]
        
        if id in my_dict:
            # my_dict[id] = list_obj
            add_or_append_list(my_dict[id]._station_schedule,list_obj)
            my_dict[id]._station_schedule = my_dict[id]._station_schedule[-15*time_interval:]
            #gia na krataei (olo to istoriko) ton CS new ton telefteon 15 imeron
            print("my_dict[id]._station_schedule",my_dict[id]._station_schedule)
            return self.__class__.SendUpdateScheduleOutcome(self, id,"SUCCESS SCHEDULE UPDATE")
     
        print("DEN IPARXEI")
        return self.__class__.SendUpdateScheduleOutcome(self, id,"FAIL SCHEDULE UPDATE")
    def SendUpdateScheduleOutcome(self):
        print("Send Update Schedule Outcome")

    """"    Electricity Prices Request Protocol 6.7"""
    def SendElectricityPricesRequest(self):
        print("Send Electricity Prices Request")
        return "0"
    def ReceiveElectricityPrices(self, msg, topic):
        print("Receive Electricity Prices")
        # x = ast.literal_eval(msg)
        # self.prices = [TimePrice.parse_raw(y) for y in x]
        buy,sell = ast.literal_eval(msg)
        # self._buy_prices = [TimePrice.parse_raw(y) for y in buy]
        # self._sell_prices = [TimePrice.parse_raw(y) for y in sell]
        buy_list = [TimePrice.parse_raw(y) for y in buy]
        sell_list = [TimePrice.parse_raw(y) for y in sell]
        
        # self._buy_prices.extend([TimePrice.parse_raw(y) for y in buy])
        # self._buy_prices = self._buy_prices[-2*time_interval:]
        update_or_append_list(self._buy_prices,buy_list)
        self._buy_prices = self._buy_prices[-2*time_interval:]
        print(self._buy_prices)

        # self._sell_prices.extend([TimePrice.parse_raw(y) for y in sell])
        # self._sell_prices = self._sell_prices[-2*time_interval:]
        update_or_append_list(self._sell_prices,sell_list)
        self._sell_prices = self._sell_prices[-2*time_interval:]
        print(self._sell_prices)



    """"    Electricity Imbalance Request Protocol 6.8"""
    def SendElectricityImbalanceRequest(self):
        print("Send Electricity Imbalance Request")
        return "0"

    def RecieveElectricityImbalance(self, msg, topic):
        print("Recieve Electricity Imbalance")
        q_plus,q_minus = ast.literal_eval(msg)
        # self.imbalance_plus = [PowerSlot.parse_raw(y) for y in q_plus]
        # self.imbalance_minus = [PowerSlot.parse_raw(y) for y in q_minus]
        imbalance_plus_list = [PowerSlot.parse_raw(y) for y in q_plus]
        imbalance_minus_list = [PowerSlot.parse_raw(y) for y in q_minus]

        # self.imbalance_plus.extend([PowerSlot.parse_raw(y) for y in q_plus])
        # self.imbalance_plus = self.imbalance_plus[-2*time_interval:]
        update_or_append_list(self.imbalance_plus,imbalance_plus_list)
        self.imbalance_plus = self.imbalance_plus[-2*time_interval:]
        print(self.imbalance_plus)

        # self.imbalance_minus.extend([PowerSlot.parse_raw(y) for y in q_minus])
        # self.imbalance_minus = self.imbalance_minus[-2*time_interval:]
        update_or_append_list(self.imbalance_minus,imbalance_minus_list)
        self.imbalance_minus = self.imbalance_minus[-2*time_interval:]
        print(self.imbalance_minus)

class ElectricityImbalance(BaseModel):
    prod_dict = {}
    cons_dict = {}
    station_dict = {}

    prod_conf_dict = {}
    cons_conf_dict = {}
    station_conf_dict = {}

    imbalance_plus = []
    imbalance_minus =[]
    """"    Expected Production Consumption Update Protocol 6.4 """
    def ReceiveUpdateExpectedProfileRequest(self, msg, topic):
        print("Receive Update Expected Profile Request")
        return ServiceProvider.ReceiveUpdateExpectedProfileRequest(self, msg, topic)
    def HandleUpdateExpectedProfileRequest(self):
        print("Handle Update Expected Profile Request")
    def SendUpdateProfileOutcome(self, id, outcome):
        print("Send Update Profile Outcome")
        return (out_topic_4.replace('+', id),outcome)

    """"    Charging Station Registration Protocol 6.5 cp4"""
    def ReceiveChargingStationRequest(self, msg, topic):
        print("Receive Charging Station Request")
        return ServiceProvider.ReceiveChargingStationRequest(self, msg, topic)
    def HandleChargingStationRegistration(self):
        print("Handle Charging Station Registration")
    def SendStationRegistrationOutcome(self, id, outcome):
        print("Send Station Registration Outcome")
        return (out_topic_1.replace('+', id),outcome)


    """"    Electricity Imbalance Request Protocol 6.8"""
    def ReceiveElectricityImbalanceRequest(self, msg, topic):
        print("Receive Electricity Imbalance Request")
        id = find_ID(in_topic_12,topic)
        if id is not None:
            return ElectricityImbalance.HandleElectricityImbalanceRequest(self, msg, id)

    def HandleElectricityImbalanceRequest(self, msg, id):
        print("Handle Electricity Imbalance Request")
        the_imbalance = 0
        the_imbalance2 = np.zeros(3*time_interval)
        print("################################################################################################",)
        sim_now0=sim_now-timedelta(hours=sim_now.hour)
        print("sim_now0",sim_now0)
        for prodid,value in self.prod_dict.items():
            i=0
            for v in (value):
                # if v.dateTime<datetime.now()+timedelta(days=days):
                # if sim_now0<=v.dateTime<=sim_now0+timedelta(days=1):
                if sim_now0+timedelta(days=int(msg))<=v.dateTime<sim_now0+timedelta(days=1+int(msg)):
                    # index_min=int(v.dateTime.minute/(time_step.seconds/60))
                    # index_hr =  int(v.dateTime.hour * time_intervals_per_hour)
                    # index_day = (v.dateTime-sim_now0).days*24*time_intervals_per_hour-24
                    # the_imbalance2[index_min+index_hr+index_day]+=v.kwh
                    the_imbalance2[i]+=v.kwh
                    i+=1
                    # print("the_imbalance2[index_min+index_hr+index_day]",the_imbalance2[index_min+index_hr+index_day])

        # print(the_imbalance2)

        # for prodid,value in self.prod_dict.items():
        #     print("to prodid= ",prodid)
        #     print("to value= ",value[0].kwh)
        #     print("to hour= ",value[0].dateTime.hour)
        #     the_imbalance += value[0].kwh
            
        #     for v in value:
        #         if v.dateTime<=sim_now0+timedelta(days=1):
        #             the_imbalance2[v.dateTime.hour]+=v.kwh

        #     print("to the_imbalance2= ",the_imbalance2)

        # for consid,value in self.cons_dict.items():
        #     print("to consid= ",consid)
        #     print("to value= ",value[0].kwh)
        #     the_imbalance -= value[0].kwh
        the_imbalance3 = np.zeros(3*time_interval)
        for prodid,value in self.cons_dict.items():
            i=0
            for v in value:
                # if sim_now0<=v.dateTime<=sim_now0+timedelta(days=1):
                if sim_now0+timedelta(days=int(msg))<=v.dateTime<sim_now0+timedelta(days=1+int(msg)):
                    # index_min=int(v.dateTime.minute/(time_step.seconds/60))
                    # index_hr =  int(v.dateTime.hour * time_intervals_per_hour)
                    # index_day = (v.dateTime-sim_now0).days*24*time_intervals_per_hour-24
                    # # print(index_min+index_hr+index_day)
                    # the_imbalance3[index_min+index_hr+index_day]+=v.kwh
                    the_imbalance3[i]+=v.kwh
                    i+=1

        # print(the_imbalance3)
        for prodid,value in self.station_dict.items():
            for v in value._station_schedule:
                # if sim_now0<=v.dateTime<=sim_now0+timedelta(days=1):
                if sim_now0+timedelta(days=int(msg))<=v.dateTime<sim_now0+timedelta(days=1+int(msg)):
                    # index_min=int(v.dateTime.minute/(time_step.seconds/60))
                    # index_hr =  int(v.dateTime.hour * time_intervals_per_hour)
                    # index_day = (v.dateTime-sim_now0).days*24*time_intervals_per_hour-24
                    # # print(index_min+index_hr+index_day)
                    # the_imbalance3[index_min+index_hr+index_day]+=v.kwh
                    index_hr =  int(v.dateTime.hour )
                    # index_day = (v.dateTime-sim_now0).days*24-24
                    if v.kwh<0:
                        the_imbalance2[index_hr]+=-v.kwh*1
                    else:
                        the_imbalance3[index_hr]+=v.kwh*1
                    
        return ElectricityImbalance.SendElectricityImbalance(self,msg, id,the_imbalance2, the_imbalance3)

    def SendElectricityImbalance(self,msg ,id ,the_imbalance2, the_imbalance3):
        print("Send Electricity Imbalance")
        sim_now0=sim_now-timedelta(hours=sim_now.hour)
        p1 = PowerSlot(dateTime= sim_now0 - time_step+timedelta(days=int(msg)))#auriani mera
        print(p1)
        print(the_imbalance2)
        x = []
        for i in range(time_interval):
            p1.set_kwh(the_imbalance2[i])
            x.append(p1.copy())
        msg1 = str([a.json() for a in x[-time_interval:]])

        p2=PowerSlot(dateTime= sim_now0 - time_step+timedelta(days=int(msg)))#auriani mera
        y = []
        for i in range(time_interval):
            p2.set_kwh(the_imbalance3[i])
            y.append(p2.copy())
        msg2 = str([a.json() for a in y[-time_interval:]])

        msg = str(([a.json() for a in x[-time_interval:]],[a.json() for a in y[-time_interval:]]))
        # msg = str(([a.json() for a in x[-time_interval:]],0))
        print(msg)

        return (out_topic_12,msg)
        # return (out_topic_12.replace('+', id),msg)

    """"    Charging Station Update Schedule Protocol 6.9"""
    def ReceiveRequestUpdatedStationSchedule(self, msg, topic):
        print("Receive Request Updated Station Schedule")
        return ServiceProvider.ReceiveRequestUpdatedStationSchedule(self, msg, topic)
    def HandleUpdateStationSchedule(self):
        print("Handle Update Station Schedule")
    def SendUpdateScheduleOutcome(self, id, outcome):
        print("Send Update Schedule Outcome")
        print(out_topic_8.replace('+', id),outcome)
        return (out_topic_8.replace('+', id),outcome)

    """"    Producer Consumer Registration Protocol 6.10"""
    def ReceiveRegistrationRequest(self, msg, topic):
        return ServiceProvider.ReceiveRegistrationRequest(self, msg, topic)
        # print("Receive Registration Request")
        # # EP/EP1/expected_production
        # a = topic.split("/")
        # x = ast.literal_eval(msg)
        # list_obj = [PowerSlot.parse_raw(y) for y in x]
        # if a[0]=="EP":
        #     self.prod_dict[a[1]] = list_obj
        #     print(self.prod_dict)
        #     return self.prod_dict
        # if a[0]=="EC":
        #     self.cons_dict[a[1]] = list_obj
        #     print(self.cons_dict)
        #     return self.cons_dict

        
        
    def HandleRegistrationRequest(self):
        print("Handle Registration Request")
    def SendRegistrationOutcome(self, id, outcome):
        # print("Electricity Imbalance Send Registration Outcome "+outcome)
        print(out_topic_1.replace('+', id),outcome)
        return (out_topic_1.replace('+', id),outcome)

    """"Update Energy Profile Confidence Protocol 6.11"""
    def ReceiveUpdateConfidenceRequest(self, msg, topic):
        print("Receive Update Confidence Request")
        return ServiceProvider.ReceiveUpdateConfidenceRequest(self, msg, topic)
    def HandleConfidenceUpdateRequest(self):
        print("Handle Confidence Update Request")
    def SendConfidenceUpdateOutcome(self, id, outcome):
        print("Send Confidence Update Outcome")
        print(out_topic_6.replace('+', id),outcome)
        return (out_topic_6.replace('+', id),outcome)

class MechanismDesign(BaseModel):
    prod_dict = {}
    cons_dict = {}
    station_dict = {}

    prod_conf_dict = {}
    cons_conf_dict = {}
    station_conf_dict = {}

    imbalance_plus = []
    imbalance_minus =[]

    _buy_prices =  PrivateAttr([])
    _sell_prices =  PrivateAttr([])

    # """"    Expected Production Consumption Update Protocol 6.4 """
    # def ReceiveUpdateExpectedProfileRequest(self):
    #     print("Receive Update Expected Profile Request")
    # def HandleUpdateExpectedProfileRequest(self):
    #     print("Handle Update Expected Profile Request")
    # def SendUpdateProfileOutcome(self):
    #     print("Send Update Profile Outcome")
    """"    Expected Production Consumption Update Protocol 6.4 """
    def ReceiveUpdateExpectedProfileRequest(self, msg, topic):
        print("Receive Update Expected Profile Request")
        return ServiceProvider.ReceiveUpdateExpectedProfileRequest(self, msg, topic)
    def HandleUpdateExpectedProfileRequest(self):
        print("Handle Update Expected Profile Request")
    def SendUpdateProfileOutcome(self, id, outcome):
        print("Send Update Profile Outcome")
        return (out_topic_5.replace('+', id),outcome)

    """"    Charging Station Registration Protocol 6.5 cp4"""
    def ReceiveChargingStationRequest(self, msg, topic):
        print("Receive Charging Station Request")
        return ServiceProvider.ReceiveChargingStationRequest(self, msg, topic)
    def HandleChargingStationRegistration(self):
        print("Handle Charging Station Registration")
    def SendStationRegistrationOutcome(self, id, outcome):
        print("Send Station Registration Outcome")
        print(out_topic_2.replace('+', id),outcome)
        return (out_topic_2.replace('+', id),outcome)

    """"    Electricity Imbalance Request Protocol 6.8"""
    def SendElectricityImbalanceRequest(self):
        print("Send Electricity Imbalance Request")
        return ServiceProvider.SendElectricityImbalanceRequest(self)
    def RecieveElectricityImbalance(self, msg, topic):
        print("Recieve Electricity Imbalance")
        ServiceProvider.RecieveElectricityImbalance(self, msg, topic)
        print("self.imbalance_plus",self.imbalance_plus)
        print()
        print("self.imbalance_minus",self.imbalance_minus)
   
    """"    Charging Station Update Schedule Protocol 6.9"""
    def ReceiveRequestUpdatedStationSchedule(self, msg, topic):
        print("Receive Request Updated Station Schedule")
        return ServiceProvider.ReceiveRequestUpdatedStationSchedule(self, msg, topic)
    def HandleUpdateStationSchedule(self):
        print("Handle Update Station Schedule")
    def SendUpdateScheduleOutcome(self, id, outcome):
        print("Send Update Schedule Outcome")
        print(out_topic_9.replace('+', id),outcome)
        return (out_topic_9.replace('+', id),outcome)
    
    """"    Producer Consumer Registration Protocol 6.10"""
    def ReceiveRegistrationRequest(self, msg, topic):
        return ServiceProvider.ReceiveRegistrationRequest(self, msg, topic)

    def HandleRegistrationRequest(self):
        print("Handle Registration Request")
    def SendRegistrationOutcome(self, my_ID4, outcome):
        # print("Mechanism Design Send Registration Outcome "+ outcome)
        print(out_topic_2.replace('+', my_ID4),outcome)
        return (out_topic_2.replace('+', my_ID4),outcome)

    """"Update Energy Profile Confidence Protocol 6.11"""
    def ReceiveUpdateConfidenceRequest(self, msg, topic):
        print("Receive Update Confidence Request")
        return ServiceProvider.ReceiveUpdateConfidenceRequest(self, msg, topic)
    def HandleConfidenceUpdateRequest(self):
        print("Handle Confidence Update Request")
    def SendConfidenceUpdateOutcome(self, id, outcome):
        print("Send Confidence Update Outcome")
        print(out_topic_7.replace('+', id),outcome)
        return (out_topic_7.replace('+', id),outcome)

    """"    Electricity Prices Request Protocol 6.7"""
    def ReceiveElectiricityPricesRequest(self, msg, topic):
        print("Receive Electiricity Prices Request")
        id = find_ID(in_topic_11,topic)
        if id is not None:
            return MechanismDesign.HandleElectricityPricesRequest(self, msg, id)

    def HandleElectricityPricesRequest(self, msg, id):
        print("Handle Electricity Prices Request")
        #CHECKAREI TO IMBALANCE KAI IPOLOGIZEI TIS TIMES
        the_price = 8


        return MechanismDesign.SendElectricityPrices(self,msg, id, the_price)

    def SendElectricityPrices(self,msg ,id ,the_price):
        global min2,max2
        print("SendElectricityPrices")
        price_round=5
        sim_now0=sim_now-timedelta(hours=sim_now.hour)
        

        if  nrgcoin_algo:
        # if sim_now0 <= datetime(2020,1,2,0,0) or nrgcoin_algo:
            p1 = TimePrice(dateTime= sim_now0 - time_step+timedelta(days=int(msg)))
            if sim_now0 ==datetime(2020,1,1,0,0) and msg == "0" :
                p1 = TimePrice(dateTime= sim_now0 +timedelta(days=int(msg)))

            # p1 = TimePrice(dateTime= self.imbalance_plus[0].dateTime- time_step)
            print(p1)
            x = []
            # for i in range(time_interval):
            for i,imb in enumerate(self.imbalance_plus): 
                if sim_now0+timedelta(days=int(msg))<=imb.dateTime<sim_now0+timedelta(days=1+int(msg)):
                # if sim_now+timedelta(days=int(msg)-2/24)<=imb.dateTime<sim_now+timedelta(days=1+int(msg)):
                    try:
                        b_price = Bt_buy(1,self.imbalance_plus[i].kwh,self.imbalance_minus[i].kwh)
                        
                        p1.set_price( round(b_price,price_round))
                        print("buyyyyyy",i,self.imbalance_plus[i].kwh,self.imbalance_minus[i].kwh,b_price)
                        # username = input("discharge !! ")
                    except:
                        p1.set_price(0.5)
                    x.append(p1.copy())

            

            # for i in range(time_interval):
            #     try:
            #         p1.set_price( round(Bt_buy(1,self.imbalance_plus[i].kwh,self.imbalance_minus[i].kwh),price_round))
            #     except:
            #         p1.set_price(0.5)
            #     x.append(p1.copy())
            msg1 = str([a.json() for a in x[-time_interval:]])

            p2=TimePrice(dateTime= sim_now0 - time_step+timedelta(days=int(msg)))
            if sim_now0 ==datetime(2020,1,1,0,0) and msg == "0":
                p2=TimePrice(dateTime= sim_now0 +timedelta(days=int(msg)))


            # p2 = TimePrice(dateTime= self.imbalance_plus[0].dateTime- time_step)
            y = []
            for i,imb in enumerate(self.imbalance_plus): 
                # if sim_now+timedelta(days=int(msg)-2/24)<=imb.dateTime<sim_now+timedelta(days=1+int(msg)):
                if sim_now0+timedelta(days=int(msg))<=imb.dateTime<sim_now0+timedelta(days=1+int(msg)):
            
                    try:
                        s_price = Bt_sell(1,self.imbalance_plus[i].kwh,self.imbalance_minus[i].kwh)
                        p2.set_price( round(s_price,price_round) )
                        # print("selllll",i,self.imbalance_plus[i].kwh,self.imbalance_minus[i].kwh,s_price)
                        # username = input("discharge !! ")

                    except:
                        p2.set_price(0.2)
                    y.append(p2.copy())
            msg2 = str([a.json() for a in y[-time_interval:]])

            msg = str(([a.json() for a in x[-time_interval:]],[a.json() for a in y[-time_interval:]]))
            # msg = str(([a.json() for a in x[-time_interval:]],0))
            print(msg)



            update_or_append_list(self._buy_prices,x)
            self._buy_prices = self._buy_prices[-15*time_interval:]
            print("self._buy_prices MD",self._buy_prices)

            update_or_append_list(self._sell_prices,y)
            self._sell_prices = self._sell_prices[-15*time_interval:]
            print("self._sell_prices MD",self._sell_prices)

            print()

        
        #an den einai i proti mera
        else:        
            c_star = []
            for i,imb in enumerate(self.imbalance_plus): 
                if sim_now0+timedelta(days=int(msg))<=imb.dateTime<sim_now0+timedelta(days=1+int(msg)):
                    c_star.append(self.imbalance_plus[i].kwh - self.imbalance_minus[i].kwh)
            print("c_star",c_star)
            """  c_prime = np.zeros(1*time_interval*15)
            for prodid,value in self.station_dict.items():
                for i,v in enumerate(value._station_schedule):
                    # if sim_now0+timedelta(days=-1)<=v.dateTime<sim_now0+timedelta(days=0):#-1 gia tin proigoumeni mera
                    if start_sim<=v.dateTime<sim_now0+timedelta(days=1):#apo arxi tou sim mexri to telos tis simerinis meras
                        # index_hr =  int(v.dateTime.hour )
                        if v.kwh>0:# koitazei tin fortisi ton EV
                            c_prime[i]+=v.kwh*1
                        # else:
                        #     the_imbalance3[index_hr]+=-v.kwh*1
            print("c_prime",c_prime)
            P_prime = np.zeros(time_interval*15)
            for i,v in enumerate(self._buy_prices):
                # if sim_now0+timedelta(days=-1)<=v.dateTime<sim_now0+timedelta(days=0):#-1 gia tin proigoumeni mera
                if start_sim<=v.dateTime<sim_now0+timedelta(days=1):#apo arxi tou sim mexri to telos tis simerinis meras
                    # index_hr =  int(v.dateTime.hour )
                    P_prime[i]=v.price
            print("P_prime",P_prime) """

            # a_hat = a_hat_calcutator(P_prime,c_prime)
            # print("a_hat",a_hat)
            # if np.isnan(a_hat) :
            #     print("a_hat =",a_hat,"??????????????????????????????????????????")

            a_hat=-0.000150690
            P=[2*a_hat*c+0.6 for c in c_star]
            # a_hat=-0.169131779
            # P=[2*a_hat*c/1000+0.6 for c in c_star]
            print(P)
            """ arr = np.array(P)
            l, h = 0.1, 0.65
            # min2 =min(min2,arr.min())
            # max2 =max(max2,arr.max())
            #https://stackoverflow.com/questions/50159000/how-to-scale-a-list-by-a-weight
            # P_scaled=(arr - min2) / (max2 - min2) * (h - l) + l
            P_scaled=(arr - arr.min()) / (arr.max() - arr.min()) * (h - l) + l
            print("P_scaled",P_scaled) """

            p3=TimePrice(dateTime= sim_now0 - time_step+timedelta(days=int(msg)))
            
            z = []
            for p in P:
                s_price = p
                p3.set_price( round(s_price,price_round) )
                z.append(p3.copy())

            msg = str(([a.json() for a in z[-time_interval:]],[a.json() for a in z[-time_interval:]]))
            # msg = str(([a.json() for a in x[-time_interval:]],0))
            print(msg)

            update_or_append_list(self._buy_prices,z)
            self._buy_prices = self._buy_prices[-15*time_interval:]
            print("self._buy_prices MD",self._buy_prices)

            update_or_append_list(self._sell_prices,z)
            self._sell_prices = self._sell_prices[-15*time_interval:]
            print("self._sell_prices MD",self._sell_prices)

            print()


        # p1=TimePrice()# PREPEI NA GINEI TIMEPRICE
        # print(p1)
        # x = []
        # for i in range(time_interval):
        #     p1.set_price(the_price)
        #     x.append(p1.copy())
        # msg = str([a.json() for a in x[-time_interval:]])

        # print(msg)
        return (out_topic_11,msg)
        # return (out_topic_11.replace('+', id),msg)



class StationRecommender(BaseModel):

    station_dict = {}
    # prices = []
    imbalance = []
    rec_history = []

    _buy_prices =  PrivateAttr([])
    _sell_prices =  PrivateAttr([])

    rec_id = 3000

    """"    Charging Recommendation Protocol 6.1 """
    def ReceiveRecommendationRequest(self, msg, topic):
        print("Receive Recommendation Request")
        id = find_ID(in_topic_13,topic)
        if id is not None:
            print("ID",id)
            return StationRecommender.CalculateChargingRecommendations(self, msg, id)

    def CalculateChargingRecommendations(self, msg, id):
        print("Calculate Charging Recommendations")
        print("ID",id)
        ev_id=id
       
        # ev=ElectricVehicle.parse_raw(msg)
        # pref = ev.preferences
        msg1,msg2 = ast.literal_eval(msg)
        pref = EVPreferences.parse_raw(msg1) 
        EV_loc = Location.parse_raw(msg2) 

        print("************************************************************************")
        print(pref)
        print("************************************************************************")
        # rec1 = ChargingRecommendation(id = "rec999", stationID = "CS999",slotID = 1)

        #  self.station_dict
        
        # rec2 = [ ChargingRecommendation(id = "rec100"+str(i), stationID = key) for i,key in enumerate(self.station_dict)]

        rec3 = []
        for key,value in self.station_dict.items():
            # if key.chargingSlot.connectorType == pref.chargingType.connectorType:
            print(value)
            print(type(value))
            # x = ast.literal_eval(value)
            # slots = [ChargingSlot.parse_raw(y) for y in x]
            slots = value.chargingSlots

            for slot in slots:
                if pref.chargingType.connectorType == slot.connectorType and pref.chargingType.ratedPower == slot.ratedPower:
                    #an iparxei slot pou to prf.arival einai < mikrotero apo ta departure 
                    # kai to
                    can_fit = True#exei xoro na mpei to reservation
                    # if slot.reservations is not None:
                    for reservation in slot.reservations:
                        if reservation.departure<pref.arrival or pref.departure <reservation.departure:
                            can_fit = True
                            print("CAN FIT")
                            break
                        else:
                            print("CAN NOT FIT")
                            can_fit=False
                    
                    if can_fit:
                        consumption_charging=pref.totalKWHs
                        rec3.append(ChargingRecommendation(id = "rec"+str(self.rec_id), stationID = key,
                            slotID=slot.slotID,issueDate = sim_now, arrival = pref.arrival, departure = pref.departure,
                            connectorType=pref.chargingType.connectorType,chargingPower=pref.chargingType.ratedPower,category=pref.chargingType.category,
                            totalKWHs=consumption_charging,stationNetwork=value.stationNetwork,location=value.location,evid = ev_id))
                        self.rec_id+=1
                        break#vriskei 1 mono slot gia recommendation gia kathe CS
                        
        #rec3.sort(distance,price)
        #
        # rec3 = sorted(rec3, key=lambda x: (x[1][0]-EV_loc[0])**2+(x[1][1]-EV_loc[1])**2)
        rec3 = sorted(rec3, key=lambda x: EV_loc.distance(x.location))
        rec3=rec3[:max_recom]                
        print("THE RECCOMENTATION is ",rec3)        
        self.rec_history.append(rec3.copy())
        return StationRecommender.SendChargingRecommendations(self, id,rec3)

    def SendChargingRecommendations(self,id ,recommendations):
        print("Send Charging Recommendations")
        # msg = recommendations.json()
        msg = str([a.json() for a in recommendations])
        return (out_topic_13.replace('+', id),msg)

    """"    Charging Station Registration Protocol 6.5 cp4"""
    def ReceiveChargingStationRequest(self, msg, topic):
        print("Receive Charging Station Request")
        return ServiceProvider.ReceiveChargingStationRequest(self, msg, topic)
    def HandleChargingStationRegistration(self):
        print("Handle Charging Station Registration")
    def SendStationRegistrationOutcome(self, id, outcome):
        print("Send Station Registration Outcome")
        print(out_topic_3.replace('+', id),outcome)
        return (out_topic_3.replace('+', id),outcome)

    """"    Authenticate Recommendation Protocol 6.6"""
    def ReceiveRecommendationAuthenticationQuery(self, msg, topic):
        print("Receive Recommendation Authentication Query")
        test_rec = ChargingRecommendation(id = "rec1000", stationID= "CS0")
        check_rec = ChargingRecommendation.parse_raw(msg)
        print("check_rec")
        print(check_rec)
        print("self.rec_history")
        print(self.rec_history)
        outcome_bool = any([check_rec in y for y in self.rec_history])
        outcome = str((check_rec.json(),outcome_bool))
        id = check_rec.stationID
        return StationRecommender.SendAuthenticationOutcome(self, id, outcome)
    def SendAuthenticationOutcome(self, id, outcome):
        print("Send Authentication Outcome")
        return (out_topic_15.replace('+', id),outcome)

    """"    Electricity Imbalance Request Protocol 6.8"""
    def SendElectricityImbalanceRequest(self):
        print("Send Electricity Imbalance Request")
        return ServiceProvider.SendElectricityImbalanceRequest(self)
    def RecieveElectricityImbalance(self, msg, topic):
        print("Recieve Electricity Imbalance")
        ServiceProvider.RecieveElectricityImbalance(self, msg, topic)
        print(self.imbalance)

    """"    Update Station Availability Protocol 6.12"""
    def ReceiveRequestUpdateAvailability(self, msg, topic):
        print("Receive Request Update Availability")
        my_ID = find_ID(in_topic_10,topic)
        if my_ID is not None:
            return StationRecommender.HandleStationAvailabilityUpdate(self, msg, my_ID, self.station_dict)
        

    def HandleStationAvailabilityUpdate(self, msg, id, my_dict):
        print("Handle Station Availability Update")
        # x = ast.literal_eval(msg)
        # list_obj = [ChargingSlot.parse_raw(y) for y in x]
        x = ChargingRecommendation.parse_raw(msg)
        # csid, slotid, reservation = ast.literal_eval(msg)
        
        if id in my_dict:
            # my_dict[id] = list_obj
            # for slot in list_obj:
            # my_dict[id].chargingSlots[x.slotID].reservations = Reservation(id=x.id,
            #                 arrival = x.arrival,departure= x.departure)
            my_dict[id].chargingSlots[x.slotID].reservations.append(Reservation(id=x.id,
                            arrival = x.arrival,departure= x.departure))
            my_dict[id].chargingSlots[x.slotID].reservations=my_dict[id].chargingSlots[x.slotID].reservations[-5:]

            print(my_dict)
            return StationRecommender.SendUpdateAvailabilityOutcome(self, id,"SUCCESS SCHEDULE UPDATE")
     
        print("DEN IPARXEI")
        return StationRecommender.SendUpdateAvailabilityOutcome(self, id,"FAIL SCHEDULE UPDATE")
    
    def SendUpdateAvailabilityOutcome(self, id, outcome):
        print("Send Update Availability Outcome")
        return (out_topic_10.replace('+', id),outcome)

    """"    Electricity Prices Request Protocol 6.7"""
    def SendElectricityPricesRequest(self):
        print("Send Electricity Prices Request")
        return ServiceProvider.SendElectricityPricesRequest(self)
    def ReceiveElectricityPrices(self, msg, topic):
        print("Receive Electricity Prices")
        ServiceProvider.ReceiveElectricityPrices(self, msg, topic)
        print("self._buy_prices",self._buy_prices)
        print("self._sell_prices",self._sell_prices)


            
def simulate():
    sim_start = datetime(2020, 10, 1, 0, 0)
    sim_end = datetime(2020, 10, 2, 0, 0)
    # time_step = timedelta(hours=1)

    print(sim_start)
    print(sim_end)
    print(time_step)

    t= sim_start
    while t<sim_end:

        print(t)
        t+=time_step

def main(): 
    global sim_now
# def __init__(self):
    

    md=MechanismDesign()

    
    EP1=ElectricityProducer(id = "EP1")
    EP2=ElectricityProducer(id = "EP2")
    EC0=ElectricityProducer(id = "EC0")
    imbal = ElectricityImbalance()
    # EP0.SendRegistrationRequest()

    producer_type = ["Wind_Gen(MW)","Wind_Gen(MW)"]
    n_prod = 2 

    # msg = EP2.SendRegistrationRequest(column="Lignite_Gen(MW)")
    msg = EP1.SendRegistrationRequest(column= "Wind_Gen(MW)")
    # msg3 = EP1.SendRegistrationRequest(column= "Wind_Gen(MW)")
    msg2 = EC0.SendRegistrationRequest(column = "Total_Load(MW)")

    a=imbal.ReceiveRegistrationRequest(msg,"EP/EP1/RegisterElectricityProducer")
    b=imbal.ReceiveRegistrationRequest(msg2,"EC/EC0/RegisterElectricityConsumer")
    print("AAAAA",a)
    print("BBBBB",b)

    msg = EP1.SendUpdateExpectedEnergyProfile(day=1,column= "Wind_Gen(MW)")
    msg2 = EC0.SendUpdateExpectedEnergyProfile(day=1,column = "Total_Load(MW)")

    a=imbal.ReceiveUpdateExpectedProfileRequest(msg,"EP/EP1/UpdateExpectedProduction")
    b=imbal.ReceiveUpdateExpectedProfileRequest(msg2,"EC/EC0/UpdateExpectedConsumption")
    
    print("AAAAA",a)
    print("BBBBB",b)
    topic , msg111 = imbal.ReceiveElectricityImbalanceRequest("mpla","EI/MD/ElectricityImbalanceRequest")
    print(msg111)
    msg = EP1.SendUpdateExpectedEnergyProfile(day=2,column= "Wind_Gen(MW)")
    msg2 = EC0.SendUpdateExpectedEnergyProfile(day=2,column = "Total_Load(MW)")
    a=imbal.ReceiveUpdateExpectedProfileRequest(msg,"EP/EP1/UpdateExpectedProduction")
    b=imbal.ReceiveUpdateExpectedProfileRequest(msg2,"EC/EC0/UpdateExpectedConsumption")
    sim_now += timedelta(hours=24)
    
    topic , msg111 = imbal.ReceiveElectricityImbalanceRequest("mpla","EI/MD/ElectricityImbalanceRequest")
    
    print(msg111)
    aa=md.SendElectricityImbalanceRequest()
    md.RecieveElectricityImbalance( msg111,topic )


    EV0 = ElectricVehicle()
    EV0.location=Location(latitude=0,longitude=0)
    # print(EV0)
    
    CS0 = ChargingStation(id = "CS0")
    CS0.location=Location(latitude=30,longitude=30)
    # type={ratedPower=120.0, connectorType='3 phase-DC', category='level 3'}
    CS0.add_charging_slots(3,ratedPower=120.0, connectorType='3 phase-DC', category='level 3')
    print(CS0)
    
    CS1 = ChargingStation(id = "CS1")
    CS1.location=Location(latitude=40,longitude=40)
    CS1.add_charging_slots(1,ratedPower=120.0, connectorType='3 phase-DC', category='level 3')
    CS1.add_charging_slots(1,ratedPower=50.0, connectorType='3 phase-DC', category='level 3')
    CS1.add_charging_slots(1,ratedPower=43.0, connectorType='3 phase-60A per phase', category='level 3')
    CS1.add_charging_slots(1,ratedPower=22.0, connectorType='3 phase-32A per phase', category='level 2')
    CS1.add_charging_slots(1,ratedPower=11.0, connectorType='3 phase-16A per phase', category='level 2')
    CS1.add_charging_slots(1,ratedPower=7.4, connectorType='Single phase 32A', category='level 2')
    print(CS1)
   
    CS2 = ChargingStation(id = "CS2")
    
    SR = StationRecommender()

    msg1 = CS1.SendChargingStationRegistration()
    top2, msg2 = SR.ReceiveChargingStationRequest(msg1,in_topic_3.replace("+",CS1.id))

    msg1 = CS0.SendChargingStationRegistration()
    top2, msg2 = SR.ReceiveChargingStationRequest(msg1,in_topic_3.replace("+",CS0.id))

    
    # EV0 = ElectricVehicle(id=id,model=model)
    print("EV0",EV0)
    print()
    print("EV0",EV0._status_array)
    print()
    last_departure =0
    day_range = 7 
    sim_hours = 24*3
    # sim_now+=timedelta(days=3*365-1)
   
    for k in range(3):#duration
        
        print()
        print(k)
        print(last_departure,last_departure+day_range*time_interval)
        last_departure = EV0.find_linked_session(last_departure,last_departure+day_range*time_interval)
        
        
        # # print(top2)
        # # print(msg2)
        # print(SR.station_dict)

        msg3 = EV0.SendChargingRecommendationsRequest()
        top3, msg3 = SR.ReceiveRecommendationRequest(msg3,in_topic_13.replace("+",EV0.id))
        EV0.ReceiveRecommendations(msg3,top3)
        print(top3)
        print(msg3)
        print("LALALALALLALALAL")
        print(EV0.recommendations)

        print("RESERVATION !!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!")
        top4, msg4 = EV0.SendStationReservationRequest()
    
        print(top4)
        print(msg4)
        
        top5 , msg5 = CS1.ReceiveChargingReservationRequest(msg4,top4)
        print("RESERVATION2 !!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!")
        print(top5)
        print(msg5)
        
        top6 , msg6 = SR.ReceiveRecommendationAuthenticationQuery(msg5,top5)

        top7 , msg7 = CS1.ReceiveAuthenticationResponse(msg6,top6)
        EV0.ReceiveReservationOutcome(msg7,top7)
        print("EV0.reservations",EV0.reservations)
        print(top7)
        print(msg7)
        print("**CS1**      ",CS1) 
        # CS1.scheduling("rec"+str(3000+k))
        sim_now-=time_step*24
        for h in range(24):
            sim_now+=time_step
            print(sim_now)
            CS1.payment_calculation()

    # CS1.scheduling()
    # sim_hours = 24*3
    # sim_now+=timedelta(days=3*365)
    # for h in range(sim_hours):
    #     if h %24 == 0:
    #         CS1.scheduling()
    #     sim_now+=time_step
    #     print(sim_now)
    #     CS1.payment_calculation()
        #payment calculation
        #kathe ora tsekarei ta cs 
        #                   kathe cs tsekarei slot
        #                   kathe slot tsekarei ta reservations
                            #an i current hour(h) einai mesa sto arrival-departure
        # tote auksanei to payment kata buy_price[i]*  

        #payment calculation
        #kathe ora tsekarei ta cs
        #gia kathe id(car) sto schedule
        #an to sim_now einai == me to PowerSlot.dateTime tote 
        #   an to PowerSlot.kwh>0 tote balance-=PowerSlot.kwh*self._buy_price
        #   an to PowerSlot.kwh<0 tote balance+=PowerSlot.kwh*self._sell_price
        
        #se mia oriaia routina tha kaleite to payment calculation gia kathe CS
        


    # CS1 = ChargingStation(id = "CS1")
    # CS1.location=Location(latitude=40,longitude=40)
    # CS1.add_charging_slots(1,ratedPower=120.0, connectorType='3 phase-DC', category='level 3')
    # CS1.add_charging_slots(1,ratedPower=50.0, connectorType='3 phase-DC', category='level 3')
    # CS1.add_charging_slots(1,ratedPower=43.0, connectorType='3 phase-60A per phase', category='level 3')
    # CS1.add_charging_slots(1,ratedPower=22.0, connectorType='3 phase-32A per phase', category='level 2')
    # CS1.add_charging_slots(1,ratedPower=11.0, connectorType='3 phase-16A per phase', category='level 2')
    # CS1.add_charging_slots(1,ratedPower=7.4, connectorType='Single phase 32A', category='level 2')
    
    return
    
    """ imbal.ReceiveRegistrationRequest(msg2,"EP/EP1/RegisterElectricityProducer")
    imbal.ReceiveRegistrationRequest(msg,"EC/EC0/RegisterElectricityConsumer")
    
    # imbal.ReceiveRegistrationRequest(msg,"EP/EP1/RegisterElectricityProducer")
    

    msg2 = EP1.SendUpdateExpectedEnergyProfile(day=1,column= "Wind_Gen(MW)")
    msg = EC0.SendUpdateExpectedEnergyProfile(day=1,column = "Total_Load(MW)")

    imbal.ReceiveUpdateExpectedProfileRequest(msg2,"EP/EP1/UpdateExpectedProduction")
    imbal.ReceiveUpdateExpectedProfileRequest(msg,"EC/EC0/UpdateExpectedConsumption")
    
    print(msg)

    topic , msg111 = imbal.ReceiveElectricityImbalanceRequest("mpla","EI/MD/ElectricityImbalanceRequest")
    print(msg111)
    sim_now += timedelta(hours=25)
    topic , msg111 = imbal.ReceiveElectricityImbalanceRequest("mpla","EI/MD/ElectricityImbalanceRequest")
    print(msg111)
    # return
    msg2 = EP1.SendUpdateEnergyProfileConfidence(day=100,column= "Solar_Gen(MW)")
    msg = EC0.SendUpdateEnergyProfileConfidence(day=100,column = "Total_Load(MW)")
    print(msg)

    # return
    
    EV0 = ElectricVehicle()
    EV0.location=Location(latitude=0,longitude=0)
    # print(EV0)
    
    CS0 = ChargingStation(id = "CS0")
    CS0.location=Location(latitude=30,longitude=30)
    # type={ratedPower=120.0, connectorType='3 phase-DC', category='level 3'}
    CS0.add_charging_slots(3,ratedPower=120.0, connectorType='3 phase-DC', category='level 3')
    print(CS0)
    
    CS1 = ChargingStation(id = "CS1")
    CS1.location=Location(latitude=40,longitude=40)
    CS1.add_charging_slots(1,ratedPower=120.0, connectorType='3 phase-DC', category='level 3')
    CS1.add_charging_slots(1,ratedPower=50.0, connectorType='3 phase-DC', category='level 3')
    CS1.add_charging_slots(1,ratedPower=43.0, connectorType='3 phase-60A per phase', category='level 3')
    CS1.add_charging_slots(1,ratedPower=22.0, connectorType='3 phase-32A per phase', category='level 2')
    CS1.add_charging_slots(1,ratedPower=11.0, connectorType='3 phase-16A per phase', category='level 2')
    CS1.add_charging_slots(1,ratedPower=7.4, connectorType='Single phase 32A', category='level 2')
    print(CS1)
    # return
    # CS0.add_charging_slots()
    # CS0.add_charging_slots()
    # CS1 = ChargingStation(id = "CS1")
    # CS1.location=Location(latitude=40,longitude=40)
    # CS1.add_charging_slots()
    CS2 = ChargingStation(id = "CS2")
    
    SR = StationRecommender()

    msg1 = CS1.SendChargingStationRegistration()
    top2, msg2 = SR.ReceiveChargingStationRequest(msg1,in_topic_3.replace("+",CS1.id))

    msg1 = CS0.SendChargingStationRegistration()
    top2, msg2 = SR.ReceiveChargingStationRequest(msg1,in_topic_3.replace("+",CS0.id))

    
    # EV0 = ElectricVehicle(id=id,model=model)
    print("EV0",EV0)
    print()
    print("EV0",EV0._status_array)
    print()
    last_departure =0
    day_range = 7 
    for k in range(10):#duration
        
        print()
        print(k)
        print(last_departure,last_departure+day_range*time_interval)
        last_departure = EV0.find_linked_session(last_departure,last_departure+day_range*time_interval)
        
        
        # # print(top2)
        # # print(msg2)
        # print(SR.station_dict)

        msg3 = EV0.SendChargingRecommendationsRequest()
        top3, msg3 = SR.ReceiveRecommendationRequest(msg3,in_topic_13.replace("+",EV0.id))
        EV0.ReceiveRecommendations(msg3,top3)
        print(top3)
        print(msg3)
        print("LALALALALLALALAL")
        print(EV0.recommendations)

        print("RESERVATION !!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!")
        top4, msg4 = EV0.SendStationReservationRequest()
    
        print(top4)
        print(msg4)
        
        top5 , msg5 = CS1.ReceiveChargingReservationRequest(msg4,top4)
        print("RESERVATION2 !!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!")
        print(top5)
        print(msg5)
        
        top6 , msg6 = SR.ReceiveRecommendationAuthenticationQuery(msg5,top5)

        top7 , msg7 = CS1.ReceiveAuthenticationResponse(msg6,top6)
        EV0.ReceiveReservationOutcome(msg7,top7)
        print("EV0.reservations",EV0.reservations)
        print(top7)
        print(msg7)
    print("**CS1**      ",CS1) """

    
    # print(msg0)
    # ev1.find_linked_session(0,120)
    # for i in range(duration*time_interval):
    #     if ev1.get_status(i)==charging_id and ev1.get_status(i-1)!=charging_id:
    #         for k in range(1,linked_hours+1):
    #             # set_linked_hour(i-k,-1)
    #             ev1.set_linked_hour(i+k,1)


    """
    # EP0=ElectricityProducer(id = "EP0")
    EP1=ElectricityProducer(id = "EP1")
    # EP2=ElectricityProducer(id = "EP2")
    EC0=ElectricityProducer(id = "EC0")

    # EP0.SendRegistrationRequest()

    # msg = EP2.SendRegistrationRequest(column="Lignite_Gen(MW)")
    msg2 = EP1.SendRegistrationRequest(column= "Lignite_Gen(MW)")
    msg = EC0.SendRegistrationRequest(column = "Total_Load(MW)")

    msg2 = EP1.SendUpdateExpectedEnergyProfile(day=100,column= "Lignite_Gen(MW)")
    msg = EC0.SendUpdateExpectedEnergyProfile(day=100,column = "Total_Load(MW)")
    print(msg)
    # msg5 = EC0.SendRegistrationRequest()
    imba = ElectricityImbalance()
    imba.ReceiveRegistrationRequest(msg,"EC/EC0/RegisterElectricityConsumer")
    # imba.ReceiveRegistrationRequest(msg5,"EC/EC0/RegisterElectricityConsumer")
    imba.ReceiveRegistrationRequest(msg2,"EP/EP1/RegisterElectricityProducer")
    """    
    # topic , msg111 = imba.ReceiveElectricityImbalanceRequest("mpla","EI/CS111/ElectricityImbalanceRequest")

    # md = MechanismDesign()
    # md.RecieveElectricityImbalance(msg111,"EI/MD/ElectricityImbalance")


    # print("topic",topic)
    # print("msg111",msg111)
    """
    sim_start = datetime(2020, 10, 1, 0, 0)
    sim_end = datetime(2020, 10, 2, 0, 0)

    EV0 = ElectricVehicle(id = "EV0")
    EV0.preferences = EVPreferences(arrival=sim_start+1*time_step,departure = sim_start+6*time_step )

    CS0 = ChargingStation(id = "CS0")
    CS0.add_charging_slots()
    CS0.add_charging_slots()
    CS0.add_charging_slots()
    CS1 = ChargingStation(id = "CS1")
    CS1.add_charging_slots()
    CS2 = ChargingStation(id = "CS2")

    SR = StationRecommender()

    msg1 = CS1.SendChargingStationRegistration()
    top2, msg2 = SR.ReceiveChargingStationRequest(msg1,in_topic_3.replace("+",CS1.id))

    msg1 = CS0.SendChargingStationRegistration()
    top2, msg2 = SR.ReceiveChargingStationRequest(msg1,in_topic_3.replace("+",CS0.id))

    # # print(top2)
    # # print(msg2)
    # print(SR.station_dict)

    msg3 = EV0.SendChargingRecommendationsRequest()
    top3, msg3 = SR.ReceiveRecommendationRequest(msg3,in_topic_13.replace("+",EV0.id))
    EV0.ReceiveRecommendations(msg3,top3)
    print(top3)
    print(msg3)
    print("LALALALALLALALAL")
    print(EV0.recommendations)

    print("RESERVATION !!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!")
    top4, msg4 = EV0.SendStationReservationRequest()
   
    print(top4)
    print(msg4)
    
    top5 , msg5 = CS1.ReceiveChargingReservationRequest(msg4,top4)
    print("RESERVATION2 !!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!")
    print(top5)
    print(msg5)
    
    top6 , msg6 = SR.ReceiveRecommendationAuthenticationQuery(msg5,top5)

    top7 , msg7 = CS1.ReceiveAuthenticationResponse(msg6,top6)
    EV0.ReceiveReservationOutcome(msg7,top7)
    print(EV0.reservations)
    print(top7)
    print(msg7)

    CS1.scheduling()
    
    CS1.charging_with_schedule()

    t= sim_start
    while t<sim_end:
        # print("1",EV0.battery)
        CS1.charging(t)
        # print("2",EV0.battery)




        print(t)
        t+=time_step
    
    
    msg = CS1.SendUpdatedStationAvailability()
    SR.ReceiveRequestUpdateAvailability(msg,"CS/CS1/UpdatedStationAvailability")
    # CS1.SendUpdatedChargingSchedule()

    EV0.preferences = EVPreferences(arrival=datetime.now()+2*time_step,departure = datetime.now()+ 5*time_step )
    msg3 = EV0.SendChargingRecommendationsRequest()
    top3, msg3 = SR.ReceiveRecommendationRequest(msg3,in_topic_13.replace("+",EV0.id))
    EV0.ReceiveRecommendations(msg3,top3)
    print(top3)
    print(msg3)
    print("LALALALALLALALAL")
    print(EV0.recommendations)

    print("RESERVATION !!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!")
    top4, msg4 = EV0.SendStationReservationRequest()
    print(top4)
    print(msg4)
    top5 , msg5 = CS0.ReceiveChargingReservationRequest(msg4,top4)
    print("RESERVATION2 !!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!")
    print(top5)
    print(msg5)
    top6 , msg6 = SR.ReceiveRecommendationAuthenticationQuery(msg5,top5)

    top7 , msg7 = CS0.ReceiveAuthenticationResponse(msg6,top6)
    EV0.ReceiveReservationOutcome(msg7,top7)
    print(EV0.reservations)
    print(top7)
    print(msg7)

    msg=CS0.SendUpdatedStationAvailability()
    SR.ReceiveRequestUpdateAvailability(msg,"CS/CS0/UpdatedStationAvailability")
"""
    # print(CS0)
    # print(EV0)

    

    # top0,msg0 = CS0.SendAuthenticateRecommendationQuery()
    # print(top0)
    # print(msg0)
    
    # top1, msg1 = SR.ReceiveRecommendationAuthenticationQuery(msg0,top0)
    # print(top1)
    # print(msg1)
    # lala = CS0.ReceiveAuthenticationResponse(msg1,top1)


    # msg1 = CS0.SendChargingStationRegistration()
    # top2, msg2 = SR.ReceiveChargingStationRequest(msg1,in_topic_3.replace("+",CS0.id))

    # msg1 = CS1.SendChargingStationRegistration()
    # top2, msg2 = SR.ReceiveChargingStationRequest(msg1,in_topic_3.replace("+",CS1.id))

    # msg1 = CS2.SendChargingStationRegistration()
    # top2, msg2 = SR.ReceiveChargingStationRequest(msg1,in_topic_3.replace("+",CS2.id))

    # # print(top2)
    # # print(msg2)
    # print(SR.station_dict)

    # msg3 = EV0.SendChargingRecommendationsRequest()
    # top3, msg3 = SR.ReceiveRecommendationRequest(msg3,in_topic_13.replace("+",EV0.id))
    # EV0.ReceiveRecommendations(msg3,top3)
    # print(top3)
    # print(msg3)
    # print("LALALALALLALALAL")
    # print(EV0.recommendations)

    # print("RESERVATION !!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!")
    # top4, msg4 = EV0.SendStationReservationRequest()
    # print(top4)
    # print(msg4)
    # top5 , msg5 = CS1.ReceiveChargingReservationRequest(msg4,top4)
    # print("RESERVATION2 !!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!")
    # print(top5)
    # print(msg5)
    # EV0.ReceiveReservationOutcome(msg5,top5)
    # print(EV0.reservations)
    # rec2 = ChargingRecommendation()
    # print(rec2)
    # msg = rec2.copy().json()
    # x = ChargingRecommendation.parse_raw(msg)
    # print(x)
    # EV0 = ElectricVehicle(id = "EV0")

    # my_dict = {"CS1":"lalalala","CS2":"TATATATATA","CS3":"KAKAKAKA"}
    # time_int = 1
    # p1=ChargingRecommendation()
    # print(p1)
    # x = []
    # for i,key in enumerate(my_dict):
    #     p1 = ChargingRecommendation(id = "rec"+str(i),stationID=key)
    #     x.append(p1.copy())

    # # x = [ k := p1.set_kwh(10) for i in range(time_int)]
    # msg = str([a.json() for a in x[-time_int:]])
    # print(msg)
    # print(type(msg))

    # x1 =ast.literal_eval(msg)
    # print(x1)
    # aa = ChargingRecommendation.parse_raw(x1[0])
    # print(aa)
    # print(type(aa))

    # print("INIT")
    # EP0 = ElectricityProducer(id="EP0")
    # EC0 = ElectricityConsumer(id="EC0")

    # print(EP0.json())
    # print(EC0.json())


    # # prod_dict = {}
    # msg = EP0.SendRegistrationRequest()
    # msg2 = EC0.SendRegistrationRequest()
    # imba = ElectricityImbalance()
    # imba.ReceiveRegistrationRequest(msg,"EP/EP0/RegisterElectricityProducer")
    # imba.ReceiveRegistrationRequest(msg2,"EC/EC0/RegisterElectricityConsumer")
    # imba.ReceiveRegistrationRequest(msg,"EP/EP1/RegisterElectricityProducer")
    # print("MDDDDDDDD")
    # md = MechanismDesign()
    # md.ReceiveRegistrationRequest(msg,"EP/EP0/RegisterElectricityProducer")
    # md.ReceiveRegistrationRequest(msg2,"EC/EC0/RegisterElectricityConsumer")
    # md.ReceiveRegistrationRequest(msg,"EP/EP1/RegisterElectricityProducer")

    # EP0.ReceiveRegistrationOutcome( "SUCCESS","MD/EP0/RegistrationOutcome")
    # EP0.ReceiveRegistrationOutcome( "SUCCESS","EI/EP0/RegistrationOutcome")
    # id = "EP0"
    # print(type(msg))
    # x = ast.literal_eval(msg)
    # list_obj = [PowerSlot.parse_raw(y) for y in x]
    # print("type(list_obj)",type(list_obj))
    # print(type(list_obj[0]))

    # prod_dict[id] = list_obj
    # print(prod_dict)


if __name__ == '__main__':
    # execute only if run as the entry point into the program
    main()

    
# ev1 = ElectricVehicle()

# print(ev1.battery.soc)
# ev1.drive(10)
# print(ev1.battery.soc)
# ev1.battery.charge(0.05)
# print(ev1.battery.soc)

# loc1 = Location()
# faros = Location()
# faros.latitude = 24.01675932211589
# faros.longitude = 35.51958017109308
# ev1.location.distance(faros)

# cs1 = ChargingStation()
# cs1.add_charging_slots()
# cs1.add_charging_slots()
# cs1.add_charging_slots()
# cs1.chargingSlots[1].slotID = 1
# print(cs1.chargingSlots[1].slotID)

# data = {"mean":11,"std":0.3}
# a=Confidence(mean=12,std=0.7)
# print(a)
# b=ConfidenceSlot(confidence=a)
# str1=b.json()
# print(str1)
# con_slot = ConfidenceSlot.parse_raw(str1)
# print(con_slot)
# print(type(con_slot))



# p1=PowerSlot()

# print(p1.dateTime)
# now = datetime.now()
# sleep(1)
# next = datetime.today()
# # today = date.today()
# strnow = str(now)

# dt = datetime.strptime(strnow,"%Y-%m-%d %H:%M:%S.%f")
# print("Today's date: ", dt )
# print("Today's date2:", next)
# print("sub: ", next - now)

# charge_type = ChargingType(
#                             ratedPower=3,
#                             connectorType="type1",
#                             category="slow")
# print(charge_type)

# c_slot = ChargingSlot(**charge_type.dict(),slotID=1,netProfitPerKwh=1)
# print(c_slot)


