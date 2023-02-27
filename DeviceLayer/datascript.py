import paho.mqtt.client as mqtt
import time, datetime, random
import json
import os
def on_message(client, userdata, message):
    print("message received " ,str(message.payload.decode("utf-8")))
    print("message topic=",message.topic)
    print("message qos=",message.qos)
    print("message retain flag=",message.retain)

def on_log(client, userdata, level, buf):
    #print("log: ",buf)
    with open("logs.txt","a+") as f:
        f.write(f"log: {buf} \n")


file = open("logs.txt","w")
file.close()
client =mqtt.Client("Energy")
client.on_message=on_message 
client.on_log=on_log
client.connect("127.0.0.1")
client_water =mqtt.Client("Water")
client_water.on_message=on_message 
client_water.on_log=on_log
client_water.connect("127.0.0.1",port=1884)
client_temp =mqtt.Client("Temperature")
client_temp.on_message=on_message 
client_temp.on_log=on_log
client_temp.connect("127.0.0.1",port=1885)
topics_energy = ["IoT/energy/Airconditioning/HVAC1","IoT/energy/Airconditioning/HVAC2","IoT/energy/Rest_appliances/MiAC1","IoT/energy/Rest_appliances/MiAC2","IoT/energy/Etot"]
topics_water = ["IoT/water/W1","IoT/water/Wtot"]
topics_temp = ["IoT/temperature/TH1","IoT/temperature/TH2","IoT/movement/Mov1"]
for topic in topics_energy:
    client.subscribe(topic,qos=1)
#    client.publish(topic,"I'm in")
for topic in topics_water:
    client_water.subscribe(topic,qos=1)
for topic in topics_temp:
    client_temp.subscribe(topic,qos=1)
counter = 0
start_timestamp = datetime.datetime.strptime("2023-01-01 00:00:00", "%Y-%m-%d %H:%M:%S")
print(start_timestamp)
Etot = 0
Wtot = 0
W112days = 0
W12days = 0
late2=late1=0
movements = random.sample(range(0,96), 5)
while (True):
    move = 0
    th1 = round(random.uniform(12,35),2)
    th2 = round(random.uniform(12,35),2)
    HVAC1 = round(random.uniform(0,100),2)
    HVAC2 = round(random.uniform(0,200),2)
    MIAC1 = round(random.uniform(0,150),2)
    MIAC2 = round(random.uniform(0,200),2)
    W1 = round(random.uniform(0,1),2)
    if (counter%96==0):
        Etot += 2600*24 + round(random.uniform(-1000,1000),2)
        Etot = round(Etot,2)
        etotfinal = start_timestamp.strftime("%Y-%m-%d %H:%M:%S") + f" | {Etot}"
        client.publish("IoT/energy/Etot", etotfinal,qos=1)
        client.loop(2,10)
        Wtot += 110 + round(random.uniform(-10,10),2)
        Wtot = round(Wtot,2)
        wtotfinal = start_timestamp.strftime("%Y-%m-%d %H:%M:%S") + f" | {Wtot}"
        client_water.publish("IoT/water/Wtot", wtotfinal,qos=1)
        client_water.loop(2,10)
        movements = random.sample(range(0,96), 5)
    if (counter%20==0 and counter != 0):
        W12days = random.uniform(0,1)
        W12days = round(W12days, 2)
        late1 = start_timestamp - datetime.timedelta(days = 2)
        twodayslate = late1.strftime("%Y-%m-%d %H:%M:%S") + f" | {W12days}"
        client_water.publish("IoT/water/W1", twodayslate,qos=1)
        client_water.loop(2,10)
    if (counter % 120 == 0 and counter != 0):
        W112days = random.uniform(0,1)
        W112days = round(W112days, 2)
        late2 = start_timestamp - datetime.timedelta(days = 10)
        tendayslate = late2.strftime("%Y-%m-%d %H:%M:%S") + f" | {W112days}" + "f"
        client_water.publish("IoT/water/W1", tendayslate,qos=1)
        client_water.loop(2,10)
    if (counter%96) in movements:
        move = 1
    data_energy = [HVAC1,HVAC2,MIAC1,MIAC2]
    data_temp = [th1,th2,move]
    data_water = W1
    for i, topic in enumerate(topics_energy):
        if(i<4):
            datafinal = start_timestamp.strftime("%Y-%m-%d %H:%M:%S") + f" | {data_energy[i]}"
            client.publish(topic,datafinal,qos=1)
            client.loop(2,10)
    for i, topic in enumerate(topics_temp):
        datafinal = start_timestamp.strftime("%Y-%m-%d %H:%M:%S") + f" | {data_temp[i]}"
        client_temp.publish(topic,datafinal,qos=1)
        client_temp.loop(2,10)
    datafinal = start_timestamp.strftime("%Y-%m-%d %H:%M:%S") + f" | {data_water}"
    client_water.publish("IoT/water/W1",datafinal,qos=1)
    client_water.loop(2,10)
    time.sleep(1)
    counter += 1
    start_timestamp = start_timestamp + datetime.timedelta(minutes = 15)
