import paho.mqtt.client as mqtt
from pykafka import KafkaClient
import time, datetime, random
import json
import os

topics_energy = ["IoT/energy/Airconditioning/HVAC1","IoT/energy/Airconditioning/HVAC2","IoT/energy/Rest_appliances/MiAC1","IoT/energy/Rest_appliances/MiAC2","IoT/energy/Etot"]
topics_water = ["IoT/water/W1","IoT/water/Wtot"]
topics_temp = ["IoT/temperature/TH1","IoT/temperature/TH2","IoT/movement/Mov1"]

# Callback on receive. If message is received from any Energy topic of MQTT it is passed on raw energy Kafka topic
def on_message(client, userdata, message):
    msg_payload = str(message.payload.decode("utf-8"))
    print("message received " , msg_payload)
    msg_topic = str(message.topic)
    find_topic = msg_topic.split("/")
    find_topic = find_topic[-1:][0]
    final1 = msg_payload.split(" | ")
    print(final1)
    to_send = {}
    to_send["Sensor"] = find_topic
    to_send["DateTime"] = final1[0]
    to_send["value"] = float(final1[1])
    final_payload = json.dumps(to_send)
    kafka_producer1.produce(final_payload.encode('ascii'))

# Callback on receive. If message is received from any Water topic of MQTT it is passed on raw water Kafka topic
def on_message1(client, userdata, message):
    msg_payload = str(message.payload.decode("utf-8"))
    print("message received " , msg_payload)
    msg_topic = str(message.topic)
    find_topic = msg_topic.split("/")
    find_topic = find_topic[-1:][0]
    final1 = msg_payload.split(" | ")
    print(final1)
    to_send = {}
    to_send["Sensor"] = find_topic
    to_send["DateTime"] = final1[0]
    try:
        to_send["value"] = float(final1[1])
    except:
        return 
    final_payload = json.dumps(to_send)
    kafka_producer2.produce(final_payload.encode('ascii'))

# Callback on receive. If message is received from any temperature topic of MQTT it is passed on raw temperature Kafka topic
def on_message2(client, userdata, message):
    msg_payload = str(message.payload.decode("utf-8"))
    print("message received " , msg_payload)
    msg_topic = str(message.topic)
    find_topic = msg_topic.split("/")
    find_topic = find_topic[-1:][0]
    final1 = msg_payload.split(" | ")
    print(final1)
    to_send = {}
    to_send["Sensor"] = find_topic
    to_send["DateTime"] = final1[0]
    to_send["value"] = float(final1[1])
    final_payload = json.dumps(to_send)
    kafka_producer3.produce(final_payload.encode('ascii'))

# MQTT clients for fetching the measurements

client_energy = mqtt.Client("client1")
client_energy.on_message=on_message 
client_energy.connect("127.0.0.1")

client_water = mqtt.Client("client2")
client_water.on_message=on_message1 
client_water.connect("127.0.0.1",port=1884)

client_temp = mqtt.Client("client3")
client_temp.on_message=on_message2 
client_temp.connect("127.0.0.1", port=1885)

# Kafka clients for redirecting the raw measurements to Kafka topics

kafka_client = KafkaClient(hosts="localhost:9092")
kafka_energy = kafka_client.topics["raw_energy"]
kafka_producer1 = kafka_energy.get_sync_producer()

kafka_water = kafka_client.topics["raw_water"]
kafka_producer2 = kafka_water.get_sync_producer()

kafka_temp = kafka_client.topics["raw_temp"]
kafka_producer3 = kafka_temp.get_sync_producer()

for topic in topics_energy:
    client_energy.subscribe(topic,qos=1)
for topic in topics_water:
    client_water.subscribe(topic,qos=1)
for topic in topics_temp:
    client_temp.subscribe(topic,qos=1)
client_energy.loop_start()
client_water.loop_start()
client_temp.loop_forever()