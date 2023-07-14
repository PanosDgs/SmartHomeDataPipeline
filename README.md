# IoT Smart Home Data Pipeline for Storage, Aggregation and Presentation
University project of Big Data subject - 9th semester of NTUA.

Project BK 1.7: IoT Live Streaming System

Team 14: Panagiotis Ntagkas, Andreas Nikolaros, Maria Glarou

## Technologies
Mosquitto MQTT - Apache Spark - (Confluent Kafka) - MongoDB - Grafana

## Project Structure

### Device Layer
Device layer is simulating real measurements of water, energy and temperature sensors.

Written in Python and using Paho MQTT client.

### Messaging Broker Layer
Used Mosquitto MQTT on Windows environment.

Folder MQTTBrokers contains the configuration files for MQTT.

3 different brokers are used in ports 1883 (responsible for Energy measurements), 1884 (responsible for Water measurements) and 1885 (responsible for Temperature measurements).

### Live Streaming Layer
Using Apache Spark 2.4.0 with Hadoop 2.7 (compatible with MQTT).

Aggregation of measurements in daily sums, averages and differences. Water and Energy Leakage as well.

Written in Scala.

### Spark - MongoDB Gateway
Confluent Kafka withins WSL2 is used to group the aggregated and raw data in topics and store them to MongoDB using MongoDB Kafka connectors.

Port mapper script needs to be used to make the Kafka Broker visible to Apache Spark when operating on Windows environment.

In Bridge folder there is a python script for passing the raw data from MQTT brokers to Kafka broker.

In Confluent Kafka folder there are MongoDB connector property files.

Topics are shown in pictures.

### Data Storage Layer
MongoDB Atlas was used to store the aggregated and raw data in the cloud. 

Database folder contains the chosen architecture of the Database and the configuration added to allow connections.

### Presentation Layer
Grafana was used to present the aggregated and raw data.

In Grafana folder there is the exported json file for a dashboard that presents the data in various tables and bar charts.
