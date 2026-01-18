# Intelligent Edge IoT System using AI & Cloud (AWS) for Predictive Maintenance in IIoT

## Overview
This project demonstrates an **Industrial IoT (IIoT)** architecture where sensor data is collected at the **edge**, published via **MQTT**, stored in **MongoDB**, and can be used for **analytics and predictive maintenance** workflows.  
The implementation is designed to be modular so it can be extended to AWS cloud services (IoT Core, storage, analytics, dashboards, etc.).

## Key Features
- Edge-side sensor simulation / integration (DS18B20 – temperature)
- MQTT publish/subscribe workflow
- Data persistence in MongoDB
- Analytics scripts for basic insights and table generation
- Modular structure ready for AWS cloud integration

## Tech Stack
- **Python** (edge scripts, publishers/subscribers, analytics)
- **MQTT** (Mosquitto / broker)
- **MongoDB** (storage + analytics collections)
- **GitHub** (version control)
- *(Optional extension)* AWS IoT Core, S3, Lambda, CloudWatch

## Repository Structure
- `edge/` → Edge scripts (sensors, MQTT publisher/subscriber)
- `database/` → MongoDB configs, schema notes
- `cloud/` → AWS integration plan/scripts (optional)
- `docs/` → Diagrams, screenshots, reports

## How It Works (High Level Flow)
1. Sensor data is collected at the edge (DS18B20) or simulated.
2. Data is published to MQTT topics.
3. Subscriber consumes MQTT messages and writes to MongoDB.
4. Analytics scripts process stored data for monitoring / insights.

## Setup Prerequisites
- Python 3.9+
- MQTT broker (Mosquitto recommended)
- MongoDB (local or remote)

## Configuration
Update your configuration files inside:
`edge/sensors/ds18b20/`

Common fields usually include:
- MQTT broker IP/Hostname and port
- MQTT topic name
- MongoDB connection string, DB name, collection name

## Running the DS18B20 Pipeline (Example)
> Commands depend on your environment and config values.

1) Start MQTT broker (if local):
- Windows: install Mosquitto and run it
- Linux:
```bash
sudo systemctl start mosquitto
