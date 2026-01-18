#!/usr/bin/env python3
"""
AWS IoT Core Sensor Simulator (MPU6050, DS18B20, SCT-013, INMP441)

Publishes JSON telemetry to AWS IoT Core using AWSIoTPythonSDK, similar to your BedSideMonitor example.

Notes:
- INMP441 publishes audio FEATURES (sound_db, rms_amp, hf_energy_ratio), NOT raw audio stream.
"""

from AWSIoTPythonSDK.MQTTLib import AWSIoTMQTTClient
from AWSIoTPythonSDK.exception.AWSIoTExceptions import publishTimeoutException
from AWSIoTPythonSDK.core.protocol.internal.defaults import DEFAULT_OPERATION_TIMEOUT_SEC

import argparse
import datetime
import json
import logging
import math
import random
import time
import sched

AllowedActions = ["both", "publish", "subscribe"]


# ----------------------------
# MQTT callback (optional)
# ----------------------------
def customCallback(client, userdata, message):
    print("Received a new message:")
    try:
        print(message.payload.decode("utf-8"))
    except Exception:
        print(message.payload)
    print("from topic:", message.topic)
    print("--------------\n")


# ----------------------------
# Helpers
# ----------------------------
def clamp(x, lo, hi):
    return max(lo, min(hi, x))


def rms(vals):
    return math.sqrt(sum(v * v for v in vals) / max(len(vals), 1))


def iso_now():
    return datetime.datetime.now(datetime.timezone.utc).isoformat()


# ----------------------------
# Sensor Simulation
# ----------------------------
def simulate_mpu6050(fault_mode: str, rpm: float, t_sec: float):
    """
    Returns MPU6050-like features:
    - ax_g, ay_g, az_g (approx)
    - vibration_rms_g
    - health_score (0..100)
    """
    f = rpm / 60.0  # Hz
    ax = random.gauss(0, 0.02)
    ay = random.gauss(0, 0.02)
    az = 1.0 + random.gauss(0, 0.02)

    # Add vibration patterns based on fault
    if fault_mode == "bearing_wear":
        ax += 0.06 * math.sin(2 * math.pi * 8 * f * t_sec)
        ay += 0.06 * math.cos(2 * math.pi * 8 * f * t_sec)
    elif fault_mode == "misalignment":
        ax += 0.05 * math.sin(2 * math.pi * 2 * f * t_sec)
        ay += 0.05 * math.cos(2 * math.pi * 2 * f * t_sec)
    else:  # normal
        ax += 0.03 * math.sin(2 * math.pi * f * t_sec)
        ay += 0.03 * math.cos(2 * math.pi * f * t_sec)

    vib_rms = rms([ax, ay, az - 1.0])
    health = clamp(100 - vib_rms * 400, 0, 100)

    return {
        "ax_g": round(ax, 5),
        "ay_g": round(ay, 5),
        "az_g": round(az, 5),
        "vibration_rms_g": round(vib_rms, 5),
        "health_score": round(health, 2),
        "fault_state": fault_mode,
    }


def simulate_temperature(temp_c: float, fault: str):
    """
    DS18B20 temperature model.
    """
    # normal operating band
    normal_min = 28.0
    normal_max = 45.0
    rise_rate = 0.05  # C/sec-ish (depending on publish interval)

    if fault == "overheating":
        temp_c += rise_rate * 3.0
    elif fault == "cooling_failure":
        temp_c += rise_rate * 1.5
    else:
        target = random.uniform(normal_min, normal_max)
        temp_c += (target - temp_c) * 0.05

    return round(temp_c, 2)


def simulate_current(rpm: float, fault: str):
    """
    SCT-013 current clamp model.
    """
    rated_current_a = 8.0
    overload_current_a = 13.0
    noise_a = 0.25

    base = rated_current_a * (rpm / 1450.0)

    if fault == "overload":
        current = overload_current_a
    elif fault == "imbalance":
        current = base * random.uniform(1.2, 1.5)
    else:
        current = base

    current += random.uniform(-noise_a, noise_a)
    return round(max(current, 0), 2)


def simulate_sound(vib_health: float, fault: str):
    """
    INMP441 simulation: publish acoustic FEATURES (not raw audio)
    - sound_db
    - rms_amp (0..1)
    - hf_energy_ratio (0..1)
    """
    baseline_db = 55.0
    noise_db = 2.0

    bearing_db_boost = 8.0
    grinding_db_boost = 12.0

    hf_ratio_normal = 0.35
    hf_ratio_bearing = 0.65
    hf_ratio_grinding = 0.80

    base_db = baseline_db + random.uniform(-noise_db, noise_db)

    # correlate: if vib health is low, noise tends to go up
    if vib_health < 80:
        base_db += (80 - vib_health) * 0.15

    if fault == "bearing_noise":
        sound_db = base_db + bearing_db_boost
        hf_ratio = random.uniform(hf_ratio_bearing - 0.05, hf_ratio_bearing + 0.05)
    elif fault == "grinding":
        sound_db = base_db + grinding_db_boost
        hf_ratio = random.uniform(hf_ratio_grinding - 0.05, hf_ratio_grinding + 0.05)
    else:
        sound_db = base_db
        hf_ratio = random.uniform(hf_ratio_normal - 0.05, hf_ratio_normal + 0.05)

    rms_amp = clamp((sound_db - 40) / 60, 0, 1)

    return {
        "sound_db": round(sound_db, 2),
        "rms_amp": round(rms_amp, 3),
        "hf_energy_ratio": round(clamp(hf_ratio, 0, 1), 3),
        "fault_state": fault,
    }


# ----------------------------
# Publishing function
# ----------------------------
def publishSensorTelemetry(loopCount):
    global device_state

    try:
        now_ts = iso_now()
        elapsed = time.time() - device_state["start_time"]

        # optional rpm drift
        base_rpm = device_state["rpm_base"]
        drift = base_rpm * (device_state["rpm_drift_percent"] / 100.0) * math.sin(
            2 * math.pi * elapsed / device_state["rpm_drift_period_sec"]
        )
        jitter = random.uniform(-device_state["rpm_jitter"], device_state["rpm_jitter"])
        rpm_now = max(0.0, base_rpm + drift + jitter)

        # Publish MPU6050 every 1 sec
        if loopCount % device_state["freq_mpu"] == 0:
            mpu = simulate_mpu6050(device_state["mpu_fault"], rpm_now, elapsed)
            payload = {
                "timestamp_utc": now_ts,
                "device_id": device_state["device_id"],
                "sensor": "MPU6050",
                "rpm": round(rpm_now, 2),
                "data": mpu,
            }
            myAWSIoTMQTTClient.publish(topic, json.dumps(payload), 1)
            if args.mode == "publish":
                print(f"[PUB] {topic} MPU6050 -> {payload['data']}")

        # Publish DS18B20 every 15 sec
        if loopCount % device_state["freq_temp"] == 0:
            device_state["temp_c"] = simulate_temperature(device_state["temp_c"], device_state["temp_fault"])
            payload = {
                "timestamp_utc": now_ts,
                "device_id": device_state["device_id"],
                "sensor": "DS18B20",
                "rpm": round(rpm_now, 2),
                "data": {
                    "temperature_c": device_state["temp_c"],
                    "fault_state": device_state["temp_fault"],
                },
            }
            myAWSIoTMQTTClient.publish(topic, json.dumps(payload), 1)
            if args.mode == "publish":
                print(f"[PUB] {topic} DS18B20 -> {payload['data']}")

        # Publish SCT-013 every 10 sec
        if loopCount % device_state["freq_current"] == 0:
            current_a = simulate_current(rpm_now, device_state["current_fault"])
            payload = {
                "timestamp_utc": now_ts,
                "device_id": device_state["device_id"],
                "sensor": "SCT-013",
                "rpm": round(rpm_now, 2),
                "data": {
                    "current_a": current_a,
                    "fault_state": device_state["current_fault"],
                },
            }
            myAWSIoTMQTTClient.publish(topic, json.dumps(payload), 1)
            if args.mode == "publish":
                print(f"[PUB] {topic} SCT-013 -> {payload['data']}")

        # Publish INMP441 every 5 sec
        if loopCount % device_state["freq_sound"] == 0:
            # correlate sound with last mpu health by simulating again quickly:
            mpu_for_health = simulate_mpu6050(device_state["mpu_fault"], rpm_now, elapsed)
            sound = simulate_sound(mpu_for_health["health_score"], device_state["sound_fault"])
            payload = {
                "timestamp_utc": now_ts,
                "device_id": device_state["device_id"],
                "sensor": "INMP441",
                "rpm": round(rpm_now, 2),
                "data": sound,
            }
            myAWSIoTMQTTClient.publish(topic, json.dumps(payload), 1)
            if args.mode == "publish":
                print(f"[PUB] {topic} INMP441 -> {payload['data']}")

    except publishTimeoutException:
        print(
            "Unstable connection detected. Wait for {} seconds. No data pushed from {} to {}.".format(
                DEFAULT_OPERATION_TIMEOUT_SEC,
                (datetime.datetime.now() - datetime.timedelta(seconds=DEFAULT_OPERATION_TIMEOUT_SEC)),
                datetime.datetime.now(),
            )
        )


# ----------------------------
# Args (same style as your code)
# ----------------------------
parser = argparse.ArgumentParser()
parser.add_argument("-e", "--endpoint", action="store", required=True, dest="host", help="AWS IoT custom endpoint")
parser.add_argument("-r", "--rootCA", action="store", required=True, dest="rootCAPath", help="Root CA file path")
parser.add_argument("-c", "--cert", action="store", dest="certificatePath", help="Certificate file path")
parser.add_argument("-k", "--key", action="store", dest="privateKeyPath", help="Private key file path")
parser.add_argument("-p", "--port", action="store", dest="port", type=int, help="Port override")
parser.add_argument("-w", "--websocket", action="store_true", dest="useWebsocket", default=False, help="MQTT over WebSocket")
parser.add_argument("-id", "--clientId", action="store", dest="clientId", default="sensorSimulator", help="Client ID")
parser.add_argument("-t", "--topic", action="store", dest="topic", default="factory/sim/telemetry", help="Topic")
parser.add_argument("-m", "--mode", action="store", dest="mode", default="both", help=f"Modes: {AllowedActions}")

# Simulator tuning
parser.add_argument("--device", default="EDGE_D001", help="Device ID")
parser.add_argument("--rpm", type=float, default=1450.0, help="Base RPM")
parser.add_argument("--mpu_fault", default="normal", help="MPU fault: normal|bearing_wear|misalignment")
parser.add_argument("--temp_fault", default="normal", help="Temp fault: normal|overheating|cooling_failure")
parser.add_argument("--current_fault", default="normal", help="Current fault: normal|overload|imbalance")
parser.add_argument("--sound_fault", default="normal", help="Sound fault: normal|bearing_noise|grinding")

args = parser.parse_args()

host = args.host
rootCAPath = args.rootCAPath
certificatePath = args.certificatePath
privateKeyPath = args.privateKeyPath
port = args.port
useWebsocket = args.useWebsocket
clientId = args.clientId
topic = args.topic

if args.mode not in AllowedActions:
    parser.error(f"Unknown --mode {args.mode}. Must be one of {AllowedActions}")
    raise SystemExit(2)

if args.useWebsocket and args.certificatePath and args.privateKeyPath:
    parser.error("X.509 cert auth and WebSocket are mutually exclusive.")
    raise SystemExit(2)

if not args.useWebsocket and (not args.certificatePath or not args.privateKeyPath):
    parser.error("Missing credentials for authentication (cert/key).")
    raise SystemExit(2)

# Port defaults
if args.useWebsocket and not args.port:
    port = 443
if not args.useWebsocket and not args.port:
    port = 8883

# Logging (same style)
logger = logging.getLogger("AWSIoTPythonSDK.core")
logger.setLevel(logging.INFO)  # change to DEBUG if needed
streamHandler = logging.StreamHandler()
formatter = logging.Formatter("%(asctime)s - %(name)s - %(levelname)s - %(message)s")
streamHandler.setFormatter(formatter)
logger.addHandler(streamHandler)

# Init client
if useWebsocket:
    myAWSIoTMQTTClient = AWSIoTMQTTClient(clientId, useWebsocket=True)
    myAWSIoTMQTTClient.configureEndpoint(host, port)
    myAWSIoTMQTTClient.configureCredentials(rootCAPath)
else:
    myAWSIoTMQTTClient = AWSIoTMQTTClient(clientId)
    myAWSIoTMQTTClient.configureEndpoint(host, port)
    myAWSIoTMQTTClient.configureCredentials(rootCAPath, privateKeyPath, certificatePath)

# Connection config
myAWSIoTMQTTClient.configureAutoReconnectBackoffTime(1, 32, 20)
myAWSIoTMQTTClient.configureOfflinePublishQueueing(-1)
myAWSIoTMQTTClient.configureDrainingFrequency(2)
myAWSIoTMQTTClient.configureConnectDisconnectTimeout(10)
myAWSIoTMQTTClient.configureMQTTOperationTimeout(5)

# Connect and (optional) subscribe
myAWSIoTMQTTClient.connect()
if args.mode in ("both", "subscribe"):
    myAWSIoTMQTTClient.subscribe(topic, 1, customCallback)
time.sleep(1)

# Device state
device_state = {
    "device_id": args.device,
    "start_time": time.time(),

    "rpm_base": float(args.rpm),
    "rpm_drift_percent": 2.0,
    "rpm_drift_period_sec": 120.0,
    "rpm_jitter": 5.0,

    "mpu_fault": args.mpu_fault,
    "temp_fault": args.temp_fault,
    "current_fault": args.current_fault,
    "sound_fault": args.sound_fault,

    "temp_c": 30.0,

    # publish frequencies (in seconds)
    "freq_mpu": 1,         # every 1 sec
    "freq_temp": 15,       # every 15 sec
    "freq_current": 10,    # every 10 sec
    "freq_sound": 5,       # every 5 sec
}

# Scheduler loop (same pattern as your sample)
loopCount = 0
scheduler = sched.scheduler(time.time, time.sleep)
now = time.time()

try:
    while True:
        if args.mode in ("both", "publish"):
            scheduler.enterabs(now + loopCount, 1, publishSensorTelemetry, (loopCount,))
            loopCount += 1
            scheduler.run()
except KeyboardInterrupt:
    pass

print("Initiate the connection closing process from AWS.")
myAWSIoTMQTTClient.disconnect()
print("Connection closed.")
