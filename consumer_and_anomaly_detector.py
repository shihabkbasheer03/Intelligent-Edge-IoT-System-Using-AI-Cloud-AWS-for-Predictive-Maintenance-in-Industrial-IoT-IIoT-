import boto3
import time
import json

REGION = "eu-north-1"
STREAM_NAME = "IoT_Sensor_Stream"          # your stream name
SHARD_ID = "shardId-000000000000"          # choose 000/001/002 as needed

client = boto3.client("kinesis", region_name=REGION)

shard_iterator = client.get_shard_iterator(
    StreamName=STREAM_NAME,
    ShardId=SHARD_ID,
    ShardIteratorType="LATEST",            # use "TRIM_HORIZON" to read old records
)["ShardIterator"]

def is_anomaly(payload: dict) -> bool:
    """
    Adjust thresholds as per your project needs.
    Expected payload example:
    {
      "device_id": "device_01",
      "sensor": "DS18B20",
      "datatype": "temperature_c",
      "value": 36.5,
      "timestamp_utc": "2026-01-18T09:00:03Z"
    }
    """
    sensor = str(payload.get("sensor", "")).lower()
    datatype = str(payload.get("datatype", "")).lower()

    # value may be string or number
    try:
        value = float(payload.get("value"))
    except Exception:
        return False

    # ---- DS18B20 (Temperature in °C) ----
    if datatype in ("temperature", "temperature_c", "temp_c") or "ds18b20" in sensor:
        # Example thresholds for machine/ambient temperature
        # Change these based on your use-case (motor bearing temp etc.)
        return (value < 0) or (value > 80)

    # ---- SCT-013 (Current in Amps) ----
    if datatype in ("current", "current_a", "amps") or "sct" in sensor:
        # Example: anomaly if current too low/high
        return (value < 0.1) or (value > 15)

    # ---- MPU6050 (Vibration / Acceleration magnitude) ----
    if datatype in ("vibration", "vibration_g", "accel_mag", "accel") or "mpu" in sensor:
        # Example: anomaly if vibration exceeds threshold
        return value > 2.0

    # ---- INMP441 (Sound / noise level) ----
    if datatype in ("sound", "noise", "sound_db", "mic_rms") or "inmp" in sensor:
        # Example: anomaly if sound level too high
        return value > 80

    return False


while True:
    response = client.get_records(ShardIterator=shard_iterator, Limit=100)
    shard_iterator = response["NextShardIterator"]

    for item in response.get("Records", []):
        # Kinesis 'Data' is bytes
        payload_str = item["Data"].decode("utf-8")
        readings = json.loads(payload_str)

        print(readings)

        if is_anomaly(readings):
            print("⚠️ Anomaly detected:", readings.get("datatype"), "value=", readings.get("value"))

    time.sleep(0.2)
