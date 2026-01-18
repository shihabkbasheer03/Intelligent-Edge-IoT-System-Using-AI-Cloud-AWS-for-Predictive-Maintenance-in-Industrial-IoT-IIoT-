import boto3
import time
import json
from decimal import Decimal
from datetime import datetime, timezone

REGION = "eu-north-1"
KINESIS_STREAM = "IoT_Sensor_Stream"
SHARD_ID = "shardId-000000000002"

DDB_TABLE = "IoT_Sensor_anamoly"

# -------------------------
# Thresholds (tune as needed)
# -------------------------
# DS18B20 temperature in °C (example for industrial equipment ambient)
TEMP_MIN_C = Decimal("0")
TEMP_MAX_C = Decimal("80")

# SCT-013 current in Amps (example)
CURRENT_MIN_A = Decimal("0.10")
CURRENT_MAX_A = Decimal("15.0")

# INMP441 sound in dB (or RMS proxy if you map it like dB)
SOUND_MAX_DB = Decimal("80")

# MPU6050
VIB_RMS_ANOMALY_G = Decimal("0.05")
HEALTH_SCORE_MIN = Decimal("80")


kinesis = boto3.client("kinesis", region_name=REGION)
dynamodb = boto3.resource("dynamodb", region_name=REGION)
table = dynamodb.Table(DDB_TABLE)


def now_utc_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


def parse_kinesis_record(data_bytes: bytes) -> dict:
    # Kinesis record Data is bytes -> decode -> json
    # Use Decimal for DynamoDB compatibility
    return json.loads(data_bytes.decode("utf-8"), parse_float=Decimal)


def to_decimal(val):
    """Convert number/string/Decimal safely to Decimal; return None if not possible."""
    if val is None:
        return None
    if isinstance(val, Decimal):
        return val
    try:
        return Decimal(str(val))
    except Exception:
        return None


def get_value(readings: dict):
    """
    Many payloads store value either:
      - top-level: readings["value"]
      - nested: readings["data"]["value"] or sensor-specific keys
    We'll try several common patterns.
    """
    if "value" in readings:
        return readings.get("value")

    data = readings.get("data") or {}
    if isinstance(data, dict):
        if "value" in data:
            return data.get("value")

    return None


def classify_and_check_anomaly(readings: dict):
    """
    Returns: (is_anomaly: bool, reason: str, metrics: dict)

    metrics is a flattened dict saved into DynamoDB for easy filtering.
    """
    sensor = str(readings.get("sensor", "")).upper()
    datatype = str(readings.get("datatype", "")).lower()
    data = readings.get("data") or {}

    metrics = {"sensor": sensor, "datatype": datatype}

    # ---------------- MPU6050 ----------------
    if sensor == "MPU6050":
        vib_rms = to_decimal(data.get("vibration_rms_g"))
        health = to_decimal(data.get("health_score"))
        fault_state = str(data.get("fault_state", "normal")).lower()

        metrics.update({
            "rpm": readings.get("rpm"),
            "vibration_rms_g": vib_rms,
            "health_score": health,
            "fault_state": fault_state,
        })

        if fault_state != "normal":
            return True, f"fault_state={fault_state}", metrics
        if vib_rms is not None and vib_rms > VIB_RMS_ANOMALY_G:
            return True, f"vibration_rms_g>{VIB_RMS_ANOMALY_G}", metrics
        if health is not None and health < HEALTH_SCORE_MIN:
            return True, f"health_score<{HEALTH_SCORE_MIN}", metrics

        return False, "normal", metrics

    # ---------------- DS18B20 ----------------
    if sensor == "DS18B20" or "ds18b20" in datatype or "temp" in datatype:
        # Try common keys
        temp = to_decimal(readings.get("temperature_c")) \
               or to_decimal(data.get("temperature_c")) \
               or to_decimal(readings.get("temp_c")) \
               or to_decimal(data.get("temp_c")) \
               or to_decimal(get_value(readings))

        metrics.update({"temperature_c": temp})

        if temp is not None and (temp < TEMP_MIN_C or temp > TEMP_MAX_C):
            return True, f"temperature_c_out_of_range({TEMP_MIN_C}-{TEMP_MAX_C})", metrics

        return False, "normal", metrics

    # ---------------- SCT-013 ----------------
    if sensor == "SCT-013" or "sct" in datatype or "current" in datatype or "amps" in datatype:
        current = to_decimal(readings.get("current_a")) \
                  or to_decimal(data.get("current_a")) \
                  or to_decimal(readings.get("amps")) \
                  or to_decimal(data.get("amps")) \
                  or to_decimal(get_value(readings))

        metrics.update({"current_a": current})

        if current is not None and (current < CURRENT_MIN_A or current > CURRENT_MAX_A):
            return True, f"current_a_out_of_range({CURRENT_MIN_A}-{CURRENT_MAX_A})", metrics

        return False, "normal", metrics

    # ---------------- INMP441 ----------------
    if sensor == "INMP441" or "inmp" in datatype or "sound" in datatype or "noise" in datatype:
        sound = to_decimal(readings.get("sound_db")) \
                or to_decimal(data.get("sound_db")) \
                or to_decimal(readings.get("noise_db")) \
                or to_decimal(data.get("noise_db")) \
                or to_decimal(readings.get("rms")) \
                or to_decimal(data.get("rms")) \
                or to_decimal(get_value(readings))

        metrics.update({"sound_db": sound})

        if sound is not None and sound > SOUND_MAX_DB:
            return True, f"sound_db>{SOUND_MAX_DB}", metrics

        return False, "normal", metrics

    # Unknown sensor -> no anomaly rule
    return False, "unknown_sensor", metrics


def build_ddb_item(readings: dict, reason: str, metrics: dict) -> dict:
    """
    IMPORTANT: DynamoDB table must accept keys.
    Common design:
      PK: device_id
      SK: timestamp_utc

    This item includes both flattened anomaly fields + full payload.
    """
    device_id = str(readings.get("device_id", "unknown_device"))
    ts = str(readings.get("timestamp_utc", now_utc_iso()))
    sensor = str(readings.get("sensor", "UNKNOWN"))

    return {
        "device_id": device_id,
        "timestamp_utc": ts,
        "sensor": sensor,
        "anomaly_reason": reason,

        # flattened metrics
        **{k: v for k, v in metrics.items() if v is not None},

        # original payload (for audit/debug)
        "payload": readings,
    }


# Create shard iterator
shard_iterator = kinesis.get_shard_iterator(
    StreamName=KINESIS_STREAM,
    ShardId=SHARD_ID,
    ShardIteratorType="LATEST",
)["ShardIterator"]

print(f"Listening: stream={KINESIS_STREAM}, shard={SHARD_ID}, region={REGION} -> DDB={DDB_TABLE}")

while True:
    resp = kinesis.get_records(ShardIterator=shard_iterator, Limit=100)
    shard_iterator = resp["NextShardIterator"]

    for rec in resp.get("Records", []):
        readings = parse_kinesis_record(rec["Data"])
        print(readings)

        is_anom, reason, metrics = classify_and_check_anomaly(readings)
        if is_anom:
            item = build_ddb_item(readings, reason, metrics)
            table.put_item(Item=item)
            print(f"⚠️ Anomaly stored in DynamoDB: sensor={item.get('sensor')} reason={reason}")

    time.sleep(0.2)
