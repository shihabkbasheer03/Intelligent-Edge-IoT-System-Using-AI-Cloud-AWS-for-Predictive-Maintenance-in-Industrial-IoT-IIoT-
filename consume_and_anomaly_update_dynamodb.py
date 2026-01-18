import boto3
import time
import json
from decimal import Decimal
from datetime import datetime, timezone

REGION = "eu-north-1"
KINESIS_STREAM = "IoT_Sensor_Stream"
SHARD_ID = "shardId-000000000002"
DDB_TABLE = "IoT_Sensor_anamoly"

# Thresholds (adjust to your project)
TEMP_MIN_C = Decimal("0")
TEMP_MAX_C = Decimal("80")

CURRENT_MIN_A = Decimal("0.10")
CURRENT_MAX_A = Decimal("15.0")

SOUND_MAX_DB = Decimal("80")          # if you store sound in dB
SOUND_RMS_MAX = Decimal("0.50")       # if you store RMS (0-1), adjust based on your data

VIB_RMS_ANOMALY_G = Decimal("0.05")
HEALTH_SCORE_MIN = Decimal("80")

kinesis = boto3.client("kinesis", region_name=REGION)
dynamodb = boto3.resource("dynamodb", region_name=REGION)
table = dynamodb.Table(DDB_TABLE)

def now_utc_iso():
    return datetime.now(timezone.utc).isoformat()

def to_decimal(x):
    if x is None:
        return None
    if isinstance(x, Decimal):
        return x
    try:
        return Decimal(str(x))
    except Exception:
        return None

def parse_kinesis(data_bytes: bytes) -> dict:
    return json.loads(data_bytes.decode("utf-8"), parse_float=Decimal)

def get_nested(data: dict, *keys):
    cur = data
    for k in keys:
        if not isinstance(cur, dict) or k not in cur:
            return None
        cur = cur[k]
    return cur

def anomaly_for_record(r: dict):
    """
    Return (is_anomaly, reason, metrics_dict)
    """
    sensor = str(r.get("sensor", "")).upper()
    data = r.get("data") or {}

    # ---------- MPU6050 ----------
    if sensor == "MPU6050":
        vib = to_decimal(data.get("vibration_rms_g"))
        health = to_decimal(data.get("health_score"))
        fault = str(data.get("fault_state", "normal")).lower()

        metrics = {
            "sensor": sensor,
            "rpm": r.get("rpm"),
            "vibration_rms_g": vib,
            "health_score": health,
            "fault_state": fault,
        }

        if fault != "normal":
            return True, f"fault_state={fault}", metrics
        if vib is not None and vib > VIB_RMS_ANOMALY_G:
            return True, f"vibration_rms_g>{VIB_RMS_ANOMALY_G}", metrics
        if health is not None and health < HEALTH_SCORE_MIN:
            return True, f"health_score<{HEALTH_SCORE_MIN}", metrics

        return False, "normal", metrics

    # ---------- DS18B20 ----------
    if sensor == "DS18B20":
        # Support multiple possible field names
        temp = (
            to_decimal(r.get("temperature_c"))
            or to_decimal(data.get("temperature_c"))
            or to_decimal(r.get("temp_c"))
            or to_decimal(data.get("temp_c"))
            or to_decimal(data.get("temperature"))
        )

        metrics = {"sensor": sensor, "temperature_c": temp}

        if temp is not None and (temp < TEMP_MIN_C or temp > TEMP_MAX_C):
            return True, f"temperature_c_out_of_range({TEMP_MIN_C}-{TEMP_MAX_C})", metrics

        return False, "normal", metrics

    # ---------- SCT-013 ----------
    if sensor in ("SCT-013", "SCT013"):
        current = (
            to_decimal(r.get("current_a"))
            or to_decimal(data.get("current_a"))
            or to_decimal(data.get("current"))
            or to_decimal(data.get("amps"))
        )

        metrics = {"sensor": "SCT-013", "current_a": current}

        if current is not None and (current < CURRENT_MIN_A or current > CURRENT_MAX_A):
            return True, f"current_a_out_of_range({CURRENT_MIN_A}-{CURRENT_MAX_A})", metrics

        return False, "normal", metrics

    # ---------- INMP441 ----------
    if sensor == "INMP441":
        # Support either dB or RMS style
        sound_db = (
            to_decimal(r.get("sound_db"))
            or to_decimal(data.get("sound_db"))
            or to_decimal(data.get("noise_db"))
        )
        sound_rms = (
            to_decimal(r.get("rms"))
            or to_decimal(data.get("rms"))
            or to_decimal(data.get("sound_rms"))
        )

        metrics = {"sensor": sensor, "sound_db": sound_db, "sound_rms": sound_rms}

        if sound_db is not None and sound_db > SOUND_MAX_DB:
            return True, f"sound_db>{SOUND_MAX_DB}", metrics

        if sound_rms is not None and sound_rms > SOUND_RMS_MAX:
            return True, f"sound_rms>{SOUND_RMS_MAX}", metrics

        return False, "normal", metrics

    # Unknown sensor
    return False, "unknown_sensor", {"sensor": sensor}


def build_ddb_item(r: dict, reason: str, metrics: dict) -> dict:
    # DynamoDB keys must exist
    device_id = str(r.get("device_id", "unknown_device"))
    ts = str(r.get("timestamp_utc", now_utc_iso()))

    # Ensure timestamp exists (important for RANGE key)
    if not ts:
        ts = now_utc_iso()

    item = {
        "device_id": device_id,
        "timestamp_utc": ts,
        "anomaly_reason": reason,
        "payload": r,
    }

    # Add flattened metrics (no None values)
    for k, v in metrics.items():
        if v is not None:
            item[k] = v

    return item


# ---- Optional: test write to confirm DynamoDB insert works ----
test_item = {
    "device_id": "test_device",
    "timestamp_utc": now_utc_iso(),
    "anomaly_reason": "test_write",
    "sensor": "TEST"
}
print("DynamoDB TEST write:", table.put_item(Item=test_item)["ResponseMetadata"]["HTTPStatusCode"])

# ---- Kinesis iterator ----
it = kinesis.get_shard_iterator(
    StreamName=KINESIS_STREAM,
    ShardId=SHARD_ID,
    ShardIteratorType="LATEST",
)["ShardIterator"]

print(f"Listening: {KINESIS_STREAM} / {SHARD_ID} ({REGION}) -> DynamoDB: {DDB_TABLE}")

while True:
    resp = kinesis.get_records(ShardIterator=it, Limit=100)
    it = resp["NextShardIterator"]

    for rec in resp.get("Records", []):
        readings = parse_kinesis(rec["Data"])
        is_anom, reason, metrics = anomaly_for_record(readings)

        # Print all events to confirm payload structure
        print(readings)

        if is_anom:
            item = build_ddb_item(readings, reason, metrics)
            try:
                out = table.put_item(Item=item)
                code = out.get("ResponseMetadata", {}).get("HTTPStatusCode")
                print(f"✅ Inserted anomaly to DynamoDB (HTTP {code}): {item['device_id']} {item.get('sensor')} {reason}")
            except Exception as e:
                print("❌ DynamoDB insert failed:", repr(e))

    time.sleep(0.2)
