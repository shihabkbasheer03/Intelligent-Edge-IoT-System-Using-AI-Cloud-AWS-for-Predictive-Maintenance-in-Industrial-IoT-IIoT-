import boto3
import time
import json
from decimal import Decimal
from datetime import datetime, timezone

# ---------- AWS ----------
REGION = "eu-north-1"
KINESIS_STREAM = "IoT_Sensor_Stream"
SHARD_ID = "shardId-000000000002"
DDB_TABLE = "IoT_Sensor_anamoly"

kinesis = boto3.client("kinesis", region_name=REGION)
dynamodb = boto3.resource("dynamodb", region_name=REGION)
table = dynamodb.Table(DDB_TABLE)

# ---------- THRESHOLDS (tune as per your project) ----------
TH = {
    # MPU6050
    "VIB_RMS_G_HIGH": Decimal("0.055"),     # anomaly if >= 0.055g
    "HEALTH_SCORE_LOW": Decimal("78"),      # anomaly if <= 78

    # DS18B20 temperature (°C)
    "TEMP_C_LOW": Decimal("10"),
    "TEMP_C_HIGH": Decimal("70"),

    # SCT-013 current (A)
    "CURRENT_A_LOW": Decimal("0.20"),
    "CURRENT_A_HIGH": Decimal("15.0"),

    # INMP441 sound (if using RMS 0-1 scale or similar)
    "SOUND_RMS_HIGH": Decimal("0.60"),

    # If you store sound in dB instead, use this too
    "SOUND_DB_HIGH": Decimal("85"),
}

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

def parse_record(data_bytes: bytes) -> dict:
    return json.loads(data_bytes.decode("utf-8"), parse_float=Decimal)

def get_data_dict(r: dict) -> dict:
    d = r.get("data")
    return d if isinstance(d, dict) else {}

def get_timestamp(r: dict) -> str:
    ts = r.get("timestamp_utc")
    return str(ts) if ts else now_utc_iso()

def get_device_id(r: dict) -> str:
    return str(r.get("device_id", "unknown_device"))

# ----------------- ANOMALY DETECTION (ALL SENSORS) -----------------
def detect_anomaly(r: dict):
    """
    Returns:
      (is_anomaly: bool, anomaly_type: str, reason: str, metrics: dict)
    """
    sensor = str(r.get("sensor", "")).upper()
    datatype = str(r.get("datatype", "")).lower()
    data = get_data_dict(r)

    # ---- MPU6050 ----
    if sensor == "MPU6050":
        vib = to_decimal(data.get("vibration_rms_g"))
        health = to_decimal(data.get("health_score"))
        fault_state = str(data.get("fault_state", "normal"))

        metrics = {
            "sensor": sensor,
            "rpm": to_decimal(r.get("rpm")),
            "vibration_rms_g": vib,
            "health_score": health,
            "fault_state": fault_state,
        }

        if vib is not None and vib >= TH["VIB_RMS_G_HIGH"]:
            return True, "MPU6050_HIGH_VIBRATION", f"vibration_rms_g={vib} >= {TH['VIB_RMS_G_HIGH']}", metrics

        if health is not None and health <= TH["HEALTH_SCORE_LOW"]:
            return True, "MPU6050_LOW_HEALTH", f"health_score={health} <= {TH['HEALTH_SCORE_LOW']}", metrics

        # NOTE: we DO NOT mark anomaly only by fault_state label (prevents too many rows)
        return False, "NORMAL", "within_threshold", metrics

    # ---- DS18B20 ----
    if sensor == "DS18B20" or "ds18b20" in datatype or "temp" in datatype:
        temp = (
            to_decimal(r.get("temperature_c"))
            or to_decimal(data.get("temperature_c"))
            or to_decimal(r.get("temp_c"))
            or to_decimal(data.get("temp_c"))
            or to_decimal(r.get("value"))
            or to_decimal(data.get("value"))
        )

        metrics = {"sensor": "DS18B20", "temperature_c": temp}

        if temp is not None and temp < TH["TEMP_C_LOW"]:
            return True, "DS18B20_LOW_TEMP", f"temperature_c={temp} < {TH['TEMP_C_LOW']}", metrics
        if temp is not None and temp > TH["TEMP_C_HIGH"]:
            return True, "DS18B20_HIGH_TEMP", f"temperature_c={temp} > {TH['TEMP_C_HIGH']}", metrics

        return False, "NORMAL", "within_threshold", metrics

    # ---- SCT-013 ----
    if sensor in ("SCT-013", "SCT013") or "current" in datatype or "amps" in datatype:
        cur = (
            to_decimal(r.get("current_a"))
            or to_decimal(data.get("current_a"))
            or to_decimal(data.get("current"))
            or to_decimal(data.get("amps"))
            or to_decimal(r.get("value"))
            or to_decimal(data.get("value"))
        )

        metrics = {"sensor": "SCT-013", "current_a": cur}

        if cur is not None and cur < TH["CURRENT_A_LOW"]:
            return True, "SCT013_LOW_CURRENT", f"current_a={cur} < {TH['CURRENT_A_LOW']}", metrics
        if cur is not None and cur > TH["CURRENT_A_HIGH"]:
            return True, "SCT013_HIGH_CURRENT", f"current_a={cur} > {TH['CURRENT_A_HIGH']}", metrics

        return False, "NORMAL", "within_threshold", metrics

    # ---- INMP441 ----
    if sensor == "INMP441" or "sound" in datatype or "noise" in datatype:
        sound_rms = (
            to_decimal(r.get("sound_rms"))
            or to_decimal(data.get("sound_rms"))
            or to_decimal(r.get("rms"))
            or to_decimal(data.get("rms"))
            or to_decimal(r.get("value"))
            or to_decimal(data.get("value"))
        )
        sound_db = (
            to_decimal(r.get("sound_db"))
            or to_decimal(data.get("sound_db"))
            or to_decimal(r.get("noise_db"))
            or to_decimal(data.get("noise_db"))
        )

        metrics = {"sensor": "INMP441", "sound_rms": sound_rms, "sound_db": sound_db}

        if sound_db is not None and sound_db >= TH["SOUND_DB_HIGH"]:
            return True, "INMP441_HIGH_DB", f"sound_db={sound_db} >= {TH['SOUND_DB_HIGH']}", metrics

        if sound_rms is not None and sound_rms >= TH["SOUND_RMS_HIGH"]:
            return True, "INMP441_HIGH_RMS", f"sound_rms={sound_rms} >= {TH['SOUND_RMS_HIGH']}", metrics

        return False, "NORMAL", "within_threshold", metrics

    return False, "UNKNOWN_SENSOR", "no_rules_for_sensor", {"sensor": sensor}

# ----------------- DYNAMODB ITEM -----------------
def build_item(r: dict, anomaly_type: str, reason: str, metrics: dict) -> dict:
    # Keys
    device_id = get_device_id(r)
    timestamp_utc = get_timestamp(r)

    item = {
        "device_id": device_id,
        "timestamp_utc": timestamp_utc,
        "sensor": metrics.get("sensor", str(r.get("sensor", ""))),
        "anomaly_type": anomaly_type,
        "anomaly_reason": reason,
        "payload": r,
    }

    # Add flattened metrics for easy filtering
    for k, v in metrics.items():
        if v is not None:
            item[k] = v

    return item

# ----------------- MAIN LOOP -----------------
shard_iterator = kinesis.get_shard_iterator(
    StreamName=KINESIS_STREAM,
    ShardId=SHARD_ID,
    ShardIteratorType="LATEST",
)["ShardIterator"]

print(f"Listening: stream={KINESIS_STREAM}, shard={SHARD_ID}, region={REGION}")
print(f"Writing ONLY anomalies to DynamoDB table: {DDB_TABLE}")

while True:
    resp = kinesis.get_records(ShardIterator=shard_iterator, Limit=100)
    shard_iterator = resp["NextShardIterator"]

    for rec in resp.get("Records", []):
        readings = parse_record(rec["Data"])
        is_anom, anom_type, reason, metrics = detect_anomaly(readings)

        # Print for debug (optional)
        print(readings)

        if is_anom:
            item = build_item(readings, anom_type, reason, metrics)
            try:
                out = table.put_item(Item=item)
                code = out.get("ResponseMetadata", {}).get("HTTPStatusCode")
                print(f"✅ ANOMALY SAVED (HTTP {code}): {item['device_id']} {item['sensor']} {anom_type} | {reason}")
            except Exception as e:
                print("❌ DynamoDB put_item failed:", repr(e))

    time.sleep(0.2)
