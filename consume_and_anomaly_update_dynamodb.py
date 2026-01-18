import boto3
import time
import json
from decimal import Decimal
from datetime import datetime, timezone

REGION = "eu-north-1"
KINESIS_STREAM = "IoT_Sensor_Stream"
SHARD_ID = "shardId-000000000002"

DDB_TABLE = "IoT_Sensor_anamoly"   # your DynamoDB table name

# Thresholds (tune as you like)
VIB_RMS_ANOMALY_G = Decimal("0.05")      # > 0.05g is anomaly (example)
HEALTH_SCORE_MIN = Decimal("80")        # < 80 is anomaly (example)

kinesis = boto3.client("kinesis", region_name=REGION)
dynamodb = boto3.resource("dynamodb", region_name=REGION)
table = dynamodb.Table(DDB_TABLE)


def now_utc_iso():
    return datetime.now(timezone.utc).isoformat()


def parse_kinesis_record(data_bytes: bytes) -> dict:
    # Keep floats as Decimal for DynamoDB
    return json.loads(data_bytes.decode("utf-8"), parse_float=Decimal)


def is_anomaly_mpu(readings: dict) -> tuple[bool, str]:
    """
    Returns (is_anomaly, reason)
    """
    if str(readings.get("sensor", "")).upper() != "MPU6050":
        return False, "not_mpu"

    data = readings.get("data", {}) or {}

    fault_state = str(data.get("fault_state", "normal")).lower()
    vib_rms = data.get("vibration_rms_g")
    health = data.get("health_score")

    # Rule 1: model says not normal
    if fault_state != "normal":
        return True, f"fault_state={fault_state}"

    # Rule 2: vibration above threshold
    if isinstance(vib_rms, Decimal) and vib_rms > VIB_RMS_ANOMALY_G:
        return True, f"vibration_rms_g>{VIB_RMS_ANOMALY_G}"

    # Rule 3: health score below threshold
    if isinstance(health, Decimal) and health < HEALTH_SCORE_MIN:
        return True, f"health_score<{HEALTH_SCORE_MIN}"

    return False, "normal"


def build_ddb_item(readings: dict, reason: str) -> dict:
    """
    DynamoDB needs a primary key. Since we don't know your table key schema,
    we create safe keys that almost always work if table has:
      - partition key: device_id (string)
      - sort key: timestamp_utc (string)
    If your key schema is different, tell me and I’ll adjust.
    """
    device_id = str(readings.get("device_id", "unknown_device"))
    ts = str(readings.get("timestamp_utc", now_utc_iso()))

    # Flatten key anomaly fields for easy querying
    data = readings.get("data", {}) or {}

    return {
        "device_id": device_id,
        "timestamp_utc": ts,
        "sensor": str(readings.get("sensor", "")),
        "rpm": readings.get("rpm", Decimal("0")),
        "anomaly_reason": reason,

        # Keep full original payload too (useful for audit)
        "payload": readings,

        # Optional flattened metrics (handy for dashboards)
        "vibration_rms_g": data.get("vibration_rms_g", Decimal("0")),
        "health_score": data.get("health_score", Decimal("0")),
        "fault_state": str(data.get("fault_state", "unknown")),
    }


# Create iterator
shard_iterator = kinesis.get_shard_iterator(
    StreamName=KINESIS_STREAM,
    ShardId=SHARD_ID,
    ShardIteratorType="LATEST",   # change to TRIM_HORIZON if needed
)["ShardIterator"]

print(f"Listening: stream={KINESIS_STREAM}, shard={SHARD_ID}, ddb={DDB_TABLE}, region={REGION}")

while True:
    resp = kinesis.get_records(ShardIterator=shard_iterator, Limit=100)
    shard_iterator = resp["NextShardIterator"]

    for rec in resp.get("Records", []):
        readings = parse_kinesis_record(rec["Data"])
        print(readings)

        is_anom, reason = is_anomaly_mpu(readings)
        if is_anom:
            item = build_ddb_item(readings, reason)
            table.put_item(Item=item)
            print("⚠️ Anomaly detected -> inserted to DynamoDB:", reason)

    time.sleep(0.2)
