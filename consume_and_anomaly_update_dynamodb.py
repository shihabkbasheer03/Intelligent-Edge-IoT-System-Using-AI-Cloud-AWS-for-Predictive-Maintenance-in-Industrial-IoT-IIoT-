import boto3, time, json
from decimal import Decimal
from datetime import datetime, timezone

REGION="eu-north-1"
STREAM="IoT_Sensor_Stream"
SHARD="shardId-000000000002"
TABLE="IoT_Sensor_anamoly"

kinesis = boto3.client("kinesis", region_name=REGION)
ddb = boto3.resource("dynamodb", region_name=REGION).Table(TABLE)

# ---- TEMP thresholds (we will tune after we see min/max) ----
TH = {
    "mpu_vib_high": Decimal("0.050"),   # start lower so you see anomalies easily
    "mpu_health_low": Decimal("80"),

    "temp_low": Decimal("10"),
    "temp_high": Decimal("70"),

    "current_low": Decimal("0.20"),
    "current_high": Decimal("15.0"),

    "sound_rms_high": Decimal("0.50"),
    "sound_db_high": Decimal("80"),
}

stats = {}  # sensor -> metric -> {min,max}

def now():
    return datetime.now(timezone.utc).isoformat()

def dec(x):
    if x is None: return None
    if isinstance(x, Decimal): return x
    try: return Decimal(str(x))
    except: return None

def upd(sensor, metric, val):
    if val is None: return
    stats.setdefault(sensor, {}).setdefault(metric, {"min": val, "max": val})
    s = stats[sensor][metric]
    if val < s["min"]: s["min"] = val
    if val > s["max"]: s["max"] = val

def parse(b):
    return json.loads(b.decode("utf-8"), parse_float=Decimal)

def detect(r):
    sensor = str(r.get("sensor","")).upper()
    data = r.get("data") if isinstance(r.get("data"), dict) else {}

    # MPU6050
    if sensor=="MPU6050":
        vib = dec(data.get("vibration_rms_g"))
        health = dec(data.get("health_score"))
        rpm = dec(r.get("rpm"))
        fault = str(data.get("fault_state","normal"))

        upd("MPU6050","vibration_rms_g", vib)
        upd("MPU6050","health_score", health)
        upd("MPU6050","rpm", rpm)

        if vib is not None and vib >= TH["mpu_vib_high"]:
            return True, "MPU_HIGH_VIB", f"vib={vib} >= {TH['mpu_vib_high']}", {"vibration_rms_g": vib, "health_score": health, "rpm": rpm, "fault_state": fault}
        if health is not None and health <= TH["mpu_health_low"]:
            return True, "MPU_LOW_HEALTH", f"health={health} <= {TH['mpu_health_low']}", {"vibration_rms_g": vib, "health_score": health, "rpm": rpm, "fault_state": fault}
        return False, "NORMAL", "ok", {}

    # DS18B20
    if sensor=="DS18B20":
        temp = dec(r.get("temperature_c")) or dec(data.get("temperature_c")) or dec(r.get("value")) or dec(data.get("value"))
        upd("DS18B20","temperature_c", temp)
        if temp is not None and (temp < TH["temp_low"] or temp > TH["temp_high"]):
            return True, "TEMP_ANOMALY", f"temp={temp}", {"temperature_c": temp}
        return False, "NORMAL", "ok", {}

    # SCT-013
    if sensor in ("SCT-013","SCT013"):
        cur = dec(r.get("current_a")) or dec(data.get("current_a")) or dec(r.get("value")) or dec(data.get("value")) or dec(data.get("amps"))
        upd("SCT-013","current_a", cur)
        if cur is not None and (cur < TH["current_low"] or cur > TH["current_high"]):
            return True, "CURRENT_ANOMALY", f"current={cur}", {"current_a": cur}
        return False, "NORMAL", "ok", {}

    # INMP441
    if sensor=="INMP441":
        rms = dec(r.get("rms")) or dec(data.get("rms")) or dec(r.get("value")) or dec(data.get("value"))
        db = dec(r.get("sound_db")) or dec(data.get("sound_db")) or dec(r.get("noise_db")) or dec(data.get("noise_db"))
        upd("INMP441","sound_rms", rms)
        upd("INMP441","sound_db", db)

        if db is not None and db >= TH["sound_db_high"]:
            return True, "SOUND_DB_ANOMALY", f"sound_db={db}", {"sound_db": db, "sound_rms": rms}
        if rms is not None and rms >= TH["sound_rms_high"]:
            return True, "SOUND_RMS_ANOMALY", f"sound_rms={rms}", {"sound_db": db, "sound_rms": rms}
        return False, "NORMAL", "ok", {}

    return False, "UNKNOWN", "no_rule", {}

def put(r, atype, reason, metrics):
    item = {
        "device_id": str(r.get("device_id","unknown_device")),
        "timestamp_utc": str(r.get("timestamp_utc", now())),
        "sensor": str(r.get("sensor","")),
        "anomaly_type": atype,
        "anomaly_reason": reason,
        "payload": r,
    }
    item.update({k:v for k,v in metrics.items() if v is not None})
    ddb.put_item(Item=item)

it = kinesis.get_shard_iterator(StreamName=STREAM, ShardId=SHARD, ShardIteratorType="LATEST")["ShardIterator"]

last_stats_print = time.time()

print("Running... will print min/max every 15s and insert anomalies when detected.")
while True:
    out = kinesis.get_records(ShardIterator=it, Limit=100)
    it = out["NextShardIterator"]

    for rec in out.get("Records", []):
        r = parse(rec["Data"])
        is_anom, atype, reason, metrics = detect(r)
        if is_anom:
            put(r, atype, reason, metrics)
            print("✅ ANOMALY SAVED:", r.get("device_id"), r.get("sensor"), atype, reason)

    # print min/max every 15 seconds
    if time.time() - last_stats_print >= 15:
        print("\n--- LIVE MIN/MAX ---")
        for s, m in stats.items():
            print(s, {k: {"min": str(v["min"]), "max": str(v["max"])} for k,v in m.items()})
        print("--------------------\n")
        last_stats_print = time.time()

    time.sleep(0.2)
