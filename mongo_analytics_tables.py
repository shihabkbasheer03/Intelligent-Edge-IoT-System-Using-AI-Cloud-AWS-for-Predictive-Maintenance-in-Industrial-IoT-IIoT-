import json
from datetime import datetime, timezone

from pymongo import MongoClient, ASCENDING
from pymongo.errors import OperationFailure


# ---------------- Config / Connection ----------------
def load_config(path="configdb.json"):
    with open(path, "r", encoding="utf-8") as f:
        return json.load(f)


def mongo_uri(cfg):
    return (
        f"mongodb://{cfg['db_user']}:{cfg['db_pass']}"
        f"@{cfg['db_host']}:{cfg.get('db_port',27017)}"
        f"/?authSource={cfg.get('db_auth_source','admin')}"
    )


def get_db(cfg):
    client = MongoClient(
        mongo_uri(cfg),
        connectTimeoutMS=int(cfg.get("mongo_connect_timeout_ms", 5000)),
        serverSelectionTimeoutMS=int(cfg.get("mongo_server_select_timeout_ms", 5000)),
    )
    client.admin.command("ping")
    return client, client[cfg.get("db_name", "iot-sensors-db")]


# ---------------- Safe index creation (prevents IndexOptionsConflict) ----------------
def safe_create_index(col, keys, *, name=None, unique=False):
    """
    Create index only if same key-pattern doesn't already exist under any name.
    Avoids: IndexOptionsConflict (code 85)
    """
    existing = col.index_information()
    wanted = [(k, d) for (k, d) in keys]

    for idx_name, info in existing.items():
        if info.get("key") == wanted:
            return idx_name  # already exists

    try:
        return col.create_index(keys, name=name, unique=unique)
    except OperationFailure as e:
        if e.code in (85, 86) or "IndexOptionsConflict" in str(e):
            return "exists"
        raise


def ensure_indexes(db):
    raw = db["iot-sensors-data"]
    flat = db["iot-sensors-data-flattened"]
    ts = db["iot-sensors-data-timestamped"]
    daily = db["daily-summary"]
    devices = db["iot-devices-list"]
    alerts = db["alerts"]

    safe_create_index(raw, [("device_id", ASCENDING), ("ts_ingested_utc", ASCENDING)], name="raw_device_ts_idx")
    safe_create_index(raw, [("ts_ingested_utc", ASCENDING)], name="raw_ingest_idx")

    safe_create_index(flat, [("device_id", ASCENDING), ("ts_ingested_utc", ASCENDING)], name="flat_device_ts_idx")
    safe_create_index(ts, [("device_id", ASCENDING), ("ts_ingested_dt", ASCENDING)], name="ts_device_dt_idx")
    safe_create_index(ts, [("ts_ingested_dt", ASCENDING)], name="ts_dt_idx")

    safe_create_index(daily, [("device_id", ASCENDING), ("day", ASCENDING)], name="daily_device_day_uniq", unique=True)
    safe_create_index(devices, [("device_id", ASCENDING)], name="devices_device_uniq", unique=True)
    safe_create_index(alerts, [("device_id", ASCENDING), ("ts_ingested_dt", ASCENDING)], name="alerts_device_dt_idx")


# ---------------- Pipelines (creates separate "tables") ----------------
def pipeline_flatten(now_iso):
    return [
        {
            "$project": {
                "_id": 0,
                "device_id": 1,
                "mqtt_topic": 1,
                "ts_ingested_utc": 1,
                "ts_utc": 1,
                "rpm": 1,

                "vib_rms_g": "$mpu6050.vibration_rms_g",
                "health_score": "$mpu6050.health_score",
                "vib_fault": "$mpu6050.fault_state",

                "temp_c": "$ds18b20.temperature_c",
                "temp_fault": "$ds18b20.fault_state",

                "current_a": "$sct013.current_a",
                "current_fault": "$sct013.fault_state",

                "sound_db": "$inmp441.sound_db",
                "sound_rms_amp": "$inmp441.rms_amp",
                "sound_hf_ratio": "$inmp441.hf_energy_ratio",
                "sound_fault": "$inmp441.fault_state",

                "analytics_updated_utc": {"$literal": now_iso}
            }
        },
        {
            "$merge": {
                "into": "iot-sensors-data-flattened",
                "whenMatched": "replace",
                "whenNotMatched": "insert"
            }
        }
    ]


def pipeline_timestamp(now_iso):
    return [
        {
            "$addFields": {
                "ts_ingested_dt": {"$dateFromString": {"dateString": "$ts_ingested_utc"}}
            }
        },
        {
            "$addFields": {
                "ts_utc_dt": {
                    "$cond": [
                        {"$ifNull": ["$ts_utc", False]},
                        {"$dateFromString": {"dateString": "$ts_utc"}},
                        None
                    ]
                }
            }
        },
        {
            "$addFields": {
                "analytics_updated_utc": {"$literal": now_iso}
            }
        },
        {
            "$merge": {
                "into": "iot-sensors-data-timestamped",
                "whenMatched": "replace",
                "whenNotMatched": "insert"
            }
        }
    ]


def pipeline_daily_summary(now_iso):
    return [
        {
            "$addFields": {
                "day": {"$dateToString": {"format": "%Y-%m-%d", "date": "$ts_ingested_dt"}}
            }
        },
        {
            "$group": {
                "_id": {"device_id": "$device_id", "day": "$day"},
                "samples": {"$sum": 1},

                "avg_rpm": {"$avg": "$rpm"},
                "min_rpm": {"$min": "$rpm"},
                "max_rpm": {"$max": "$rpm"},

                "avg_temp_c": {"$avg": "$temp_c"},
                "min_temp_c": {"$min": "$temp_c"},
                "max_temp_c": {"$max": "$temp_c"},

                "avg_current_a": {"$avg": "$current_a"},
                "min_current_a": {"$min": "$current_a"},
                "max_current_a": {"$max": "$current_a"},

                "avg_sound_db": {"$avg": "$sound_db"},
                "min_health": {"$min": "$health_score"},
                "avg_health": {"$avg": "$health_score"},

                "vib_fault_count": {"$sum": {"$cond": [{"$ne": ["$vib_fault", "normal"]}, 1, 0]}},
                "temp_fault_count": {"$sum": {"$cond": [{"$ne": ["$temp_fault", "normal"]}, 1, 0]}},
                "current_fault_count": {"$sum": {"$cond": [{"$ne": ["$current_fault", "normal"]}, 1, 0]}},
                "sound_fault_count": {"$sum": {"$cond": [{"$ne": ["$sound_fault", "normal"]}, 1, 0]}}
            }
        },
        {
            "$project": {
                "_id": 0,
                "device_id": "$_id.device_id",
                "day": "$_id.day",
                "samples": 1,

                "avg_rpm": {"$round": ["$avg_rpm", 2]},
                "min_rpm": {"$round": ["$min_rpm", 2]},
                "max_rpm": {"$round": ["$max_rpm", 2]},

                "avg_temp_c": {"$round": ["$avg_temp_c", 2]},
                "min_temp_c": {"$round": ["$min_temp_c", 2]},
                "max_temp_c": {"$round": ["$max_temp_c", 2]},

                "avg_current_a": {"$round": ["$avg_current_a", 2]},
                "min_current_a": {"$round": ["$min_current_a", 2]},
                "max_current_a": {"$round": ["$max_current_a", 2]},

                "avg_sound_db": {"$round": ["$avg_sound_db", 2]},
                "min_health": {"$round": ["$min_health", 2]},
                "avg_health": {"$round": ["$avg_health", 2]},

                "vib_fault_count": 1,
                "temp_fault_count": 1,
                "current_fault_count": 1,
                "sound_fault_count": 1,

                "analytics_updated_utc": {"$literal": now_iso}
            }
        },
        {
            "$merge": {
                "into": "daily-summary",
                "whenMatched": "replace",
                "whenNotMatched": "insert"
            }
        }
    ]


def pipeline_devices_list(now_iso):
    return [
        {"$sort": {"ts_ingested_dt": -1}},
        {
            "$group": {
                "_id": "$device_id",
                "last_seen": {"$first": "$ts_ingested_dt"},
                "rpm": {"$first": "$rpm"},
                "temp_c": {"$first": "$temp_c"},
                "current_a": {"$first": "$current_a"},
                "sound_db": {"$first": "$sound_db"},
                "health_score": {"$first": "$health_score"},
                "vib_fault": {"$first": "$vib_fault"},
                "temp_fault": {"$first": "$temp_fault"},
                "current_fault": {"$first": "$current_fault"},
                "sound_fault": {"$first": "$sound_fault"}
            }
        },
        {
            "$project": {
                "_id": 0,
                "device_id": "$_id",
                "last_seen": 1,
                "rpm": 1,
                "temp_c": 1,
                "current_a": 1,
                "sound_db": 1,
                "health_score": 1,
                "vib_fault": 1,
                "temp_fault": 1,
                "current_fault": 1,
                "sound_fault": 1,
                "analytics_updated_utc": {"$literal": now_iso}
            }
        },
        {
            "$merge": {
                "into": "iot-devices-list",
                "whenMatched": "replace",
                "whenNotMatched": "insert"
            }
        }
    ]


def pipeline_alerts(now_iso):
    """
    Simple anomaly rules for demo.
    """
    return [
        {
            "$match": {
                "$or": [
                    {"health_score": {"$lt": 70}},
                    {"temp_c": {"$gt": 80}},
                    {"current_a": {"$gt": 12}},
                    {"sound_hf_ratio": {"$gt": 0.60}}
                ]
            }
        },
        {
            "$project": {
                "_id": 0,
                "ts_ingested_dt": 1,
                "device_id": 1,
                "rpm": 1,
                "temp_c": 1,
                "current_a": 1,
                "sound_db": 1,
                "sound_hf_ratio": 1,
                "health_score": 1,
                "reason": {
                    "$concat": [
                        "RuleHit:",
                        {"$cond": [{"$lt": ["$health_score", 70]}, " health<70", ""]},
                        {"$cond": [{"$gt": ["$temp_c", 80]}, " temp>80", ""]},
                        {"$cond": [{"$gt": ["$current_a", 12]}, " current>12", ""]},
                        {"$cond": [{"$gt": ["$sound_hf_ratio", 0.6]}, " hfRatio>0.6", ""]}
                    ]
                },
                "analytics_updated_utc": {"$literal": now_iso}
            }
        },
        {
            "$merge": {
                "into": "alerts",
                "whenMatched": "replace",
                "whenNotMatched": "insert"
            }
        }
    ]


# ---------------- Runner ----------------
def run(config_path="configdb.json"):
    cfg = load_config(config_path)
    client, db = get_db(cfg)

    now_iso = datetime.now(timezone.utc).isoformat()
    ensure_indexes(db)

    # Create separate "tables" (collections) like your screenshot
    db["iot-sensors-data"].aggregate(pipeline_flatten(now_iso), allowDiskUse=True)
    db["iot-sensors-data-flattened"].aggregate(pipeline_timestamp(now_iso), allowDiskUse=True)
    db["iot-sensors-data-timestamped"].aggregate(pipeline_daily_summary(now_iso), allowDiskUse=True)
    db["iot-sensors-data-timestamped"].aggregate(pipeline_devices_list(now_iso), allowDiskUse=True)
    db["iot-sensors-data-timestamped"].aggregate(pipeline_alerts(now_iso), allowDiskUse=True)

    print("[OK] Analytics tables refreshed:")
    print(" - iot-sensors-data-flattened")
    print(" - iot-sensors-data-timestamped")
    print(" - daily-summary")
    print(" - iot-devices-list")
    print(" - alerts")

    client.close()


if __name__ == "__main__":
    run("configdb.json")
