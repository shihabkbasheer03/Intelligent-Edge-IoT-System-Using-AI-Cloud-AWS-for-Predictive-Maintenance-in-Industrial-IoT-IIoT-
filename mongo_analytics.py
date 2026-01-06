from pymongo import ASCENDING, DESCENDING
from pymongo.errors import OperationFailure


def _safe_create_index(col, keys, *, name=None, unique=False):
    """
    Create index only if an equivalent key pattern doesn't already exist.
    Avoids IndexOptionsConflict when index exists with a different name.
    """
    existing = col.index_information()  # dict of {name: {...}}
    wanted_key = [(k, d) for (k, d) in keys]

    for idx_name, idx_info in existing.items():
        # idx_info["key"] is list of tuples e.g. [('ts_ingested_utc', 1)]
        if idx_info.get("key") == wanted_key:
            # Equivalent index already exists (any name) -> skip
            # If unique mismatch matters, you can check idx_info.get("unique")
            return idx_name

    # Not found -> try to create
    try:
        return col.create_index(keys, name=name, unique=unique)
    except OperationFailure as e:
        # If conflict happens anyway (race/another job), ignore if it's about existing index
        if e.code in (85, 86) or "IndexOptionsConflict" in str(e):
            return "exists"
        raise


def ensure_indexes(db):
    raw = db["iot-sensors-data"]
    flat = db["iot-sensors-data-flattened"]
    ts = db["iot-sensors-data-timestamped"]
    daily = db["daily-summary"]
    devices = db["iot-devices-list"]

    _safe_create_index(raw, [("device_id", ASCENDING), ("ts_ingested_utc", ASCENDING)], name="raw_device_ingest_idx")
    _safe_create_index(raw, [("ts_ingested_utc", ASCENDING)], name="raw_ingest_idx")

    _safe_create_index(flat, [("device_id", ASCENDING), ("ts_ingested_utc", ASCENDING)], name="flat_device_ingest_idx")

    _safe_create_index(ts, [("device_id", ASCENDING), ("ts_ingested_dt", ASCENDING)], name="ts_device_dt_idx")
    _safe_create_index(ts, [("ts_ingested_dt", ASCENDING)], name="ts_dt_idx")

    _safe_create_index(daily, [("device_id", ASCENDING), ("day", ASCENDING)], name="daily_device_day_uniq", unique=True)
    _safe_create_index(devices, [("device_id", ASCENDING)], name="devices_device_uniq", unique=True)
