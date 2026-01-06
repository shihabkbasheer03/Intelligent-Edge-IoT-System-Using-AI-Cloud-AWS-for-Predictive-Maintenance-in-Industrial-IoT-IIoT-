import json
from datetime import datetime, timezone
from pymongo import MongoClient, ASCENDING
from pymongo.errors import ServerSelectionTimeoutError, OperationFailure


def load_config():
    with open("configdb.json", "r") as f:
        return json.load(f)


def mongo_uri(cfg):
    return (
        f"mongodb://{cfg['db_user']}:{cfg['db_pass']}"
        f"@{cfg['db_host']}:{cfg['db_port']}"
        f"/?authSource={cfg.get('db_auth_source','admin')}"
    )


def main():
    cfg = load_config()
    uri = mongo_uri(cfg)

    print(f"[INFO] Connecting to MongoDB {cfg['db_host']}:{cfg['db_port']}")

    try:
        client = MongoClient(
            uri,
            connectTimeoutMS=cfg["mongo_connect_timeout_ms"],
            serverSelectionTimeoutMS=cfg["mongo_server_select_timeout_ms"]
        )

        client.admin.command("ping")
        print("[OK] MongoDB connection successful")

        db = client[cfg["db_name"]]
        col = db[cfg["db_collection"]]

        col.create_index(
            [("device_id", ASCENDING), ("ts_utc", ASCENDING)],
            name="device_time_idx"
        )
        col.create_index("ts_ingested_utc", name="ingest_time_idx")

        print("[OK] Indexes created")

        test_doc = {
            "test": True,
            "ts_ingested_utc": datetime.now(timezone.utc).isoformat(),
            "message": "MongoDB init test"
        }

        res = col.insert_one(test_doc)
        print(f"[OK] Test insert successful → {res.inserted_id}")

    except ServerSelectionTimeoutError as e:
        print("[ERR] MongoDB not reachable:", e)
    except OperationFailure as e:
        print("[ERR] MongoDB auth error:", e)
    finally:
        client.close()


if __name__ == "__main__":
    main()
