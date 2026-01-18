import json
import uuid
import argparse
from datetime import datetime, timezone

import paho.mqtt.client as mqtt
from pymongo import MongoClient
from pymongo.errors import ServerSelectionTimeoutError


def load_config(path):
    with open(path, "r", encoding="utf-8") as f:
        return json.load(f)


def utc_now():
    return datetime.now(timezone.utc).isoformat()


def mongo_uri(cfg):
    required = ["db_host", "db_user", "db_pass"]
    missing = [k for k in required if not cfg.get(k)]
    if missing:
        raise KeyError(f"Missing MongoDB config keys: {missing}")

    return (
        f"mongodb://{cfg['db_user']}:{cfg['db_pass']}"
        f"@{cfg['db_host']}:{cfg.get('db_port',27017)}"
        f"/?authSource={cfg.get('db_auth_source','admin')}"
    )


class MongoSink:
    def __init__(self, cfg):
        self.cfg = cfg
        self.client = None
        self.collection = None

    def connect(self):
        self.client = MongoClient(
            mongo_uri(self.cfg),
            connectTimeoutMS=self.cfg.get("mongo_connect_timeout_ms", 5000),
            serverSelectionTimeoutMS=self.cfg.get("mongo_server_select_timeout_ms", 5000),
        )
        self.client.admin.command("ping")

        db = self.client[self.cfg.get("db_name", "iot-sensors-db")]
        self.collection = db[self.cfg.get("db_collection", "iot-sensors-data")]
        print("[OK] MongoDB collection initialized")

    def insert(self, doc):
        if self.collection is None:
            self.connect()
        self.collection.insert_one(doc)


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--config", default="configdb.json")
    args = parser.parse_args()

    cfg = load_config(args.config)
    print(f"[INFO] Using config file: {args.config}")

    mongo = MongoSink(cfg)

    try:
        mongo.connect()
        print("[OK] MongoDB connected")
    except Exception as e:
        print("[WARN] MongoDB initial connect failed:", e)
        print("[INFO] Will retry on first insert")

    client_id = f"edge-mongo-{uuid.uuid4().hex[:6]}"
    mqtt_client = mqtt.Client(
        client_id=client_id,
        callback_api_version=mqtt.CallbackAPIVersion.VERSION2
    )

    if cfg.get("broker_user"):
        mqtt_client.username_pw_set(
            cfg.get("broker_user"),
            cfg.get("broker_pass")
        )

    topics = cfg.get("subscribe_topics", ["factory/+/telemetry"])
    qos = int(cfg.get("mqtt_qos", 1))   # SAFE DEFAULT

    def on_connect(c, u, f, rc, p):
        if rc == 0:
            print(f"[OK] MQTT connected as {client_id}")
            for t in topics:
                c.subscribe(t, qos=qos)
                print(f"[SUB] {t} (qos={qos})")
        else:
            print(f"[ERR] MQTT connect failed rc={rc}")

    def on_message(c, u, msg):
        try:
            payload = json.loads(msg.payload.decode("utf-8"))
        except Exception as e:
            print("[WARN] Invalid JSON:", e)
            return

        doc = {
            **payload,
            "mqtt_topic": msg.topic,
            "ts_ingested_utc": utc_now(),
            "device_id": payload.get("device_id", "unknown")
        }

        try:
            mongo.insert(doc)
            print(f"[DB] Inserted → device={doc['device_id']}")
        except ServerSelectionTimeoutError as e:
            print("[ERR] MongoDB timeout:", e)
        except Exception as e:
            print("[ERR] MongoDB insert failed:", e)

    mqtt_client.on_connect = on_connect
    mqtt_client.on_message = on_message

    mqtt_client.connect(
        cfg.get("broker_host"),
        int(cfg.get("broker_port", 1883)),
        60
    )
    mqtt_client.loop_forever()


if __name__ == "__main__":
    main()
