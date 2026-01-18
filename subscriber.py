import json
import uuid
import paho.mqtt.client as mqtt


def load_config():
    with open("config.json", "r", encoding="utf-8") as f:
        return json.load(f)


cfg = load_config()

client = mqtt.Client(
    client_id=f"sub-{uuid.uuid4().hex[:6]}",
    callback_api_version=mqtt.CallbackAPIVersion.VERSION2
)
client.username_pw_set(cfg["broker_user"], cfg["broker_pass"])


def on_connect(c, u, f, rc, p):
    print("[OK] Subscriber connected")
    for t in cfg["subscribe_topics"]:
        c.subscribe(t, qos=cfg["qos"])


def on_message(c, u, msg):
    d = json.loads(msg.payload.decode())
    print(
        f"[MSG] {d['device_id']} | "
        f"RPM={d['rpm']} | "
        f"TEMP={d['ds18b20']['temperature_c']}C({d['ds18b20']['fault_state']}) | "
        f"CURR={d['sct013']['current_a']}A({d['sct013']['fault_state']}) | "
        f"SOUND={d['inmp441']['sound_db']}dB({d['inmp441']['fault_state']}) | "
        f"HEALTH={d['mpu6050']['health_score']}"
    )


client.on_connect = on_connect
client.on_message = on_message
client.connect(cfg["broker_host"], cfg["broker_port"], 60)
client.loop_forever()
