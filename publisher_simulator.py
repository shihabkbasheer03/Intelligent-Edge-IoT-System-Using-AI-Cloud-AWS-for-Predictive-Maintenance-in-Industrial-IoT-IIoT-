import json
import time
import math
import random
import uuid
from datetime import datetime, timezone
import paho.mqtt.client as mqtt


# ---------- Helpers ----------
def utc_iso():
    return datetime.now(timezone.utc).isoformat()


def load_config():
    with open("config.json", "r", encoding="utf-8") as f:
        return json.load(f)


def rms(vals):
    return math.sqrt(sum(v * v for v in vals) / len(vals))


def clamp(x, lo, hi):
    return max(lo, min(hi, x))


# ---------- MPU6050 ----------
def simulate_mpu6050(mode, rpm, t):
    f = rpm / 60.0
    ax = random.gauss(0, 0.02)
    ay = random.gauss(0, 0.02)
    az = 1.0 + random.gauss(0, 0.02)

    if mode == "bearing_wear":
        ax += 0.06 * math.sin(2 * math.pi * 8 * f * t)
        ay += 0.06 * math.cos(2 * math.pi * 8 * f * t)
    else:
        ax += 0.03 * math.sin(2 * math.pi * f * t)
        ay += 0.03 * math.cos(2 * math.pi * f * t)

    vib_rms = rms([ax, ay, az - 1.0])
    health = clamp(100 - vib_rms * 400, 0, 100)

    return {
        "vibration_rms_g": round(vib_rms, 5),
        "health_score": round(health, 2)
    }


# ---------- DS18B20 ----------
def simulate_temperature(state, temp, cfg):
    if state == "overheating":
        temp += cfg["rise_rate_c_per_sec"]
    elif state == "cooling_failure":
        temp += cfg["rise_rate_c_per_sec"] * 0.6
    else:
        target = random.uniform(cfg["normal_min_c"], cfg["normal_max_c"])
        temp += (target - temp) * 0.05
    return round(temp, 2)


# ---------- SCT-013 ----------
def simulate_current(fault, rpm, cfg):
    base = cfg["rated_current_a"] * (rpm / 1450)

    if fault == "overload":
        current = cfg["overload_current_a"]
    elif fault == "imbalance":
        current = base * random.uniform(1.2, 1.5)
    else:
        current = base

    current += random.uniform(-cfg["noise_a"], cfg["noise_a"])
    return round(current, 2)


# ---------- INMP441 (Features, not raw audio) ----------
def simulate_sound(sound_fault, vib_health, cfg):
    """
    Simulates mic acoustic features:
    - sound_db
    - rms_amp (0..1)
    - hf_energy_ratio (0..1): higher when bearing/grinding issues
    """
    base_db = cfg["baseline_db"] + random.uniform(-cfg["noise_db"], cfg["noise_db"])

    # If vibration health is low, slightly increase noise in general
    if vib_health < 80:
        base_db += (80 - vib_health) * 0.15

    if sound_fault == "bearing_noise":
        sound_db = base_db + cfg["bearing_db_boost"]
        hf_ratio = random.uniform(cfg["hf_ratio_bearing"] - 0.05, cfg["hf_ratio_bearing"] + 0.05)

    elif sound_fault == "grinding":
        sound_db = base_db + cfg["grinding_db_boost"]
        hf_ratio = random.uniform(cfg["hf_ratio_grinding"] - 0.05, cfg["hf_ratio_grinding"] + 0.05)

    else:  # normal
        sound_db = base_db
        hf_ratio = random.uniform(cfg["hf_ratio_normal"] - 0.05, cfg["hf_ratio_normal"] + 0.05)

    # Convert dB to a pseudo RMS amplitude (scaled)
    # Keep it bounded 0..1 for easy ML
    rms_amp = clamp((sound_db - 40) / 60, 0, 1)

    return {
        "sound_db": round(sound_db, 2),
        "rms_amp": round(rms_amp, 3),
        "hf_energy_ratio": round(clamp(hf_ratio, 0, 1), 3),
        "fault_state": sound_fault
    }


# ---------- RPM Drift ----------
def apply_rpm_drift(base, elapsed, cfg):
    drift = base * (cfg["drift_percent"]/100) * math.sin(2*math.pi*elapsed/cfg["drift_period_sec"])
    jitter = random.uniform(-cfg["jitter_rpm"], cfg["jitter_rpm"])
    return max(0, base + drift + jitter)


# ---------- Main ----------
def main():
    cfg = load_config()
    temp_cfg = cfg["temperature_model"]
    curr_cfg = cfg["current_model"]
    sound_cfg = cfg["sound_model"]

    state = {}
    for d in cfg["devices"]:
        state[d["device_id"]] = {
            "rpm": d["rpm"],
            "mode": d["mode"],
            "temp_fault": d["temp_fault"],
            "current_fault": d["current_fault"],
            "sound_fault": d.get("sound_fault", "normal"),
            "temp": temp_cfg["ambient_c"],
            "topic_pub": d["topic_pub"],
            "topic_cmd": d["topic_cmd"]
        }

    client = mqtt.Client(
        client_id=f"{cfg['client_id_prefix']}-{uuid.uuid4().hex[:6]}",
        callback_api_version=mqtt.CallbackAPIVersion.VERSION2
    )
    client.username_pw_set(cfg["broker_user"], cfg["broker_pass"])

    def on_connect(c, u, f, rc, p):
        print("[OK] Connected")
        for st in state.values():
            c.subscribe(st["topic_cmd"], qos=cfg["qos"])
            print(f"[CMD-SUB] {st['topic_cmd']}")

    def on_message(c, u, msg):
        cmd = json.loads(msg.payload.decode())
        for did, st in state.items():
            if msg.topic == st["topic_cmd"]:
                if "mode" in cmd:
                    st["mode"] = cmd["mode"]
                if "rpm" in cmd:
                    st["rpm"] = float(cmd["rpm"])
                if "temp_fault" in cmd:
                    st["temp_fault"] = cmd["temp_fault"]
                if "current_fault" in cmd:
                    st["current_fault"] = cmd["current_fault"]
                if "sound_fault" in cmd:
                    st["sound_fault"] = cmd["sound_fault"]
                print(f"[CMD] {did} -> {cmd}")

    client.on_connect = on_connect
    client.on_message = on_message
    client.connect(cfg["broker_host"], cfg["broker_port"], 60)
    client.loop_start()

    start = time.time()

    try:
        while True:
            elapsed = time.time() - start
            for did, st in state.items():
                rpm_now = apply_rpm_drift(st["rpm"], elapsed, cfg["rpm_drift"])
                st["temp"] = simulate_temperature(st["temp_fault"], st["temp"], temp_cfg)

                vib = simulate_mpu6050(st["mode"], rpm_now, elapsed)
                current = simulate_current(st["current_fault"], rpm_now, curr_cfg)

                # sound uses vib health correlation
                sound = simulate_sound(st["sound_fault"], vib["health_score"], sound_cfg)

                payload = {
                    "ts_utc": utc_iso(),
                    "device_id": did,
                    "rpm": round(rpm_now, 2),

                    "mpu6050": {
                        "fault_state": st["mode"],
                        **vib
                    },

                    "ds18b20": {
                        "temperature_c": st["temp"],
                        "fault_state": st["temp_fault"]
                    },

                    "sct013": {
                        "current_a": current,
                        "fault_state": st["current_fault"]
                    },

                    "inmp441": sound
                }

                client.publish(st["topic_pub"], json.dumps(payload), qos=cfg["qos"])
                print(
                    f"[PUB] {did} | rpm={payload['rpm']} | "
                    f"temp={payload['ds18b20']['temperature_c']}C | "
                    f"current={payload['sct013']['current_a']}A | "
                    f"sound_db={payload['inmp441']['sound_db']} | "
                    f"sound_fault={payload['inmp441']['fault_state']}"
                )

            time.sleep(cfg["publish_interval_sec"])

    except KeyboardInterrupt:
        print("Stopped")
    finally:
        client.loop_stop()
        client.disconnect()


if __name__ == "__main__":
    main()
