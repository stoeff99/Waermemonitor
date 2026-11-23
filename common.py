"""Gemeinsame Hilfsfunktionen: Konfiguration, Logging, MQTT-Client."""
import logging
import os
import socket

import yaml
import paho.mqtt.client as mqtt

DEFAULT_CONFIG = "/opt/waermemonitor/config.yaml"


def load_config(path=None):
    path = path or os.environ.get("WAERME_CONFIG", DEFAULT_CONFIG)
    with open(path, encoding="utf-8") as f:
        return yaml.safe_load(f)


def setup_logging():
    logging.basicConfig(
        level=os.environ.get("LOGLEVEL", "INFO"),
        format="%(asctime)s %(levelname)s %(name)s: %(message)s",
    )


def make_client(cfg, name, subscriptions=(), on_message=None):
    """MQTT-Client mit Last Will, Auto-Reconnect und Abo nach jedem Connect.

    Funktioniert mit paho-mqtt 1.x und 2.x.
    """
    m = cfg["mqtt"]
    base = m.get("base_topic", "heizung")
    status_topic = f"{base}/status/{name}"
    client_id = f"{name}-{socket.gethostname()}"
    try:
        client = mqtt.Client(mqtt.CallbackAPIVersion.VERSION2, client_id=client_id)
    except AttributeError:  # paho 1.x
        client = mqtt.Client(client_id=client_id)

    if m.get("username"):
        client.username_pw_set(m["username"], m.get("password") or None)
    client.will_set(status_topic, "offline", qos=1, retain=True)
    log = logging.getLogger(name)

    def _on_connect(cl, userdata, flags, *args):
        log.info("MQTT verbunden mit %s", m["host"])
        cl.publish(status_topic, "online", qos=1, retain=True)
        for t in subscriptions:
            cl.subscribe(t, qos=0)

    def _on_disconnect(cl, userdata, *args):
        log.warning("MQTT getrennt – verbinde neu")

    client.on_connect = _on_connect
    client.on_disconnect = _on_disconnect
    if on_message:
        client.on_message = on_message
    client.reconnect_delay_set(min_delay=1, max_delay=60)
    client.connect_async(m["host"], int(m.get("port", 1883)), keepalive=60)
    client.loop_start()
    client.status_topic = status_topic
    return client
