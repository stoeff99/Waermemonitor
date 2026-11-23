#!/usr/bin/env python3
"""Wärmeberechnung: abonniert Rohwerte per MQTT, rechnet Leistungen und Energiezähler,
publiziert die Ergebnisse wieder auf MQTT und schreibt Tageswerte.

Live:    <base>/calc/p_fbh_kw, p_prim_kw, p_flaeche_kw, p_wp_kw      (kW, Kühlen negativ)
         <base>/calc/e_speicher_kwh                                 (Energieinhalt Speicher)
         <base>/calc/e_<zaehler>_kwh                                (Zählerstände, retained)
         <base>/diag/methode_<m>, alter_<eingang>, fehlende_eingaenge   (Diagnose)
         <base>/status/waerme_calc_fehlend                          (JSON-Liste, retained)
Täglich: <base>/tag/json   (ein JSON pro Tag, Zeitstempel = lokale Mitternacht)
         <state_dir>/tageswerte.csv

Eingänge: reine Zahl, on/off, JSON-Zahl, JSON-Objekt mit "value" oder beliebiges
JSON-Feld über 'topic#pfad.zum.feld' in der config.yaml.
"""
import csv
import json
import logging
import os
import signal
import time
from datetime import datetime, timedelta
from zoneinfo import ZoneInfo

log = logging.getLogger("waerme_calc")

CP = 1.163 / 1000.0  # kWh/(l·K) für Wasser

COUNTERS = [
    "e_fbh_heizen", "e_fbh_kuehlen",
    "e_prim_heizen", "e_prim_kuehlen",
    "e_flaeche_heizen", "e_flaeche_kuehlen",
    "e_wp", "e_wp_ww",
    "e_ww", "e_el_wp", "e_el_stab",
]
METHODS = ["fbh", "prim", "flaeche", "wp", "speicher"]

DAY_FIELDS = (
    ["datum", "ts", "t_aussen_mittel"]
    + [f"{k}_kwh" for k in COUNTERS]
    + ["de_speicher_kwh", "nutzwaerme_kwh", "az_system", "az_wp", "verlust_kwh",
       "verhaeltnis_fbh_flaeche", "verhaeltnis_fbh_prim"]
    + [f"abdeckung_{m}" for m in METHODS]
)


def split_spec(spec):
    """'a/b/c#x.y' -> ('a/b/c', 'x.y');  'a/b/c' -> ('a/b/c', None)"""
    topic, _, key = spec.partition("#")
    return topic, (key or None)


def _scalar(v):
    if isinstance(v, bool):
        return 1.0 if v else 0.0
    if isinstance(v, (int, float)):
        return float(v)
    if isinstance(v, str):
        s = v.strip()
        try:
            return float(s)
        except ValueError:
            low = s.lower()
            if low in ("on", "true", "ein"):
                return 1.0
            if low in ("off", "false", "aus"):
                return 0.0
    return None


def extract(payload, key=None):
    """Zahl aus einem MQTT-Payload: reine Zahl, on/off, JSON-Zahl oder JSON-Objekt.

    key: Punkt-Pfad im JSON (z.B. 'value' oder 'data.temp'). Ohne key wird bei einem
    JSON-Objekt das Feld 'value' verwendet.
    """
    v = _scalar(payload) if key is None else None
    if v is not None:
        return v
    try:
        obj = json.loads(payload)
    except (ValueError, TypeError):
        return None
    if key is None:
        if isinstance(obj, dict):
            obj = obj.get("value")
        return _scalar(obj)
    for part in key.split("."):
        if isinstance(obj, dict) and part in obj:
            obj = obj[part]
        elif isinstance(obj, list) and part.isdigit() and int(part) < len(obj):
            obj = obj[int(part)]
        else:
            return None
    return _scalar(obj)


class Inputs:
    """Letzte Werte pro Eingang (Spec = 'topic' oder 'topic#json.pfad') mit Zeitstempel."""

    def __init__(self, max_age):
        self.max_age = max_age
        self.v = {}

    def set(self, spec, val, now):
        self.v[spec] = (val, now)

    def update(self, spec, payload, now):
        """Für Tests/Kompatibilität: Payload direkt einem Spec zuweisen."""
        val = extract(payload, split_spec(spec)[1])
        if val is not None:
            self.set(spec, val, now)

    def age(self, spec, now):
        x = self.v.get(spec)
        return None if x is None else now - x[1]

    def get(self, spec, now):
        if not spec:
            return None
        x = self.v.get(spec)
        if x is None or now - x[1] > self.max_age:
            return None
        return x[0]


class Calculator:
    def __init__(self, cfg, state=None):
        self.c = cfg["calc"]
        self.t = self.c.get("topics", {})
        self.flow = self.c.get("durchfluss_lh", {})
        self.off = self.c.get("offsets_k", {}) or {}
        self.tz = ZoneInfo(self.c.get("timezone", "Europe/Zurich"))
        self.tick_s = float(self.c.get("tick_s", 10))
        self.inp = Inputs(float(self.c.get("max_age_s", 180)))
        st = state or {}
        self.counters = {k: float(st.get("counters", {}).get(k, 0.0)) for k in COUNTERS}
        self.last_counter_in = dict(st.get("last_counter_in", {}))
        self.day = st.get("day")
        self.last_tick = None
        self.power = {}
        self.e_speicher = None
        self.on_day_closed = lambda row: None

    # ---------- Eingänge ----------
    def input_specs(self):
        """Alle konfigurierten Eingänge als {Diagnose-Name: Spec}."""
        specs = {k: v for k, v in self.t.items() if v}
        for i, z in enumerate(self._zonen()):
            specs[f"speicher_{i}"] = z["topic"]
        for r in self._raeume():
            specs[f"boden_{r['name']}"] = r["t_boden"]
            specs[f"raum_{r['name']}"] = r["t_raum"]
        return specs

    def _zonen(self):
        """Speicherzonen mit gültigem Topic und Volumen (Zonen mit topic: null werden ignoriert)."""
        out = []
        for z in (self.c.get("speicher") or {}).get("zonen") or []:
            if isinstance(z, dict) and z.get("topic") and z.get("liter"):
                out.append(z)
            elif not getattr(self, "_warned_zonen", False):
                log.warning("Speicherzone ohne topic/liter wird ignoriert: %s", z)
        self._warned_zonen = True
        return out

    def _raeume(self):
        """Räume mit Name, Fläche und beiden Topics (unvollständige werden ignoriert)."""
        out = []
        for r in (self.c.get("flaeche") or {}).get("raeume") or []:
            if isinstance(r, dict) and r.get("name") and r.get("m2") and r.get("t_boden") and r.get("t_raum"):
                out.append(r)
            elif not getattr(self, "_warned_raeume", False):
                log.warning("Raum unvollständig, wird ignoriert: %s", r)
        self._warned_raeume = True
        return out

    def input_topics(self):
        return sorted({split_spec(s)[0] for s in self.input_specs().values()})

    def handle_message(self, topic, payload, now):
        for spec in self._routes.get(topic, ()):
            val = extract(payload, split_spec(spec)[1])
            if val is not None:
                self.inp.set(spec, val, now)

    @property
    def _routes(self):
        if not hasattr(self, "_routes_cache"):
            r = {}
            for spec in self.input_specs().values():
                r.setdefault(split_spec(spec)[0], []).append(spec)
            self._routes_cache = r
        return self._routes_cache

    def diagnose(self, now):
        """Zahlenwerte für <base>/diag/<name> und Liste fehlender Eingänge."""
        diag, fehlend = {}, []
        for name, spec in self.input_specs().items():
            age = self.inp.age(spec, now)
            diag[f"alter_{name}"] = -1 if age is None else round(age)
            if age is None or age > self.inp.max_age:
                fehlend.append(f"{name} ({spec})")
        for m in ("fbh", "prim", "flaeche", "wp"):
            diag[f"methode_{m}"] = 0 if self.power.get(m) is None else 1
        diag["methode_speicher"] = 0 if self.e_speicher is None else 1
        diag["fehlende_eingaenge"] = len(fehlend)
        return diag, fehlend

    def _temp(self, key, now):
        v = self.inp.get(self.t.get(key), now)
        return None if v is None else v + float(self.off.get(key, 0.0) or 0.0)

    # ---------- Methoden ----------
    def _kreis(self, name, now):
        """Leistung eines Wasserkreises in kW (Kühlen negativ). None = nicht berechenbar."""
        vl, rl = self._temp(f"{name}_vl", now), self._temp(f"{name}_rl", now)
        if vl is None or rl is None:
            return None
        flow_topic = self.t.get(f"{name}_flow_lh")
        flow = self.inp.get(flow_topic, now) if flow_topic else self.flow.get(name)
        if flow is None:
            return None
        pumpe_topic = self.t.get(f"{name}_pumpe")
        if not pumpe_topic:
            return None  # ohne Pumpensignal würde stehendes Wasser Energie vortäuschen
        pumpe = self.inp.get(pumpe_topic, now)
        if pumpe is None:
            return None
        if pumpe < 0.5:
            return 0.0
        return flow * (vl - rl) * CP

    def _flaeche(self, now):
        f = self.c.get("flaeche") or {}
        raeume = self._raeume()
        if not raeume:
            return None
        k = float(f.get("k_w_m2k", 10.8))
        total = 0.0
        for r in raeume:
            tb, tr = self.inp.get(r["t_boden"], now), self.inp.get(r["t_raum"], now)
            if tb is None or tr is None:
                return None
            total += float(r["m2"]) * k * (tb - tr)
        return total / 1000.0

    def _speicher(self, now):
        s = self.c.get("speicher") or {}
        zonen = self._zonen()
        if not zonen:
            return None
        tref = float(s.get("t_ref", 10.0))
        e = 0.0
        for z in zonen:
            t = self.inp.get(z["topic"], now)
            if t is None:
                return None
            t += float(z.get("offset_k", 0.0) or 0.0)
            e += float(z["liter"]) * CP * (t - tref)
        return e

    def _delta_counter(self, key, now):
        """Zuwachs eines Zählerstands seit dem letzten Tick (Reset-fest)."""
        v = self.inp.get(self.t.get(key), now)
        if v is None:
            return None
        last = self.last_counter_in.get(key)
        self.last_counter_in[key] = v
        if last is None or v < last:
            return 0.0
        return v - last

    # ---------- Tageslogik ----------
    def _date(self, now):
        return datetime.fromtimestamp(now, self.tz).date()

    def _midnight_ts(self, d):
        return int(datetime(d.year, d.month, d.day, tzinfo=self.tz).timestamp())

    def _new_day(self, now, e_speicher_start=None):
        return {
            "datum": self._date(now).isoformat(),
            "start": dict(self.counters),
            "ta_sum": 0.0, "ta_s": 0.0,
            "valid_s": {m: 0.0 for m in METHODS},
            "e_speicher_start": e_speicher_start,
            "e_speicher_last": e_speicher_start,
        }

    def _check_day(self, now):
        if self.day is None:
            self.day = self._new_day(now, self.e_speicher)
            return
        if self.day["datum"] != self._date(now).isoformat():
            row = self.close_day()
            self.day = self._new_day(now, self.day.get("e_speicher_last"))
            self.on_day_closed(row)

    def close_day(self):
        d = self.day
        datum = datetime.fromisoformat(d["datum"]).date()
        day_len = self._midnight_ts(datum + timedelta(days=1)) - self._midnight_ts(datum)
        delta = {k: self.counters[k] - d["start"].get(k, 0.0) for k in COUNTERS}

        row = {k: None for k in DAY_FIELDS}
        row["datum"] = d["datum"]
        row["ts"] = self._midnight_ts(datum)
        if d["ta_s"] > 0:
            row["t_aussen_mittel"] = round(d["ta_sum"] / d["ta_s"], 2)
        for k, v in delta.items():
            row[f"{k}_kwh"] = round(v, 3)
        for m in METHODS:
            row[f"abdeckung_{m}"] = round(min(d["valid_s"][m] / day_len, 1.0), 3)

        de_sp = None
        if d.get("e_speicher_start") is not None and d.get("e_speicher_last") is not None:
            de_sp = d["e_speicher_last"] - d["e_speicher_start"]
            row["de_speicher_kwh"] = round(de_sp, 3)

        el = delta["e_el_wp"] + delta["e_el_stab"]
        if de_sp is not None:
            nutz = delta["e_fbh_heizen"] + delta["e_ww"] + de_sp
            row["nutzwaerme_kwh"] = round(nutz, 3)
            if el > 0.5:
                row["az_system"] = round(nutz / el, 3)
        if delta["e_el_wp"] > 0.5 and delta["e_wp"] > 0.5:
            row["az_wp"] = round(delta["e_wp"] / delta["e_el_wp"], 3)
        if self.flow.get("wp") or self.t.get("wp_flow_lh"):
            if de_sp is not None:
                row["verlust_kwh"] = round(
                    delta["e_wp"] + delta["e_el_stab"] - delta["e_fbh_heizen"]
                    - delta["e_ww"] - de_sp, 3)
        if delta["e_flaeche_heizen"] > 0.5:
            row["verhaeltnis_fbh_flaeche"] = round(delta["e_fbh_heizen"] / delta["e_flaeche_heizen"], 3)
        if delta["e_prim_heizen"] > 0.5:
            row["verhaeltnis_fbh_prim"] = round(delta["e_fbh_heizen"] / delta["e_prim_heizen"], 3)
        return row

    # ---------- Hauptschritt ----------
    def tick(self, now=None):
        now = time.time() if now is None else now
        dt = 0.0 if self.last_tick is None else min(now - self.last_tick, 3 * self.tick_s)
        self.last_tick = now

        e_sp = self._speicher(now)
        self.e_speicher = e_sp
        self._check_day(now)
        h = dt / 3600.0

        p = {
            "fbh": self._kreis("fbh", now),
            "prim": self._kreis("prim", now),
            "flaeche": self._flaeche(now),
            "wp": self._kreis("wp", now),
        }
        self.power = p
        for m in ("fbh", "prim", "flaeche"):
            if p[m] is not None:
                self.counters[f"e_{m}_heizen"] += max(p[m], 0.0) * h
                self.counters[f"e_{m}_kuehlen"] += max(-p[m], 0.0) * h
        if p["wp"] is not None:
            e = max(p["wp"], 0.0) * h
            self.counters["e_wp"] += e
            ww = self.inp.get(self.t.get("wp_ww_aktiv"), now)
            if ww is not None and ww >= 0.5:
                self.counters["e_wp_ww"] += e

        d_el = self._delta_counter("wp_el_kwh", now)
        if d_el:
            self.counters["e_el_wp"] += d_el
        d_stab = self._delta_counter("stab_el_kwh", now)
        if d_stab:
            self.counters["e_el_stab"] += d_stab
        d_l = self._delta_counter("ww_liter", now)
        if d_l:
            tm, tk = self._temp("ww_t_misch", now), self._temp("ww_t_kalt", now)
            if tm is not None and tk is not None:
                self.counters["e_ww"] += d_l * max(tm - tk, 0.0) * CP

        day = self.day
        if dt > 0:
            for m, val in p.items():
                if val is not None:
                    day["valid_s"][m] += dt
            if e_sp is not None:
                day["valid_s"]["speicher"] += dt
            ta = self.inp.get(self.t.get("t_aussen"), now)
            if ta is not None:
                day["ta_sum"] += ta * dt
                day["ta_s"] += dt
        if e_sp is not None:
            if day.get("e_speicher_start") is None:
                day["e_speicher_start"] = e_sp
            day["e_speicher_last"] = e_sp

    def state(self):
        return {"counters": self.counters, "last_counter_in": self.last_counter_in, "day": self.day}


# ---------------------------------------------------------------------
def save_json_atomic(path, data):
    tmp = path + ".tmp"
    with open(tmp, "w", encoding="utf-8") as f:
        json.dump(data, f)
    os.replace(tmp, path)


def append_csv(path, row):
    new = not os.path.exists(path)
    with open(path, "a", newline="", encoding="utf-8") as f:
        w = csv.DictWriter(f, fieldnames=DAY_FIELDS, extrasaction="ignore")
        if new:
            w.writeheader()
        w.writerow({k: ("" if v is None else v) for k, v in row.items()})


def main():
    from common import load_config, make_client, setup_logging

    setup_logging()
    cfg = load_config()
    c = cfg["calc"]
    base = cfg["mqtt"].get("base_topic", "heizung")
    state_dir = c.get("state_dir", "/var/lib/waermemonitor")
    os.makedirs(state_dir, exist_ok=True)
    state_file = os.path.join(state_dir, "state.json")
    csv_file = os.path.join(state_dir, "tageswerte.csv")

    state = None
    if os.path.exists(state_file):
        with open(state_file, encoding="utf-8") as f:
            state = json.load(f)
        log.info("Zustand geladen (%s)", state_file)

    calc = Calculator(cfg, state)
    topics = calc.input_topics()
    log.info("Abonniere %d Topics", len(topics))

    def on_message(cl, userdata, msg):
        calc.handle_message(msg.topic, msg.payload.decode("utf-8", errors="ignore"), time.time())

    client = make_client(cfg, "waerme_calc", subscriptions=topics, on_message=on_message)

    def day_closed(row):
        client.publish(f"{base}/tag/json", json.dumps(row), qos=1, retain=False)
        append_csv(csv_file, row)
        log.info("Tag %s abgeschlossen: FBH %.1f kWh, WW %s kWh, Strom %.1f kWh",
                 row["datum"], row["e_fbh_heizen_kwh"] or 0, row["e_ww_kwh"],
                 (row["e_el_wp_kwh"] or 0) + (row["e_el_stab_kwh"] or 0))

    calc.on_day_closed = day_closed

    running = True

    def stop(*_):
        nonlocal running
        running = False

    signal.signal(signal.SIGTERM, stop)
    signal.signal(signal.SIGINT, stop)

    publish_s = float(c.get("publish_s", 30))
    last_pub = 0.0
    last_fehlend = None
    time.sleep(min(10.0, calc.tick_s))  # erste Werte abwarten
    while running:
        calc.tick()
        now = time.time()
        if now - last_pub >= publish_s:
            last_pub = now
            for m, v in calc.power.items():
                if v is not None:
                    client.publish(f"{base}/calc/p_{m}_kw", f"{v:.3f}")
            if calc.e_speicher is not None:
                client.publish(f"{base}/calc/e_speicher_kwh", f"{calc.e_speicher:.3f}")
            for k, v in calc.counters.items():
                client.publish(f"{base}/calc/{k}_kwh", f"{v:.3f}", retain=True)
            diag, fehlend = calc.diagnose(now)
            for k, v in diag.items():
                client.publish(f"{base}/diag/{k}", str(v))
            client.publish(f"{base}/status/waerme_calc_fehlend", json.dumps(fehlend), retain=True)
            if fehlend != last_fehlend:
                if fehlend:
                    log.warning("Fehlende/veraltete Eingänge: %s", ", ".join(fehlend))
                else:
                    log.info("Alle Eingänge aktuell")
                last_fehlend = fehlend
            save_json_atomic(state_file, calc.state())
        slept = 0.0
        while running and slept < calc.tick_s:
            time.sleep(0.5)
            slept += 0.5

    save_json_atomic(state_file, calc.state())
    client.publish(client.status_topic, "offline", qos=1, retain=True)
    time.sleep(0.5)
    client.loop_stop()


if __name__ == "__main__":
    main()
