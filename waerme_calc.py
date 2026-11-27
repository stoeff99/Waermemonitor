#!/usr/bin/env python3
"""Wärmeberechnung: abonniert Rohwerte per MQTT, rechnet Leistungen und Energiezähler,
publiziert die Ergebnisse wieder auf MQTT und schreibt Tageswerte.

Live:    <base>/calc/p_fbh_kw, p_prim_kw, p_flaeche_kw, p_wp_kw      (kW, Kühlen negativ)
         <base>/calc/e_speicher_kwh                                 (Energieinhalt Speicher)
         <base>/calc/e_<zaehler>_kwh                                (Zählerstände, retained)
         <base>/calc/laufzeit_heiz_h, laufzeit_ww_h, starts         (Betriebszähler, retained)
         <base>/diag/methode_<m>, alter_<eingang>, fehlende_eingaenge,
                     stromklasse, fbh_oeffnung_pct, fbh_unsicher          (Diagnose)
         <base>/status/waerme_calc_fehlend                          (JSON-Liste, retained)
Laufend: <base>/heute/<feld>   (Tageswerte des laufenden Tags bis jetzt, z.B. az_system)
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
import threading
import time
from datetime import datetime, timedelta
from zoneinfo import ZoneInfo

log = logging.getLogger("waerme_calc")

CP = 1.163 / 1000.0  # kWh/(l·K) für Wasser

COUNTERS_BASIS = [
    "e_fbh_heizen", "e_fbh_kuehlen",
    "e_prim_heizen", "e_prim_kuehlen",
    "e_flaeche_heizen", "e_flaeche_kuehlen",
    "e_wp", "e_wp_ww",
    "e_ww", "e_el_wp", "e_el_stab",
]
# Neu: FBH-Wärme bei zu wenig geöffneten Stellantrieben (unsicher, nicht in der Bilanz),
# Strom nach Klasse (aus der Momentanleistung wp_el_w), Wärme nach WP-Kennlinie (Methode E)
COUNTERS_NEU = ["e_fbh_unsicher", "e_el_bh", "e_el_heiz", "e_el_ww", "e_kennlinie_heiz"]
COUNTERS = COUNTERS_BASIS + COUNTERS_NEU
OPS = ["laufzeit_heiz_h", "laufzeit_ww_h", "starts"]  # Betriebszähler (keine kWh)
METHODS = ["fbh", "prim", "flaeche", "wp", "speicher"]

# Neue Spalten nur hinten anhängen, damit bestehende Auswertungen stabil bleiben.
DAY_FIELDS = (
    ["datum", "ts", "t_aussen_mittel"]
    + [f"{k}_kwh" for k in COUNTERS_BASIS]
    + ["de_speicher_kwh", "nutzwaerme_kwh", "az_system", "az_wp", "verlust_kwh",
       "verhaeltnis_fbh_flaeche", "verhaeltnis_fbh_prim"]
    + [f"abdeckung_{m}" for m in METHODS]
    + [f"{k}_kwh" for k in COUNTERS_NEU] + OPS
    + ["e_ww_schaetz_kwh", "e_el_rest_kwh", "anteil_bh", "anteil_ww_el",
       "az_wp_netto", "az_kennlinie", "luecke_strom_kwh"]
)

# Stromklassen für <base>/diag/stromklasse
KLASSE = {None: 0, "pending": 1, "bh": 2, "heiz": 3, "ww": 4}


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
    """Letzte Werte pro Eingang (Spec = 'topic' oder 'topic#json.pfad').

    Pro Spec: (Wert, Zeit der letzten Nachricht, Zeit der letzten Wertänderung).
    Quellen senden nur bei Änderung: ein Wert gilt, bis ein neuer kommt.
    Optional (max_age) kann ein Höchstalter erzwungen werden (Watchdog für Quellen,
    die periodisch senden). None = unbegrenzt. Die Änderungszeit erkennt eingefrorene
    Werte, auch wenn eine Quelle denselben Haltewert immer wieder sendet.
    """

    def __init__(self, max_age=None, max_age_spec=None):
        self.max_age = max_age
        self.max_age_spec = dict(max_age_spec or {})  # Spec -> Sekunden oder None (= unbegrenzt)
        self.v = {}

    def dump(self):
        return {spec: list(x) for spec, x in self.v.items()}

    def restore(self, data, specs):
        for spec, x in (data or {}).items():
            if spec in specs and spec not in self.v:
                try:
                    ts = float(x[1])
                    self.v[spec] = (float(x[0]), ts, float(x[2]) if len(x) > 2 else ts)
                except (TypeError, ValueError, IndexError):
                    pass

    def limit(self, spec):
        return self.max_age_spec.get(spec, self.max_age)

    def stale(self, spec, now):
        """True, wenn nie empfangen oder älter als das Limit dieses Eingangs."""
        x = self.v.get(spec)
        if x is None:
            return True
        lim = self.limit(spec)
        return lim is not None and now - x[1] > lim

    def set(self, spec, val, now):
        old = self.v.get(spec)
        changed = now if old is None or old[0] != val else old[2]
        self.v[spec] = (val, now, changed)

    def changed(self, spec):
        """Zeit der letzten Wertänderung (None = nie empfangen)."""
        x = self.v.get(spec)
        return None if x is None else x[2]

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
        if self.stale(spec, now):
            return None
        return self.v[spec][0]


class Calculator:
    def __init__(self, cfg, state=None):
        self.c = cfg["calc"]
        self.t = self.c.get("topics", {})
        self.flow = self.c.get("durchfluss_lh", {})
        self.off = self.c.get("offsets_k", {}) or {}
        self.tz = ZoneInfo(self.c.get("timezone", "Europe/Zurich"))
        self.tick_s = float(self.c.get("tick_s", 10))
        max_age = self.c.get("max_age_s")
        self.inp = Inputs(None if max_age is None else float(max_age), self._max_age_spec())
        frozen = self.c.get("eingefroren_s", 900)
        self.frozen_s = None if frozen is None else float(frozen)
        self.lock = threading.RLock()  # MQTT-Thread (handle_message) und Hauptschleife (tick)
        st = state or {}
        self.counters = {k: float(st.get("counters", {}).get(k, 0.0)) for k in COUNTERS + OPS}
        self.last_counter_in = dict(st.get("last_counter_in", {}))
        self.day = st.get("day")
        self.inp.restore(st.get("inputs"), set(self.input_specs().values()))
        self.last_tick = None
        self.power = {}
        self.e_speicher = None
        self.fbh_unsicher = False
        self.on_day_closed = lambda row: None
        # Zustandsmaschine Stromaufteilung (bewusst nicht persistiert)
        self.el = {"phase": None, "t0": None, "p": None, "t": None,
                   "pend_hi": 0.0, "pend_hi_s": 0.0, "pend_lo": 0.0, "bh_e": 0.0}

    # ---------- Eingänge ----------
    def input_specs(self):
        """Alle konfigurierten Eingänge als {Diagnose-Name: Spec}."""
        specs = {k: v for k, v in self.t.items() if v}
        for i, z in enumerate(self._zonen()):
            specs[f"speicher_{i}"] = z["topic"]
        for r in self._raeume():
            specs[f"boden_{r['name']}"] = r["t_boden"]
            specs[f"raum_{r['name']}"] = r["t_raum"]
        for i, sa in enumerate(self._stellantriebe()):
            specs[f"stellantrieb_{i}"] = sa
        return specs

    def _fbh_flow_cfg(self):
        return self.c.get("fbh_flow") or {}

    def _stellantriebe(self):
        return [x for x in (self._fbh_flow_cfg().get("stellantriebe") or []) if x]

    def _el_cfg(self):
        d = {"p_start_min_w": 150.0, "klassifikation_s": 30.0, "p_30s_max_w": 900.0,
             "p_verdichter_w": 1500.0, "p_lauf_min_w": 1000.0, "ww_wp_vl_min_c": 40.0,
             "vorlauf_max_s": 120.0,
             "zuschlag_bh_kwh_tag": 0.0, "cop_ww_schaetzung": 3.2}
        d.update({k: v for k, v in (self.c.get("stromaufteilung") or {}).items() if v is not None})
        return d

    def _max_age_spec(self):
        """max_age_pro_eingang {Name: Sekunden | null} auf Specs abbilden.

        null = unbegrenzt (Quelle sendet nur bei Änderung). Teilen sich mehrere Namen
        einen Spec, gilt das grosszügigste Limit.
        """
        over = self.c.get("max_age_pro_eingang") or {}
        specs = self.input_specs()
        # Stellantriebe senden selten -> Haltewert ohne Höchstalter (überschreibbar)
        out = {spec: None for spec in self._stellantriebe()}
        for name, lim in over.items():
            spec = specs.get(name)
            if spec is None:
                log.warning("max_age_pro_eingang: unbekannter Eingang '%s' wird ignoriert", name)
                continue
            lim = None if lim is None else float(lim)
            if spec in out and name.startswith("stellantrieb_"):
                out[spec] = lim
                continue
            if spec in out:
                old = out[spec]
                lim = None if old is None or lim is None else max(old, lim)
            out[spec] = lim
        return out

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
        with self.lock:
            for spec in self._routes.get(topic, ()):
                val = extract(payload, split_spec(spec)[1])
                if val is not None:
                    self.inp.set(spec, val, now)
                    if spec == self.t.get("wp_el_w"):
                        self._el_event(val, now)

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
            if self.inp.stale(spec, now):
                fehlend.append(f"{name} ({spec})")
        for m in ("fbh", "prim", "flaeche", "wp"):
            diag[f"methode_{m}"] = 0 if self.power.get(m) is None else 1
        diag["methode_speicher"] = 0 if self.e_speicher is None else 1
        diag["fehlende_eingaenge"] = len(fehlend)
        if self.t.get("wp_el_w"):
            diag["stromklasse"] = KLASSE[self._el_klasse(now)]
        if self._stellantriebe():
            o = self._fbh_oeffnung(now)
            diag["fbh_oeffnung_pct"] = -1 if o is None else round(o, 1)
            diag["fbh_unsicher"] = 1 if self.fbh_unsicher else 0
        return diag, fehlend

    def _temp(self, key, now):
        v = self.inp.get(self.t.get(key), now)
        return None if v is None else v + float(self.off.get(key, 0.0) or 0.0)

    # ---------- Methoden ----------
    def _kreis(self, name, now):
        """Leistung eines Wasserkreises in kW (Kühlen negativ). None = nicht berechenbar."""
        # Methode überhaupt konfiguriert? (Fühler-Topics und Durchfluss vorhanden)
        flow_topic = self.t.get(f"{name}_flow_lh")
        if not (self.t.get(f"{name}_vl") and self.t.get(f"{name}_rl")):
            return None
        if not flow_topic and self.flow.get(name) is None:
            return None
        # Pumpe zuerst: steht sie, ist die Leistung 0 – egal ob Temperaturen oder
        # Durchfluss gerade fehlen/veraltet sind (die Pause zählt als gültige Zeit).
        pumpe_topic = self.t.get(f"{name}_pumpe")
        if not pumpe_topic:
            return None  # ohne Pumpensignal würde stehendes Wasser Energie vortäuschen
        pumpe = self.inp.get(pumpe_topic, now)
        if pumpe is None:
            return None
        if pumpe < 0.5:
            return 0.0
        on_since = self.inp.changed(pumpe_topic) or now
        # Anlaufzeit: Fühler brauchen nach dem Pumpenstart, bis sie stationär messen
        anlauf = float((self.c.get("anlauf_s") or {}).get(name) or 0.0)
        if anlauf and now - on_since < anlauf:
            return None
        vl, rl = self._temp(f"{name}_vl", now), self._temp(f"{name}_rl", now)
        if vl is None or rl is None:
            return None
        # Eingefroren: Pumpe läuft, aber VL oder RL ändern sich seit frozen_s nicht mehr
        if self.frozen_s is not None:
            for key in (f"{name}_vl", f"{name}_rl"):
                ch = self.inp.changed(self.t[key]) or now
                if now - max(ch, on_since) > self.frozen_s:
                    return None
        if flow_topic:
            flow = self.inp.get(flow_topic, now)
        elif name == "fbh":
            flow = self._fbh_flow(now)
        else:
            flow = self.flow.get(name)
        if flow is None:
            return None
        return flow * (vl - rl) * CP

    # ---------- FBH-Stellantriebe ----------
    def _fbh_oeffnung(self, now):
        """Mittlere Öffnung der Stellantriebe in % (None = keine Werte)."""
        vals = [self.inp.get(s, now) for s in self._stellantriebe()]
        vals = [v for v in vals if v is not None]
        return sum(vals) / len(vals) if vals else None

    def _fbh_flow(self, now):
        """FBH-Durchfluss: Kennlinie Öffnung -> l/h, sonst die Konstante durchfluss_lh.fbh."""
        const = self.flow.get("fbh")
        pts = sorted((float(a), float(b)) for a, b in (self._fbh_flow_cfg().get("kennlinie") or [])
                     if a is not None and b is not None)
        o = self._fbh_oeffnung(now) if pts else None
        if o is None:
            return const
        if o <= pts[0][0]:
            return pts[0][1]
        for (x0, y0), (x1, y1) in zip(pts, pts[1:]):
            if o <= x1:
                return y0 + (y1 - y0) * (o - x0) / (x1 - x0)
        return pts[-1][1]

    def _fbh_ist_unsicher(self, now):
        """True, wenn die Stellantriebe im Mittel unter min_oeffnung_pct stehen."""
        lim = self._fbh_flow_cfg().get("min_oeffnung_pct")
        if lim is None:
            return False
        o = self._fbh_oeffnung(now)
        return o is not None and o < float(lim)

    # ---------- Stromaufteilung (Momentanleistung wp_el_w) ----------
    def _el_spec_frozen(self, p, now):
        """Leistung über der Standby-Schwelle, die sich lange nicht ändert = Quelle hängt."""
        if self.frozen_s is None or p is None or p < self._el_cfg()["p_start_min_w"]:
            return False
        ch = self.inp.changed(self.t.get("wp_el_w"))
        return ch is not None and now - ch > self.frozen_s

    def _ww_modus(self, now):
        vl = self._temp("wp_vl", now)
        return vl is not None and vl > float(self._el_cfg()["ww_wp_vl_min_c"])

    def _el_lauf(self, e_kwh, dt_s, now):
        """Verdichterlauf: Strom und Laufzeit auf Heizen bzw. WW verbuchen."""
        if self._ww_modus(now):
            self.counters["e_el_ww"] += e_kwh
            self.counters["laufzeit_ww_h"] += dt_s / 3600.0
            return
        self.counters["e_el_heiz"] += e_kwh
        self.counters["laufzeit_heiz_h"] += dt_s / 3600.0
        k = self.c.get("kennlinie_wp") or {}
        if k.get("p_th_kw") is not None:
            p_th = float(k["p_th_kw"])
            tq = self._temp("t_quelle", now)
            if tq is not None:
                p_th += float(k.get("dp_th_pro_k") or 0.0) * tq
            self.counters["e_kennlinie_heiz"] += max(p_th, 0.0) * dt_s / 3600.0

    def _el_advance(self, now):
        """Leistung seit der letzten Nachricht/dem letzten Tick der aktuellen Klasse gutschreiben."""
        el, cfg = self.el, self._el_cfg()
        p, t = el["p"], el["t"]
        el["t"] = now
        if p is None or t is None or now <= t or self._el_spec_frozen(p, now):
            return
        dt = min(now - t, 3600.0)
        e = p * dt / 3.6e6
        hi = p >= cfg["p_lauf_min_w"]
        ph = el["phase"]
        if ph == "pending":
            if hi:
                el["pend_hi"] += e
                el["pend_hi_s"] += dt
            else:
                el["pend_lo"] += e
        elif ph == "bh":
            self.counters["e_el_bh"] += e
            el["bh_e"] += e
        elif ph == "vd" and hi:
            self._el_lauf(e, dt, now)
        # Standby, Pumpen-Vorlauf/-Nachlauf: Rest (ergibt sich aus dem Zählerstand)

    def _el_check(self, now):
        """30 s nach Phasenbeginn klassieren: Begleitheizung oder Verdichter."""
        el, cfg = self.el, self._el_cfg()
        if el["phase"] != "pending" or now - el["t0"] < cfg["klassifikation_s"]:
            return
        if el["p"] is not None and el["p"] < cfg["p_30s_max_w"]:
            el["phase"] = "bh"
            el["bh_e"] = el["pend_hi"] + el["pend_lo"]
            self.counters["e_el_bh"] += el["bh_e"]
        else:
            el["phase"] = "vd"
            self.counters["starts"] += 1
            self._el_lauf(el["pend_hi"], el["pend_hi_s"], now)
        el["pend_hi"] = el["pend_hi_s"] = el["pend_lo"] = 0.0

    def _el_event(self, p_new, now):
        el, cfg = self.el, self._el_cfg()
        self._el_advance(now)
        self._el_check(now)
        ph = el["phase"]
        if p_new < cfg["p_start_min_w"]:
            el["phase"] = None  # Phase zu Ende; ein kurzer Ausschlag < 30 s zählt als Rest
            el["pend_hi"] = el["pend_hi_s"] = el["pend_lo"] = 0.0
        elif ph is None:
            el["phase"], el["t0"] = "pending", now
        elif ph == "bh" and p_new >= cfg["p_verdichter_w"]:
            el["phase"] = "vd"  # Verdichterstart aus einer Begleitheizungsphase
            self.counters["starts"] += 1
            if now - el["t0"] <= cfg["vorlauf_max_s"]:
                # So kurz vor dem Verdichter war es der Pumpen-Vorlauf (≈300 W), keine Begleitheizung
                self.counters["e_el_bh"] -= el["bh_e"]
        el["p"] = p_new

    def _el_klasse(self, now):
        ph = self.el["phase"]
        if ph == "vd":
            return "ww" if self._ww_modus(now) else "heiz"
        return ph

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
            "luecke_strom_kwh": 0.0,
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
        delta = {k: self.counters[k] - d["start"].get(k, 0.0) for k in COUNTERS + OPS}

        row = {k: None for k in DAY_FIELDS}
        row["datum"] = d["datum"]
        row["ts"] = self._midnight_ts(datum)
        if d["ta_s"] > 0:
            row["t_aussen_mittel"] = round(d["ta_sum"] / d["ta_s"], 2)
        for k in COUNTERS:
            row[f"{k}_kwh"] = round(delta[k], 3)
        for k in OPS:
            row[k] = round(delta[k], 3)
        row["luecke_strom_kwh"] = round(d.get("luecke_strom_kwh") or 0.0, 3)
        for m in METHODS:
            row[f"abdeckung_{m}"] = round(min(d["valid_s"][m] / day_len, 1.0), 3)

        de_sp = None
        if d.get("e_speicher_start") is not None and d.get("e_speicher_last") is not None:
            de_sp = d["e_speicher_last"] - d["e_speicher_start"]
            row["de_speicher_kwh"] = round(de_sp, 3)

        el = delta["e_el_wp"] + delta["e_el_stab"]
        ww = delta["e_ww"]
        bh = None
        if self.t.get("wp_el_w"):
            cfg = self._el_cfg()
            ref = self.last_tick if self.last_tick is not None else row["ts"]
            frac = min(max((ref - row["ts"]) / day_len, 0.0), 1.0)
            bh = delta["e_el_bh"] + float(cfg["zuschlag_bh_kwh_tag"]) * frac
            row["e_el_bh_kwh"] = round(bh, 3)
            row["e_el_rest_kwh"] = round(
                delta["e_el_wp"] - bh - delta["e_el_heiz"] - delta["e_el_ww"], 3)
            ww_s = delta["e_el_ww"] * float(cfg["cop_ww_schaetzung"])
            row["e_ww_schaetz_kwh"] = round(ww_s, 3)
            ww = max(ww, ww_s)  # bis ein WW-Zähler oder WP-Durchfluss vorhanden ist
            if el > 0.5:
                row["anteil_bh"] = round(bh / el, 3)
                row["anteil_ww_el"] = round(delta["e_el_ww"] / el, 3)
            netto = delta["e_el_wp"] - bh
            if netto > 0.5 and delta["e_kennlinie_heiz"] > 0:
                row["az_kennlinie"] = round((delta["e_kennlinie_heiz"] + ww_s) / netto, 3)
        if de_sp is not None:
            nutz = delta["e_fbh_heizen"] + ww + de_sp
            row["nutzwaerme_kwh"] = round(nutz, 3)
            if el > 0.5:
                row["az_system"] = round(nutz / el, 3)
            if bh is not None and el - bh > 0.5:
                row["az_wp_netto"] = round(nutz / (el - bh), 3)
        if delta["e_el_wp"] > 0.5 and delta["e_wp"] > 0.5:
            row["az_wp"] = round(delta["e_wp"] / delta["e_el_wp"], 3)
        if self.flow.get("wp") or self.t.get("wp_flow_lh"):
            if de_sp is not None:
                row["verlust_kwh"] = round(
                    delta["e_wp"] + delta["e_el_stab"] - delta["e_fbh_heizen"]
                    - ww - de_sp, 3)
        if delta["e_flaeche_heizen"] > 0.5:
            row["verhaeltnis_fbh_flaeche"] = round(delta["e_fbh_heizen"] / delta["e_flaeche_heizen"], 3)
        if delta["e_prim_heizen"] > 0.5:
            row["verhaeltnis_fbh_prim"] = round(delta["e_fbh_heizen"] / delta["e_prim_heizen"], 3)
        return row

    # ---------- Hauptschritt ----------
    def tick(self, now=None):
        with self.lock:
            self._tick(time.time() if now is None else now)

    def _tick(self, now):
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
        # FBH bei zu wenig geöffneten Stellantrieben: Durchfluss unbekannt -> separat zählen
        self.fbh_unsicher = bool(p["fbh"]) and self._fbh_ist_unsicher(now)
        if self.fbh_unsicher:
            self.counters["e_fbh_unsicher"] += max(p["fbh"], 0.0) * h
        for m in ("fbh", "prim", "flaeche"):
            if m == "fbh" and self.fbh_unsicher:
                continue
            if p[m] is not None:
                self.counters[f"e_{m}_heizen"] += max(p[m], 0.0) * h
                self.counters[f"e_{m}_kuehlen"] += max(-p[m], 0.0) * h
        if p["wp"] is not None:
            e = max(p["wp"], 0.0) * h
            self.counters["e_wp"] += e
            ww = self.inp.get(self.t.get("wp_ww_aktiv"), now)
            if ww is not None and ww >= 0.5:
                self.counters["e_wp_ww"] += e

        if self.t.get("wp_el_w"):
            self._el_advance(now)
            self._el_check(now)

        d_el = self._delta_counter("wp_el_kwh", now)
        if d_el:
            self.counters["e_el_wp"] += d_el
            # Grosser Sprung auf einmal = nachgeholte Lücke (Quelle oder Dienst waren weg)
            if d_el > float(self.c.get("luecke_strom_kwh", 1.0)):
                self.day["luecke_strom_kwh"] = self.day.get("luecke_strom_kwh", 0.0) + d_el
                log.warning("Stromzähler springt um %.2f kWh – Tag wird als lückenhaft markiert", d_el)
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
                if val is not None and not (m == "fbh" and self.fbh_unsicher):
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
        with self.lock:
            return self._state()

    def _state(self):
        return {"counters": self.counters, "last_counter_in": self.last_counter_in, "day": self.day,
                "inputs": self.inp.dump()}

    def publish_values(self, now):
        """Alles, was die Hauptschleife publiziert, konsistent unter dem Lock einsammeln."""
        with self.lock:
            power = dict(self.power)
            counters = dict(self.counters)
            heute = self.close_day() if self.day is not None else {}
            diag, fehlend = self.diagnose(now)
            return power, self.e_speicher, counters, heute, diag, fehlend


# ---------------------------------------------------------------------
def save_json_atomic(path, data):
    tmp = path + ".tmp"
    with open(tmp, "w", encoding="utf-8") as f:
        json.dump(data, f)
    os.replace(tmp, path)


def append_csv(path, row):
    new = not os.path.exists(path)
    if not new:
        with open(path, newline="", encoding="utf-8") as f:
            header = next(csv.reader(f), None)
        if header and header != DAY_FIELDS:
            # Neue Spalten: Datei einmalig mit dem neuen Kopf umschreiben
            with open(path, newline="", encoding="utf-8") as f:
                old = list(csv.DictReader(f))
            tmp = path + ".tmp"
            with open(tmp, "w", newline="", encoding="utf-8") as f:
                w = csv.DictWriter(f, fieldnames=DAY_FIELDS, extrasaction="ignore", restval="")
                w.writeheader()
                w.writerows(old)
            os.replace(tmp, path)
            log.info("tageswerte.csv auf neue Spalten umgestellt (%d Zeilen)", len(old))
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
            power, e_sp, counters, heute, diag, fehlend = calc.publish_values(now)
            for m, v in power.items():
                if v is not None:
                    client.publish(f"{base}/calc/p_{m}_kw", f"{v:.3f}")
            if e_sp is not None:
                client.publish(f"{base}/calc/e_speicher_kwh", f"{e_sp:.3f}")
            for k, v in counters.items():
                name = k if k in OPS else f"{k}_kwh"
                client.publish(f"{base}/calc/{name}", f"{v:.3f}", retain=True)
            # Laufender Tag bis jetzt (gleiche Felder wie die Tageswerte, inkl. Arbeitszahl)
            for k, v in heute.items():
                if k not in ("datum", "ts") and v is not None:
                    client.publish(f"{base}/heute/{k}", str(v))
            for k, v in diag.items():
                client.publish(f"{base}/diag/{k}", str(v))
            client.publish(f"{base}/status/waerme_calc_fehlend", json.dumps(fehlend), retain=True)
            if fehlend != last_fehlend:
                if fehlend:
                    log.warning("Fehlende Eingänge: %s", ", ".join(fehlend))
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
