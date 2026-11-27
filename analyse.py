#!/usr/bin/env python3
"""Tägliche Auswertung der Tageswerte (CSV von waerme_calc.py).

1. Energiesignatur: Heizwärme/Tag gegen Aussentemperatur -> H [W/K], Heizgrenze, Heizlast
2. Three-Cornered-Hat: zufällige Messunsicherheit der Methoden A (FBH sek.), B (Fläche), C (Primär)
3. Mittlere Verhältnisse zwischen den Methoden (systematische Abweichung)

Ergebnisse: <base>/analyse/<name> (retained).   Nur anzeigen:  python3 analyse.py --print
"""
import argparse
import csv
import logging
import os
import time

import numpy as np

log = logging.getLogger("analyse")


def num(r, k):
    v = r.get(k)
    try:
        return float(v) if v not in (None, "", "None") else None
    except ValueError:
        return None


def lade(path, tage):
    if not os.path.exists(path):
        return []
    with open(path, encoding="utf-8") as f:
        rows = list(csv.DictReader(f))
    return rows[-tage:]


def tag_ok(r, a, key="min_abdeckung"):
    """Tag ohne Stromlücke und mit genügend Abdeckung der FBH-Messung."""
    cov = num(r, "abdeckung_fbh")
    return cov is not None and cov >= a[key] and not (num(r, "luecke_strom_kwh") or 0.0) > 0


def signatur(rows, a):
    pts = []
    for r in rows:
        ta, e, cov = num(r, "t_aussen_mittel"), num(r, "e_fbh_heizen_kwh"), num(r, "abdeckung_fbh")
        if None in (ta, e, cov) or not tag_ok(r, a):
            continue
        if e > 0.5 and ta < a["heizgrenze_max"]:
            pts.append((ta, e))
    res = {"signatur_n_tage": len(pts)}
    if len(pts) < 10:
        return res
    x, y = np.array(pts).T
    m, b = np.polyfit(x, y, 1)
    if m >= 0:
        log.warning("Signatur ohne negative Steigung – noch zu wenig Temperaturspreizung")
        return res
    y_fit = m * x + b
    r2 = 1 - np.sum((y - y_fit) ** 2) / np.sum((y - y.mean()) ** 2)
    h_w_k = -m / 24.0 * 1000.0
    res.update({
        "h_w_k": round(h_w_k, 1),
        "heizgrenze_c": round(-b / m, 1),
        "heizlast_kw": round((m * a["t_auslegung"] + b) / 24.0, 2),
        "jahres_heizwaerme_kwh": round(h_w_k * a["hgt"] * 24 / 1000.0),
        "signatur_r2": round(float(r2), 3),
    })
    return res


def three_cornered_hat(rows, a):
    keys = {"A": ("e_fbh_heizen_kwh", "abdeckung_fbh"),
            "B": ("e_flaeche_heizen_kwh", "abdeckung_flaeche"),
            "C": ("e_prim_heizen_kwh", "abdeckung_prim")}
    data = []
    for r in rows:
        if (num(r, "luecke_strom_kwh") or 0.0) > 0:
            continue
        vals = []
        for k, cov in keys.values():
            v, c = num(r, k), num(r, cov)
            if v is None or c is None or c < a["min_abdeckung"] or v < 0.5:
                break
            vals.append(v)
        else:
            data.append(vals)
    res = {"tch_n_tage": len(data)}
    if len(data) < 14:
        return res
    A, B, C = np.array(data).T
    v_ab, v_ac, v_bc = np.var(A - B, ddof=1), np.var(A - C, ddof=1), np.var(B - C, ddof=1)
    var = {"a": (v_ab + v_ac - v_bc) / 2, "b": (v_ab + v_bc - v_ac) / 2, "c": (v_ac + v_bc - v_ab) / 2}
    mean_a = A.mean()
    for k, v in var.items():
        s = float(np.sqrt(max(v, 0.0)))
        res[f"sigma_{k}_kwh"] = round(s, 3)
        res[f"sigma_{k}_prozent"] = round(100 * s / mean_a, 1)
    res["bias_a_b"] = round(float(np.mean(A / B)), 3)
    res["bias_a_c"] = round(float(np.mean(A / C)), 3)
    return res


def kennzahlen(rows, a):
    """Kennzahlen der letzten 7 Tage, nur gültige Tage (Abdeckung, keine Stromlücke).

    Arbeitszahlen energiegewichtet: Summe Wärme / Summe Strom, nicht Mittel der Tageswerte.
    """
    res = {}
    tage = [r for r in rows[-7:] if tag_ok(r, a, "min_abdeckung_kennzahlen")]
    res["kennzahlen_n_tage"] = len(tage)

    def summe(r, *keys):
        vals = [num(r, k) for k in keys]
        return None if None in vals else sum(vals)

    for name, waerme, strom in (
        ("az_system", ("nutzwaerme_kwh",), ("e_el_wp_kwh", "e_el_stab_kwh")),
        ("az_wp", ("e_wp_kwh",), ("e_el_wp_kwh",)),
    ):
        w = s = 0.0
        for r in tage:
            ww, ss = summe(r, *waerme), summe(r, *strom)
            if ww is not None and ss is not None and ss > 0.5:
                w, s = w + ww, s + ss
        if s > 0.5 and w > 0.5:
            res[f"{name}_7d"] = round(w / s, 3)
    # Netto ohne Begleitheizung
    w = s = 0.0
    for r in tage:
        nutz, el, bh = num(r, "nutzwaerme_kwh"), summe(r, "e_el_wp_kwh", "e_el_stab_kwh"), num(r, "e_el_bh_kwh")
        if None not in (nutz, el, bh) and el - bh > 0.5:
            w, s = w + nutz, s + el - bh
    if s > 0.5:
        res["az_wp_netto_7d"] = round(w / s, 3)
    for k in ("verlust_kwh", "e_ww_kwh", "e_ww_schaetz_kwh", "e_el_bh_kwh", "starts"):
        vals = [num(r, k) for r in tage]
        vals = [v for v in vals if v is not None]
        if vals:
            res[f"{k}_7d"] = round(sum(vals) / len(vals), 3)
    return res


def main():
    from common import load_config, setup_logging

    ap = argparse.ArgumentParser()
    ap.add_argument("--print", action="store_true", help="nur ausgeben, nicht publizieren")
    ap.add_argument("--csv", help="anderer Pfad zur tageswerte.csv")
    args = ap.parse_args()

    setup_logging()
    cfg = load_config()
    a = {"tage": 60, "min_abdeckung": 0.9, "min_abdeckung_kennzahlen": 0.95, "heizgrenze_max": 16.0, "t_auslegung": -8.0, "hgt": 3500}
    a.update(cfg.get("analyse") or {})
    path = args.csv or os.path.join(cfg["calc"].get("state_dir", "/var/lib/waermemonitor"),
                                    "tageswerte.csv")
    rows = lade(path, int(a["tage"]))
    res = {}
    res.update(signatur(rows, a))
    res.update(three_cornered_hat(rows, a))
    res.update(kennzahlen(rows, a))

    for k, v in res.items():
        print(f"{k:28s} {v}")
    if args.print:
        return

    from common import make_client
    base = cfg["mqtt"].get("base_topic", "heizung")
    client = make_client(cfg, "analyse")
    time.sleep(2)
    infos = [client.publish(f"{base}/analyse/{k}", str(v), qos=1, retain=True) for k, v in res.items()]
    for i in infos:
        i.wait_for_publish(timeout=10)
    client.publish(client.status_topic, "done", qos=1, retain=True).wait_for_publish(timeout=10)
    client.loop_stop()
    client.disconnect()


if __name__ == "__main__":
    main()
