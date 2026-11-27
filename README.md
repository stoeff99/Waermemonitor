# Wärmemonitor – Sole/Wasser-WP mit Kombispeicher und FBH

Ein Rechendienst, der alle Eingänge vom MQTT-Broker **192.168.20.4** liest, also die
CAN-Werte deines CAN-CT und die Fühler und Zähler aus Loxone. Er rechnet Wärmeleistungen,
Energien und Tageswerte und publiziert die Ergebnisse wieder auf dem Broker. Telegraf
schreibt sie nach InfluxDB, Grafana zeigt sie an.

```
 CAN-CT (can_scan) ──┐                              ┌── Telegraf ── InfluxDB ── Grafana
                      ├──► MQTT 192.168.20.4 ◄──────┤
 Loxone ──────────────┘          ▲    │             └── OpenEMS / Loxone (Werte nutzen)
                                 │    ▼
                          waerme_calc.py ──► tageswerte.csv ──► analyse.py (täglich 00:30)
```

## Dateien

| Datei | Zweck |
|---|---|
| `config.yaml` | **Die einzige Datei, die du anpasst** (Topics, Durchflüsse, Speicher, Räume) |
| `waerme_calc.py` | Dauerdienst: Leistungen, Energiezähler, Speicherinhalt, Tageswerte, Diagnose |
| `analyse.py` | täglich: Energiesignatur, Heizlast, Three-Cornered-Hat, 7-Tage-Kennzahlen |
| `telegraf/heizung.conf` | MQTT → InfluxDB |
| `systemd/` | Dienst und Timer |
| `install.sh` | Installation nach `/opt/waermemonitor`, Dienstbenutzer `waerme` |

## Installation (in einem beliebigen CT, Ubuntu/Debian)

```bash
sudo ./install.sh
sudo nano /opt/waermemonitor/config.yaml        # Topics eintragen (siehe unten)
sudo systemctl enable --now waerme-calc waerme-analyse.timer
journalctl -fu waerme-calc                       # meldet fehlende Eingänge
```

Nach Änderungen an der Config: `sudo systemctl restart waerme-calc`.

Telegraf: `telegraf/heizung.conf` nach `/etc/telegraf/telegraf.d/` kopieren. Im Output trägst du
`INFLUX_HOST`, `token`, `organization` und `bucket` ein, für InfluxDB 1.x nimmst du den
auskommentierten Block. Prüfen mit
`telegraf --config /etc/telegraf/telegraf.conf --config-directory /etc/telegraf/telegraf.d --test --test-wait 60`.
Andere Outputs in der Haupt-`telegraf.conf` bekommen die `heizung_*`-Werte sonst auch. Dort
`namedrop = ["heizung_*"]` ergänzen, damit sie nur im Wärmemonitor-Bucket landen.

## Eingänge zuordnen (`calc.topics`)

Unter `calc.topics` stehen Platzhalter. Trag dort die Topics ein, die dein CAN-CT und Loxone
tatsächlich publizieren. Welche das sind, siehst du mit:
```bash
mosquitto_sub -h 192.168.20.4 -t '#' -v
```

Folgende Payload-Formate werden verstanden:

| Payload | Eintrag in config.yaml |
|---|---|
| `31.4`, `on`/`off`, `true`/`false` | `fbh_vl: haus/fbh/vorlauf` |
| `{"value": 31.4, …}` | `fbh_vl: haus/fbh/vorlauf` |
| `{"temp": 31.4, …}` | `fbh_vl: haus/fbh/vorlauf#temp` |
| `{"data": {"vl": 31.4}}` | `fbh_vl: haus/fbh#data.vl` |

Mehrere Werte aus demselben JSON-Topic sind möglich, z. B. `haus/fbh#vl` und `haus/fbh#rl`.

| Eingang | Inhalt | Für |
|---|---|---|
| `t_aussen` | Aussentemperatur (CAN 180.000c) | Tagesmittel, Signatur |
| `fbh_vl`, `fbh_rl`, `fbh_pumpe` | FBH sekundär Vorlauf/Rücklauf °C, Pumpe 0/1 | **Methode A** |
| `prim_vl`, `prim_rl`, `prim_pumpe` | Primärseite Wärmetauscher | Kontrolle |
| `wp_vl`, `wp_rl`, `wp_pumpe` | WP-Vorlauf/-Rücklauf (CAN 180.01d6 / 180.0016), Ladepumpe (180.fdad) | Methode C, nur mit `durchfluss_lh.wp` |
| `wp_ww_aktiv` | 0/1 Warmwasser-Ladung (Relaisbit, sobald bekannt) | Aufteilung WP Heizen/WW |
| `wp_el_kwh`, `stab_el_kwh` | Stromzähler WP / Elektroeinsatz, **Zählerstand** kWh | Arbeitszahl |
| `ww_liter`, `ww_t_misch`, `ww_t_kalt` | Wasserzähler **Zählerstand** Liter, Mischwasser, Kaltwasser | Warmwasser-Energie |
| `speicher.zonen[]` | Speicherfühler mit Volumen pro Zone, optional `offset_k` | Methode D |
| `flaeche.raeume[]` | Boden- und Raumtemperatur pro Raum | Methode B |

Ein Eingang, der fehlt oder auf `null` steht, schadet nicht. Die zugehörige Methode rechnet dann
einfach nicht. Die Quellen dürfen **nur bei Änderung** senden: ein Wert gilt, bis ein neuer kommt. Als
fehlend zählt ein Eingang nur, wenn noch nie ein Wert empfangen wurde. Die letzten Werte
werden in `state.json` gespeichert, ein Neustart verliert sie also nicht. `heizung/diag/alter_<eingang>`
zeigt trotzdem, wie alt der letzte Wert ist. Für Quellen, die periodisch senden, kannst du mit
`max_age_s` bzw. `max_age_pro_eingang` ein Höchstalter als Watchdog setzen.

**Eingefrorene Werte** erkennt der Dienst trotzdem: Läuft eine Pumpe, ändern sich Vor- oder
Rücklauf aber seit `eingefroren_s` (900 s) nicht mehr, gilt der Kreis als ungültig. Das greift
auch, wenn eine Quelle denselben Haltewert immer wieder sendet. Dasselbe gilt für die
Momentanleistung `wp_el_w`, wenn sie über der Standby-Schwelle stehen bleibt.

## Stromaufteilung, Begleitheizung, Warmwasser

Mit `topics.wp_el_w` (Momentanleistung in W, derselbe Zähler wie `wp_el_kwh`) teilt der Dienst den
Strom auf. Jede Phase über `p_start_min_w` (150 W) wird nach 30 s eingeordnet:

| Klasse | Erkennung | Zähler |
|---|---|---|
| Begleitheizung | nach 30 s unter 900 W | `e_el_bh` |
| Verdichter Heizen | P ≥ 1000 W, WP-Vorlauf ≤ 40 °C | `e_el_heiz`, `laufzeit_heiz_h` |
| Verdichter WW | P ≥ 1000 W, WP-Vorlauf > 40 °C | `e_el_ww`, `laufzeit_ww_h` |
| Rest | Standby, Pumpen-Vor-/Nachlauf | `e_el_rest_kwh` = Zählerstand minus Klassen |

Steigt P in einer Begleitheizungsphase auf 1500 W, ist es ein Verdichterstart (`starts`). Geschieht
das innerhalb von 2 min, war es der Pumpen-Vorlauf und zählt nicht als Begleitheizung. Während
eines Verdichterlaufs ist die Begleitheizung nicht erkennbar, dafür gibt es `zuschlag_bh_kwh_tag`.

Solange kein Warmwasserzähler liefert, schätzt der Dienst die WW-Wärme als WW-Strom ×
`cop_ww_schaetzung` (`e_ww_schaetz_kwh`). Die Nutzwärme nimmt den grösseren der beiden Werte.

Optional rechnet **Methode E** (`kennlinie_wp`) die Heizwärme aus Laufzeit × Heizleistung, abhängig
von der Soletemperatur `t_quelle`. Das ist ein Gegencheck ohne Durchflussmessung.

**FBH-Stellantriebe** (`fbh_flow.stellantriebe`): Bei ungeregelter Pumpe hängt der Durchfluss an der
Ventilöffnung. Unter `min_oeffnung_pct` (30 %) im Mittel zählt die FBH-Wärme als
`e_fbh_unsicher` und nicht in Bilanz und Abdeckung. Mit einer gemessenen `kennlinie` wird der
Durchfluss aus der Öffnung interpoliert.

## Was publiziert wird

| Topic | Inhalt |
|---|---|
| `heizung/calc/p_fbh_kw` | Leistung FBH sekundär (A), **Kühlen negativ** |
| `heizung/calc/p_prim_kw` | Leistung Primärseite (Kontrolle) |
| `heizung/calc/p_flaeche_kw` | Leistung nach Flächenmethode (B) |
| `heizung/calc/p_wp_kw` | Leistung WP-Seite (C) |
| `heizung/calc/e_speicher_kwh` | Energieinhalt des Speichers über `t_ref` |
| `heizung/calc/e_*_kwh` | fortlaufende Zählerstände, retained |
| `heizung/calc/laufzeit_heiz_h`, `laufzeit_ww_h`, `starts` | Betriebszähler, retained |
| `heizung/heute/<feld>` | laufender Tag bis jetzt, gleiche Felder wie die Tageswerte (z.B. `az_system`, `nutzwaerme_kwh`, `e_el_wp_kwh`) |
| `heizung/diag/methode_<m>` | 1 = Methode rechnet gerade, 0 = Eingänge fehlen |
| `heizung/diag/alter_<eingang>` | Alter des letzten Werts in s (−1 = noch nie empfangen) |
| `heizung/diag/stromklasse` | 0 Standby, 1 Phase noch offen, 2 Begleitheizung, 3 Verdichter Heizen, 4 Verdichter WW |
| `heizung/diag/fbh_oeffnung_pct`, `fbh_unsicher` | mittlere Stellantrieb-Öffnung, 1 = FBH-Wärme unsicher |
| `heizung/diag/fehlende_eingaenge` | Anzahl fehlender Eingänge (nie empfangen bzw. Watchdog abgelaufen) |
| `heizung/status/waerme_calc_fehlend` | JSON-Liste der fehlenden Eingänge, retained |
| `heizung/tag/json` | Tageswerte, ein JSON pro Tag kurz nach Mitternacht |
| `heizung/analyse/*` | Signatur, Heizlast, Sigma pro Methode, 7-Tage-Mittel |
| `heizung/status/waerme_calc` | `online`/`offline` |

**Tageswerte** (`heizung_tag` in InfluxDB, Zeitstempel = 00:00 des betreffenden Tages): Energie
pro Zähler, `t_aussen_mittel`, `de_speicher_kwh`, `nutzwaerme_kwh`, `az_system` (Nutzwärme
inklusive Speicheränderung geteilt durch Strom), `az_wp`, `verlust_kwh` (nur mit WP-Durchfluss),
`verhaeltnis_fbh_flaeche`, `verhaeltnis_fbh_prim` und `abdeckung_*`. Die Abdeckung ist der Anteil
des Tages mit gültigen Daten. Tage mit weniger als 0.9 ignoriert die Analyse.

Mit Stromaufteilung zusätzlich: `e_el_bh_kwh`, `e_el_heiz_kwh`, `e_el_ww_kwh`, `e_el_rest_kwh`,
`laufzeit_heiz_h`, `laufzeit_ww_h`, `starts`, `anteil_bh`, `anteil_ww_el`, `e_ww_schaetz_kwh` und
**`az_wp_netto`** (Nutzwärme durch Strom ohne Begleitheizung, die empfohlene Hauptkennzahl).
Mit `kennlinie_wp` auch `e_kennlinie_heiz_kwh` und `az_kennlinie`.

`luecke_strom_kwh` > 0 heisst: Der Stromzähler ist auf einmal um mehr als `luecke_strom_kwh`
(1 kWh) gesprungen, eine Lücke wurde nachgeholt. `analyse.py` lässt solche Tage weg und rechnet
die 7-Tage-Arbeitszahlen energiegewichtet (Summe Wärme durch Summe Strom) nur aus Tagen mit
`abdeckung_fbh` ≥ 0.95. Kommen neue Spalten dazu, stellt der Dienst `tageswerte.csv` einmalig um.

Die Zählerstände liegen in `/var/lib/waermemonitor/state.json` und überstehen einen Neustart. Die
Historie für die Analyse steht in `tageswerte.csv`.

**Für Grafana** eignen sich als Status-Panel `heizung_diag.methode_*` (State timeline) und
`fehlende_eingaenge` (Stat, rot bei > 0).

## Kalibrieren und erste Schritte

1. **Pumpensignal prüfen.** Ohne gültiges Pumpensignal wird nichts integriert, weil stehendes
   Wasser sonst Energie vortäuschen würde. Das Signal muss zwischen 0 und 1 wechseln.
2. **Fühler-Offsets bestimmen.** Bei laufender FBH-Pumpe ohne Wärmezufuhr ist die gemessene ΔT
   dein Offset. Trag ihn unter `offsets_k` ein.
3. **Relaisbits zuordnen.** Leg die Relaisbits ein paar Tage in Grafana neben den WP-Vorlauf und die
   Warmwassertemperatur. Das Bit für die Warmwasser-Ladung trägst du als `wp_ww_aktiv` ein.
4. **Speichervolumen verteilen.** Die Platzhalter sind 260 l Boiler und 710 l Puffer. Verteile das
   Volumen nach den tatsächlichen Fühlerhöhen. Fehlt für eine Zone ein Fühler, nimmst du einen
   anderen mit `offset_k` als Ersatz, z. B. `boiler_unten` mit `offset_k: -2.0` für den Pufferboden.
5. **Durchfluss FBH.** 625 l/h ist der Mittelwert aus Pumpe und Ventilen. Nach einer Referenzmessung
   trägst du dort den gemessenen Wert ein.
6. **Uhr des WP-Reglers stellen.** Sie stand beim Scan auf dem 01.01.2011.

## Grenzen

- Getestet sind die Rechenlogik mit Tagesabschluss, Kühlbetrieb, Pumpe aus, veralteten Werten,
  JSON-Payloads und Neustart, die Analyse mit synthetischen Daten und der MQTT-Ablauf über einen
  Test-Broker. **Die Telegraf-Konfiguration ist nicht getestet.** Prüfe sie mit `--test`.
- Die Flächenmethode misst nur die Wärmeabgabe nach oben. Ein Verhältnis FBH/Fläche von 1.05–1.15 ist
  daher normal und entspricht den Verlusten nach unten. Die Three-Cornered-Hat-Auswertung zeigt nur
  zufällige Fehler, keine systematischen.
