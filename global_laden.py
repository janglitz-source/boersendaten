#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
global_laden.py – globale Erweiterung der Kursdatenbank (Heimatbörsen weltweit).

Das Skript ergänzt kurse_laden.py (Kernlisten Watchlist, Midcaps, S&P 500) und rührt
dessen Dateien nicht an. Es kennt zwei Arbeitsgänge:

  auslese  (einmal pro Woche je Region)
      Prüft für ALLE Kandidaten der Region aus universum_global.csv, ob sie handelbar sind:
        - Ø-Tagesumsatz der letzten 20 Tage mindestens 1 Mio. € (an der Heimatbörse)
        - Kurs-Gegenwert mindestens 1 €
        - Kurse aktuell (letzter Kurs höchstens 7 Tage alt), mindestens 40 Handelstage
      Für Werte, die schon im Bestand stehen (Kursdatei des vorigen Laufs, --vorher), rechnet
      das Skript aus dem Bestand und lädt nichts. Nur für die übrigen Kandidaten holt es
      drei Monate Kurse bei Yahoo.
      Ergebnis: aktiv_<region>.csv – nur diese Werte werden täglich geladen.

  kurse    (täglich je Region, nach Börsenschluss)
      Führt für die aktiven Werte der Region fünf Jahre Tageskurse. FORTLAUFEND: Mit --vorher
      holt das Skript bei Yahoo nur den letzten Monat und hängt die neuen Tage an den Bestand
      an. Voll nachgeladen (fünf Jahre) wird nur gezielt ein einzelner Wert: wenn er neu ist
      oder weniger als 250 Handelstage Bestand hat, wenn Bestand und neuer Abruf nicht zusammenpassen (Split, Korrektur, lange Pause) oder
      wenn der letzte Tag einen Kurssprung von mehr als 40 % zeigt. LÜCKENREGEL: Lässt Yahoo
      den jüngsten Tag weg, den der Bestand schon hat, bleibt er aus dem Bestand erhalten.
      (Die Regeln stehen in kurse_laden.py, Abschnitt 2b – beide Datenbanken nutzen denselben Code.)
      Danach entfernt das Skript unfertige Tageskerzen, rechnet die Kennzahlen und schreibt:
        kurse_<region>.csv.gz      datum,ticker,open,high,low,close,volume (Heimatwährung)
        kennzahlen_<region>.csv    eine Zeile je Wert, mit Name, Land, Sektor, ISIN, Umsatz in €
        status_<region>.json       Stand der Daten: letzter und erwarteter Handelstag,
                                   "stand_aktuell" und im Feld "abruf", wie geladen wurde

Regionen:  asien (Japan, Australien, Neuseeland) · europa · kanada · usa (Zusatzwerte)

Aufruf:
  python global_laden.py --modus auslese --region europa --out out --vorher vorher/kurse_europa.csv.gz
  python global_laden.py --modus kurse   --region europa --out out --aktiv out/aktiv_europa.csv \
                         --vorher vorher/kurse_europa.csv.gz
  Zusatz --voll: Bestand nicht nutzen, alles voll laden (nur zur Reparatur).
"""

import argparse
import csv
import gzip
import json
import os
import sys
import time
from datetime import datetime, time as uhrzeit, timedelta, timezone
from zoneinfo import ZoneInfo

import pandas as pd

import kurse_laden as kern   # Laden in Blöcken und Kennzahlen kommen aus dem Kernskript

# ----------------------------------------------------------------------------
# Einstellungen
# ----------------------------------------------------------------------------
REGIONEN = ("asien", "europa", "kanada", "usa")
MIN_UMSATZ_MIO_EUR = 1.0     # Ø-Tagesumsatz in Mio. € (Jans Schwelle für Nebenwerte, 18.11.2026)
MIN_KURS_EUR = 1.0           # Kurs-Gegenwert in € (keine Pennystocks)
MIN_TAGE_AUSLESE = 40        # so viele Handelstage muss ein Wert in drei Monaten haben
MAX_ALTER_TAGE = 7           # der letzte Kurs darf höchstens so viele Tage alt sein
BLOCK = 50                   # Werte je Abruf
PAUSE = 4                    # Sekunden Pause zwischen zwei Blöcken (schont Yahoo)
TAGE_AUSLESE = 70            # so viele letzte Handelstage aus dem Bestand nimmt die Auslese (rund drei Monate)

# Wechselkurse bei Yahoo: 1 Euro = x Einheiten der Währung
FX_SYMBOLE = {
    "USD": "EURUSD=X", "GBP": "EURGBP=X", "JPY": "EURJPY=X", "AUD": "EURAUD=X",
    "NZD": "EURNZD=X", "CAD": "EURCAD=X", "CHF": "EURCHF=X", "SEK": "EURSEK=X",
    "DKK": "EURDKK=X", "NOK": "EURNOK=X",
}

# Börsenschluss je Börsenkürzel: (Zeitzone, Uhrzeit vor Ort inklusive Sicherheitszuschlag).
# Vor dieser Zeit gilt die Kerze des laufenden Tages als unfertig und wird entfernt.
SCHLUSS = {
    ".T": ("Asia/Tokyo", uhrzeit(15, 50)), ".AX": ("Australia/Sydney", uhrzeit(16, 30)),
    ".L": ("Europe/London", uhrzeit(16, 55)), ".TO": ("America/Toronto", uhrzeit(16, 20)),
    "": ("America/New_York", uhrzeit(16, 20)),
}
SCHLUSS_EUROPA = ("Europe/Berlin", uhrzeit(18, 0))   # alle übrigen europäischen Börsen

# Leitbörse je Region: an ihr wird gemessen, welcher Handelstag in den Daten stehen müsste.
LEITBOERSE = {"asien": ".T", "europa": ".DE", "kanada": ".TO", "usa": ""}


# ----------------------------------------------------------------------------
# Hilfen
# ----------------------------------------------------------------------------
def suffix(symbol):
    """Börsenkürzel am Ende des Yahoo-Symbols, z. B. '.DE' bei SAP.DE; '' bei US-Werten."""
    return "." + symbol.rsplit(".", 1)[1] if "." in symbol else ""


def lies_universum(pfad, region):
    """Liest die Werteliste (universum_global.csv oder aktiv_<region>.csv) für eine Region."""
    with open(pfad, encoding="utf-8", newline="") as f:
        return [z for z in csv.DictReader(f) if z.get("region") == region and z.get("symbol")]


def lade(symbole, zeitraum, lader=None):
    """Lädt Kurse in Blöcken über das Kernskript. Gibt {symbol: Tabelle} zurück."""
    kern.ZEITRAUM = zeitraum
    kern.BLOCKGROESSE = BLOCK
    kern.PAUSE_SEKUNDEN = PAUSE
    return kern.lade_alle(symbole, lader=lader or kern.lade_block)


def lade_mit_bestand(symbole, bestand, lader=None, jetzt_utc=None):
    """Fortlaufendes Laden über das Kernskript: mit Bestand nur der letzte Monat,
    volles Nachladen nur gezielt. Gibt (kurse, bericht) zurück."""
    kern.ZEITRAUM = "5y"
    kern.BLOCKGROESSE = BLOCK
    kern.PAUSE_SEKUNDEN = PAUSE
    return kern.lade_fortlaufend(symbole, bestand, lader=lader or kern.lade_block, jetzt_utc=jetzt_utc)


def wechselkurse(lader=None):
    """Holt die aktuellen Wechselkurse (1 € = x Währung). EUR ist immer 1."""
    kurse = {"EUR": 1.0}
    daten = lade(list(FX_SYMBOLE.values()), "1mo", lader=lader)
    for waehrung, symbol in FX_SYMBOLE.items():
        tabelle = daten.get(symbol)
        if tabelle is not None and not tabelle.empty:
            kurse[waehrung] = float(tabelle["close"].iloc[-1])
    return kurse


def in_euro(betrag, symbol, waehrung, fx):
    """Rechnet einen Betrag in Heimatwährung in Euro um.
    Londoner Kurse stehen bei Yahoo in Pence – deshalb durch 100."""
    waehrung = (waehrung or "").strip()
    if suffix(symbol) == ".L" and waehrung in ("GBP", "GBp", "GBX", ""):
        return betrag / 100.0 / fx.get("GBP", float("nan"))
    if waehrung not in fx:
        return float("nan")
    return betrag / fx[waehrung]


def entferne_unfertige_tage(kurse, jetzt_utc=None):
    """Entfernt je Börse die Kerze des laufenden Handelstags, solange die Börse noch offen ist."""
    jetzt_utc = jetzt_utc or datetime.now(timezone.utc)
    for symbol, tabelle in list(kurse.items()):
        zone, schluss = SCHLUSS.get(suffix(symbol), SCHLUSS_EUROPA)
        ort = jetzt_utc.astimezone(ZoneInfo(zone))
        if ort.weekday() < 5 and ort.time() < schluss:
            kurse[symbol] = tabelle[tabelle.index < pd.Timestamp(ort.date())]
    return kurse


def erwarteter_handelstag(region, jetzt_utc=None):
    """Letzter Wochentag, an dem die Leitbörse der Region schon geschlossen hat.
    Feiertage kennt die Funktion nicht – an und nach Feiertagen nennt sie einen Tag zu viel."""
    jetzt_utc = jetzt_utc or datetime.now(timezone.utc)
    zone, schluss = SCHLUSS.get(LEITBOERSE[region], SCHLUSS_EUROPA)
    ort = jetzt_utc.astimezone(ZoneInfo(zone))
    tag = ort.date()
    if ort.weekday() >= 5 or ort.time() < schluss:   # Wochenende oder Börse noch offen: einen Tag zurück
        tag -= timedelta(days=1)
    while tag.weekday() >= 5:                        # Samstag/Sonntag überspringen
        tag -= timedelta(days=1)
    return pd.Timestamp(tag)


# ----------------------------------------------------------------------------
# Arbeitsgang 1: wöchentliche Auslese
# ----------------------------------------------------------------------------
def auslese(region, universum, ausgabe, lader=None, jetzt_utc=None, vorher=None):
    kandidaten = lies_universum(universum, region)
    print(f"Auslese {region}: {len(kandidaten)} Kandidaten", flush=True)
    fx = wechselkurse(lader)
    heute = pd.Timestamp((jetzt_utc or datetime.now(timezone.utc)).date())
    # Werte mit frischem Bestand werden aus dem Bestand beurteilt – ohne Abruf bei Yahoo.
    bestand = kern.lies_bestand(vorher)
    kurse = {}
    for k in kandidaten:
        t = bestand.get(k["symbol"])
        if t is not None and not t.empty and (heute - t.index[-1]).days <= MAX_ALTER_TAGE:
            kurse[k["symbol"]] = t.iloc[-TAGE_AUSLESE:]
    aus_bestand = len(kurse)
    rest = [k["symbol"] for k in kandidaten if k["symbol"] not in kurse]
    print(f"Aus dem Bestand: {aus_bestand} Werte · bei Yahoo zu laden: {len(rest)} Werte", flush=True)
    if rest:
        kurse.update(lade(rest, "3mo", lader=lader))
    kurse = entferne_unfertige_tage(kurse, jetzt_utc)

    aktiv, gruende = [], {"keine Daten": 0, "veraltet": 0, "zu wenig Tage": 0, "Umsatz": 0, "Kurs": 0, "Währung": 0}
    for k in kandidaten:
        t = kurse.get(k["symbol"])
        if t is None or t.empty:
            gruende["keine Daten"] += 1; continue
        if (heute - t.index[-1]).days > MAX_ALTER_TAGE:
            gruende["veraltet"] += 1; continue
        if len(t) < MIN_TAGE_AUSLESE:
            gruende["zu wenig Tage"] += 1; continue
        letzte = t.iloc[-20:]
        umsatz = in_euro(float((letzte["close"] * letzte["volume"]).mean()), k["symbol"], k["waehrung"], fx) / 1e6
        kurs = in_euro(float(t["close"].iloc[-1]), k["symbol"], k["waehrung"], fx)
        if umsatz != umsatz or kurs != kurs:          # NaN: Währung unbekannt
            gruende["Währung"] += 1; continue
        if umsatz < MIN_UMSATZ_MIO_EUR:
            gruende["Umsatz"] += 1; continue
        if kurs < MIN_KURS_EUR:
            gruende["Kurs"] += 1; continue
        zeile = dict(k); zeile["umsatz20_mio_eur"] = round(umsatz, 2)
        aktiv.append(zeile)

    os.makedirs(ausgabe, exist_ok=True)
    felder = ["symbol", "name", "region", "land", "sektor", "groesse", "waehrung", "isin", "umsatz20_mio_eur"]
    with open(os.path.join(ausgabe, f"aktiv_{region}.csv"), "w", encoding="utf-8", newline="") as f:
        schreiber = csv.DictWriter(f, fieldnames=felder, extrasaction="ignore")
        schreiber.writeheader(); schreiber.writerows(aktiv)
    bericht = {"region": region, "erstellt_utc": datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M"),
               "kandidaten": len(kandidaten), "aktiv": len(aktiv), "ausgeschieden": gruende,
               "aus_bestand_beurteilt": aus_bestand, "bei_yahoo_geladen": len(rest),
               "schwellen": {"umsatz_mio_eur": MIN_UMSATZ_MIO_EUR, "kurs_eur": MIN_KURS_EUR}, "wechselkurse": fx}
    with open(os.path.join(ausgabe, f"auslese_{region}.json"), "w", encoding="utf-8") as f:
        json.dump(bericht, f, ensure_ascii=False, indent=2)
    print(json.dumps({k: v for k, v in bericht.items() if k != "wechselkurse"}, ensure_ascii=False), flush=True)
    # Fehler nur, wenn Yahoo praktisch nichts geliefert hat – dann bleibt die alte Liste stehen.
    return 0 if len(aktiv) >= 0.05 * max(1, len(kandidaten)) else 1


# ----------------------------------------------------------------------------
# Arbeitsgang 2: tägliche Kurse und Kennzahlen
# ----------------------------------------------------------------------------
def kurse_region(region, liste, ausgabe, lader=None, jetzt_utc=None, vorher=None, voll=False):
    werte = lies_universum(liste, region)
    symbole = [w["symbol"] for w in werte]
    print(f"Kurse {region}: {len(werte)} Werte", flush=True)
    fx = wechselkurse(lader)
    # Fortlaufend laden: Bestand lesen, nur den letzten Monat holen, gezielt nachladen.
    # Geht dabei irgendetwas schief, lädt das Skript wie früher alles voll.
    bestand = {} if voll else kern.lies_bestand(vorher)
    try:
        kurse, abruf = lade_mit_bestand(symbole, bestand, lader=lader, jetzt_utc=jetzt_utc)
        if voll:
            abruf["art"] = "voll (erzwungen)"
    except Exception as fehler:
        print(f"Fortlaufendes Laden gescheitert ({fehler}) – lade alles voll.", flush=True)
        kurse = lade(symbole, "5y", lader=lader)
        abruf = {"art": "voll (Notweg nach Fehler)", "kurz_geladen": 0, "voll_nachgeladen": len(symbole)}
    # Erst nach dem Zusammenführen die unfertigen Tageskerzen entfernen
    # (die Regel gilt so auch für Zeilen aus dem Bestand).
    kurse = entferne_unfertige_tage(kurse, jetzt_utc)

    letzte = [t.index[-1] for t in kurse.values() if not t.empty]
    bezugstag = pd.Series(letzte).mode().max() if letzte else None   # häufigster letzter Handelstag

    os.makedirs(ausgabe, exist_ok=True)
    kennzahlen, aktuell, veraltet, fehlend = [], 0, [], []
    with gzip.open(os.path.join(ausgabe, f"kurse_{region}.csv.gz"), "wt", encoding="utf-8", newline="") as f:
        f.write("datum,ticker,open,high,low,close,volume\n")
        for w in werte:
            symbol = w["symbol"]
            t = kurse.get(symbol)
            if t is None or t.empty:
                fehlend.append(symbol); continue
            block = t[["open", "high", "low", "close"]].astype(float).round(4)
            block.insert(0, "ticker", symbol)
            block.insert(0, "datum", t.index.strftime("%Y-%m-%d"))
            block["volume"] = t["volume"].fillna(0).astype("int64")
            block.to_csv(f, header=False, index=False, lineterminator="\n")

            k = {"ticker": symbol, "name": w.get("name", ""), "land": w.get("land", ""), "sektor": w.get("sektor", ""),
                 "groesse": w.get("groesse", ""), "waehrung": w.get("waehrung", ""), "isin": w.get("isin", "")}
            k.update(kern.kennzahlen_fuer(t))
            # Umsatz in Euro (das Kernskript rechnet in Heimatwährung) und Mindestorder für Japan
            umsatz = k.get("vol20_stueck") and k["vol20_stueck"] * k["close"]
            k["umsatz20_mio_eur"] = round(in_euro(umsatz, symbol, w.get("waehrung"), fx) / 1e6, 2) if umsatz else None
            k["kurs_eur"] = round(in_euro(k["close"], symbol, w.get("waehrung"), fx), 4)
            k["mindeststueck"] = 100 if suffix(symbol) == ".T" else 1      # Tokio handelt in 100er-Einheiten
            k["mindestorder_eur"] = round(k["kurs_eur"] * k["mindeststueck"], 2)
            k.pop("umsatz20_mio", None)                                     # wäre in Heimatwährung – irreführend
            kennzahlen.append(k)
            if t.index[-1] == bezugstag: aktuell += 1
            else: veraltet.append(symbol)

    if kennzahlen:
        with open(os.path.join(ausgabe, f"kennzahlen_{region}.csv"), "w", encoding="utf-8", newline="") as f:
            schreiber = csv.DictWriter(f, fieldnames=list(kennzahlen[0].keys()))
            schreiber.writeheader(); schreiber.writerows(kennzahlen)

    erwartet = erwarteter_handelstag(region, jetzt_utc)
    status = {"region": region, "erstellt_utc": datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M"),
              "letzter_handelstag": bezugstag.strftime("%Y-%m-%d") if bezugstag is not None else None,
              # Prüfung für alle, die die Daten nutzen: Steht der erwartete Tag in den Daten?
              "erwarteter_handelstag": erwartet.strftime("%Y-%m-%d"),
              "stand_aktuell": bool(bezugstag is not None and bezugstag >= erwartet),
              "abruf": abruf,                       # wie geladen wurde (fortlaufend oder voll, Gründe fürs Nachladen)
              "werte_gesamt": len(werte), "werte_aktuell": aktuell, "werte_veraltet": len(veraltet),
              "veraltet_beispiele": sorted(veraltet)[:40], "fehlend": len(fehlend), "fehlend_beispiele": sorted(fehlend)[:40],
              "wechselkurse": fx, "quelle": "Yahoo Finance über yfinance",
              "hinweis": "Kurse in Heimatwährung (London in Pence), um Splits bereinigt, nicht um Dividenden. "
                         "Feiertage einzelner Länder führen zu einzelnen veralteten Werten. "
                         "stand_aktuell = false heißt: Der erwartete Handelstag fehlt (Feiertag der Leitbörse "
                         "oder Lücke bei Yahoo) – Daten dann nur mit Gegenprobe verwenden."}
    with open(os.path.join(ausgabe, f"status_{region}.json"), "w", encoding="utf-8") as f:
        json.dump(status, f, ensure_ascii=False, indent=2)
    print(json.dumps({k: v for k, v in status.items() if k not in ("hinweis", "wechselkurse")}, ensure_ascii=False), flush=True)
    # Fehler, wenn weniger als die Hälfte der Werte Daten hat – dann wird nichts veröffentlicht.
    return 0 if len(kennzahlen) >= 0.5 * max(1, len(werte)) else 1


# ----------------------------------------------------------------------------
# Hauptprogramm
# ----------------------------------------------------------------------------
def main(argumente=None, lader=None, jetzt_utc=None):
    p = argparse.ArgumentParser(description="Globale Kursdatenbank: Auslese und Kurse je Region")
    p.add_argument("--modus", choices=["auslese", "kurse"], required=True)
    p.add_argument("--region", choices=REGIONEN, required=True)
    p.add_argument("--universum", default="universum_global.csv", help="Kandidatenliste (für die Auslese)")
    p.add_argument("--aktiv", default=None, help="Liste der aktiven Werte (für die Kurse); fehlt sie, gilt die Kandidatenliste")
    p.add_argument("--vorher", default=None, help="Kursdatei des vorigen Laufs (Bestand) für das fortlaufende Laden")
    p.add_argument("--voll", action="store_true", help="Bestand nicht nutzen, alles voll laden")
    p.add_argument("--out", default="out", help="Ausgabeordner")
    o = p.parse_args(argumente)
    if o.modus == "auslese":
        return auslese(o.region, o.universum, o.out, lader=lader, jetzt_utc=jetzt_utc, vorher=o.vorher)
    liste = o.aktiv if o.aktiv and os.path.exists(o.aktiv) else o.universum
    if liste == o.universum:
        print("Hinweis: keine Auslese vorhanden – lade die ganze Kandidatenliste der Region.", flush=True)
    return kurse_region(o.region, liste, o.out, lader=lader, jetzt_utc=jetzt_utc, vorher=o.vorher, voll=o.voll)


if __name__ == "__main__":
    sys.exit(main())
