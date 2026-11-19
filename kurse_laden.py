#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
kurse_laden.py – lädt Tageskurse für Jans Papierdepot und legt sie als Dateien ab.

Was das Skript macht:
  1. Es liest die Tickerliste aus tickers.csv (Spalten: ticker, gruppe).
  2. Es führt für jeden Wert fünf Jahre Tageskurse von Yahoo Finance (über die Bibliothek
     yfinance): Eröffnung, Hoch, Tief, Schluss, Umsatz in Stück.
     Die Kurse sind um Aktiensplits bereinigt, aber NICHT um Dividenden – so passen
     sie zu den Kursen im Chart und zu den Vergleichskursen von ariva.
     FORTLAUFENDES LADEN (seit 19.11.2026): Mit --vorher bekommt das Skript die Kursdatei
     des vorigen Laufs (den Bestand). Dann holt es bei Yahoo nur noch den letzten Monat
     und hängt die neuen Tage an den Bestand an. Voll nachgeladen (fünf Jahre) wird nur
     gezielt ein einzelner Wert, wenn
        - er neu in der Liste ist (kein Bestand),
        - sein Bestand weniger als 250 Handelstage hat (junge Aktie oder Yahoo hatte die
          Historie zeitweise gekappt – der volle Abruf ist dann ohnehin klein),
        - Bestand und neuer Abruf keinen gemeinsamen Tag haben (lange Pause),
        - die Kurse an den gemeinsamen Tagen nicht zusammenpassen (Split oder Korrektur
          bei Yahoo – dann stimmt die alte Historie nicht mehr),
        - der letzte Tag einen Kurssprung von mehr als 40 % zeigt (Gegenprobe).
     LÜCKENREGEL: Lässt Yahoo den jüngsten Tag weg, den der Bestand schon hat, bleibt
     dieser Tag aus dem Bestand erhalten.
     Ohne Bestand (erster Lauf, Datei nicht lesbar) oder mit --voll lädt es alles voll.
  3. Es entfernt unfertige Tageskerzen (laufender Handelstag).
  4. Es rechnet je Wert die Kennzahlen für den Grobfilter (Durchschnitte, RSI, ATR,
     52-Wochen-Marken, Renditen) – immer mit derselben Formel.
  5. Es schreibt vier Dateien in den Ausgabeordner:
        kurse.csv.gz      alle Tageskurse (datum,ticker,open,high,low,close,volume)
        kennzahlen.csv    eine Zeile je Wert mit den Kennzahlen
        letzte_kurse.csv  eine Zeile je Wert: letzter Handelstag und Schlusskurs
        status.json       Stand der Daten (letzter Handelstag, Anzahl, fehlende Werte)
  6. Sind weniger als 90 % der Listenwerte (Watchlist und Midcaps) aktuell, endet das
     Skript mit einem Fehler.
     Dann veröffentlicht der Arbeitsablauf nichts, und der alte Stand bleibt stehen.

Aufruf:  python kurse_laden.py --tickers tickers.csv --out out --vorher vorher/kurse.csv.gz
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

# ----------------------------------------------------------------------------
# Einstellungen
# ----------------------------------------------------------------------------
ZEITRAUM = "5y"          # so viel Kurshistorie wird geführt (rund 1.250 Handelstage,
                         # genug für 60 Monatskerzen in der Chartanalyse)
KURZ_ZEITRAUM = "1mo"    # so viel wird täglich bei Yahoo geholt, wenn ein Bestand da ist
JAHRE_BESTAND = 5        # ältere Tage fallen aus dem Bestand heraus (die Datei wächst nicht)
TOLERANZ = 0.005         # Bestand und neuer Abruf müssen an gemeinsamen Tagen auf 0,5 % passen
PRUEFTAGE = 5            # so viele (älteste) gemeinsame Tage werden verglichen
SPRUNG = 0.40            # Tagesveränderung, ab der ein Wert zur Gegenprobe voll nachgeladen wird
MIN_TAGE_BESTAND = 250   # Werte mit kürzerem Bestand werden jedes Mal voll geladen (kleiner Abruf)
BLOCKGROESSE = 50        # so viele Werte je Abruf bei Yahoo
PAUSE_SEKUNDEN = 3       # Pause zwischen zwei Blöcken (schont die Quelle)
VERSUCHE = 3             # so oft wird ein gescheiterter Block wiederholt
MINDESTANTEIL = 0.90     # mindestens so viele Listenwerte müssen aktuell sein
KERNGRUPPEN = ("Watchlist", "Midcaps")   # Listen des Papierdepots; nur sie zählen für den Abbruch
SPALTEN = ["open", "high", "low", "close", "volume"]


# ----------------------------------------------------------------------------
# 1. Tickerliste lesen
# ----------------------------------------------------------------------------
def lies_tickers(pfad):
    """Liest tickers.csv und gibt eine Liste von (ticker, gruppe) zurück."""
    werte = []
    with open(pfad, encoding="utf-8", newline="") as f:
        for zeile in csv.DictReader(f):
            ticker = (zeile.get("ticker") or "").strip()
            gruppe = (zeile.get("gruppe") or "").strip()
            if ticker:
                werte.append((ticker, gruppe))
    return werte


def yahoo_symbol(ticker):
    """Yahoo schreibt Aktienklassen mit Bindestrich (BRK-B), nicht mit Punkt (BRK.B).
    Börsenkürzel am Ende (z. B. IUSQ.DE) und Indizes (^GSPC) bleiben unverändert."""
    if ticker.startswith("^") or "=" in ticker:
        return ticker
    if "." in ticker and len(ticker.split(".")[-1]) == 1:   # z. B. BF.B -> BF-B
        return ticker.replace(".", "-")
    return ticker


# ----------------------------------------------------------------------------
# 2. Kurse laden
# ----------------------------------------------------------------------------
def lade_block(symbole):
    """Lädt einen Block von Werten bei Yahoo. Gibt die Tabelle von yfinance zurück
    oder None, wenn alle Versuche scheitern."""
    import yfinance as yf   # erst hier laden, damit die Tests ohne Internet laufen

    for versuch in range(1, VERSUCHE + 1):
        try:
            tabelle = yf.download(
                symbole,
                period=ZEITRAUM,
                interval="1d",
                auto_adjust=False,      # unbereinigter Schlusskurs (nur Splits bereinigt)
                group_by="ticker",      # Spalten: (Ticker, Feld)
                threads=True,
                progress=False,
            )
            if tabelle is not None and len(tabelle) > 0:
                return tabelle
            print(f"  Block leer (Versuch {versuch})", flush=True)
        except Exception as fehler:   # Netzfehler, Sperre, Zeitüberschreitung
            print(f"  Fehler im Block (Versuch {versuch}): {fehler}", flush=True)
        time.sleep(20 * versuch)      # vor dem nächsten Versuch länger warten
    return None


def block_zu_zeilen(tabelle, symbole):
    """Macht aus der Tabelle von yfinance je Wert eine einfache Tabelle mit den
    Spalten open, high, low, close, volume und dem Datum als Index."""
    ergebnis = {}
    if tabelle is None:
        return ergebnis
    mehrstufig = isinstance(tabelle.columns, pd.MultiIndex)
    for symbol in symbole:
        try:
            if mehrstufig:
                if symbol not in tabelle.columns.get_level_values(0):
                    continue
                teil = tabelle[symbol]
            else:
                if len(symbole) != 1:
                    continue
                teil = tabelle
            teil = teil[["Open", "High", "Low", "Close", "Volume"]].copy()
        except KeyError:
            continue
        teil.columns = SPALTEN
        teil = teil.dropna(subset=["close"])          # Tage ohne Kurs entfernen
        if teil.empty:
            continue
        index = pd.to_datetime(teil.index)
        if index.tz is not None:                      # Zeitzone entfernen, nur das Datum zählt
            index = index.tz_localize(None)
        teil.index = index.normalize()
        teil = teil[~teil.index.duplicated(keep="last")].sort_index()
        ergebnis[symbol] = teil
    return ergebnis


def lade_alle(symbole, lader=lade_block):
    """Lädt alle Werte in Blöcken; fehlende Werte werden am Ende in kleinen
    Blöcken noch einmal versucht."""
    kurse = {}
    for start in range(0, len(symbole), BLOCKGROESSE):
        block = symbole[start:start + BLOCKGROESSE]
        print(f"Block {start // BLOCKGROESSE + 1}: {len(block)} Werte", flush=True)
        kurse.update(block_zu_zeilen(lader(block), block))
        time.sleep(PAUSE_SEKUNDEN)

    fehlend = [s for s in symbole if s not in kurse]
    if fehlend:
        print(f"Zweiter Durchgang für {len(fehlend)} fehlende Werte", flush=True)
        for start in range(0, len(fehlend), 10):
            block = fehlend[start:start + 10]
            kurse.update(block_zu_zeilen(lader(block), block))
            time.sleep(PAUSE_SEKUNDEN)
    return kurse


# ----------------------------------------------------------------------------
# 2b. Fortlaufendes Laden: Bestand lesen, nur Neues holen, gezielt nachladen
# ----------------------------------------------------------------------------
def lies_bestand(pfad):
    """Liest die Kursdatei des vorigen Laufs. Gibt {ticker: Tabelle} zurück – leer,
    wenn die Datei fehlt oder nicht lesbar ist (dann wird alles voll geladen)."""
    if not pfad or not os.path.exists(pfad):
        return {}
    try:
        alt = pd.read_csv(pfad, parse_dates=["datum"], dtype={"ticker": str})
        alt = alt.dropna(subset=["close"])
        return {ticker: teil.set_index("datum")[SPALTEN].sort_index() for ticker, teil in alt.groupby("ticker")}
    except Exception as fehler:
        print(f"Bestand nicht lesbar ({fehler}) – es wird alles voll geladen.", flush=True)
        return {}


def haenge_neuere_an(neu, alt):
    """LÜCKENREGEL: Hängt Tage aus dem Bestand an, die jünger sind als der letzte Tag des
    neuen Abrufs – aber nur, wenn beide Reihen am letzten Tag des neuen Abrufs auf 0,5 %
    übereinstimmen (sonst liegt ein Split oder eine Korrektur dazwischen).
    Gibt (Tabelle, True/False ob etwas angehängt wurde) zurück."""
    if alt is None or alt.empty or neu.empty:
        return neu, False
    letzter = neu.index[-1]
    mehr = alt[alt.index > letzter]
    if mehr.empty or letzter not in alt.index:
        return neu, False
    a, n = float(alt.loc[letzter, "close"]), float(neu["close"].iloc[-1])
    if n <= 0 or abs(a / n - 1) > TOLERANZ:
        return neu, False
    return pd.concat([neu, mehr]), True


def fuege_zusammen(alt, neu):
    """Verbindet den Bestand eines Werts mit dem kurzen neuen Abruf.
    Gibt (Tabelle, Grund, ergänzt) zurück. Grund ist None, wenn alles passt; sonst steht
    dort, warum der Wert voll nachgeladen werden soll."""
    gemeinsam = alt.index.intersection(neu.index)
    if len(gemeinsam) == 0:
        return alt, "keine Überlappung", False
    # Vergleich an den ältesten gemeinsamen Tagen: Dort ändert Yahoo nachträglich nur bei
    # Split oder Korrektur. Jüngere Tage dürfen sich noch leicht ändern – da gilt der neue Abruf.
    pruef = gemeinsam[:PRUEFTAGE]
    a, n = alt.loc[pruef, "close"].astype(float), neu.loc[pruef, "close"].astype(float)
    if ((a / n - 1).abs() > TOLERANZ).any():
        return alt, "Abweichung zum Bestand", False
    davor = alt[alt.index < neu.index[0]]          # alte Historie vor dem neuen Abruf
    danach = alt[alt.index > neu.index[-1]]        # Lückenregel: Tage, die Yahoo jetzt weglässt
    tabelle = pd.concat([davor, neu, danach])
    if len(tabelle) >= 2:
        vortag, letzter = float(tabelle["close"].iloc[-2]), float(tabelle["close"].iloc[-1])
        if vortag > 0 and abs(letzter / vortag - 1) > SPRUNG:
            return tabelle, "Kurssprung", not danach.empty
    return tabelle, None, not danach.empty


def lade_fortlaufend(symbole, bestand, lader=lade_block, jetzt_utc=None):
    """Lädt mit Bestand nur den letzten Monat und gezielt einzelne Werte voll nach.
    Gibt (kurse, bericht) zurück. Ohne Bestand wird alles voll geladen."""
    global ZEITRAUM
    lang = ZEITRAUM
    heute = pd.Timestamp((jetzt_utc or datetime.now(timezone.utc)).date())
    grenze = heute - pd.DateOffset(years=JAHRE_BESTAND)

    # Werte mit ausreichend langem Bestand bekommen nur den kurzen Abruf.
    mit = [s for s in symbole if s in bestand and len(bestand[s]) >= MIN_TAGE_BESTAND]
    nachladen = {s: ("kurze Historie" if s in bestand and not bestand[s].empty else "neu in der Liste")
                 for s in symbole if s not in mit}                         # Wert -> Grund
    kurse, ergaenzt, ohne_antwort = {}, [], []

    # 1) kurzer Abruf für alle Werte mit Bestand
    frisch = {}
    if mit:
        print(f"Kurzer Abruf ({KURZ_ZEITRAUM}) für {len(mit)} Werte mit Bestand", flush=True)
        ZEITRAUM = KURZ_ZEITRAUM
        try:
            frisch = lade_alle(mit, lader=lader)
        finally:
            ZEITRAUM = lang
    for s in mit:
        neu = frisch.get(s)
        if neu is None or neu.empty:               # Yahoo hat nichts geliefert: Bestand bleibt stehen
            kurse[s] = bestand[s]; ohne_antwort.append(s); continue
        tabelle, grund, hat_ergaenzt = fuege_zusammen(bestand[s], neu)
        kurse[s] = tabelle                         # gilt, falls das Nachladen scheitert
        if grund:
            nachladen[s] = grund
        elif hat_ergaenzt:
            ergaenzt.append(s)

    # 2) gezieltes volles Nachladen nur der auffälligen und der neuen Werte
    gescheitert = []
    if nachladen:
        print(f"Volles Nachladen ({lang}) für {len(nachladen)} Werte", flush=True)
        voll = lade_alle(list(nachladen), lader=lader)
        for s in nachladen:
            t = voll.get(s)
            if t is None or t.empty:
                gescheitert.append(s); continue    # alter Stand bleibt (oder der Wert fehlt)
            kurse[s], hat_ergaenzt = haenge_neuere_an(t, bestand.get(s))
            if hat_ergaenzt:
                ergaenzt.append(s)

    # 3) Bestand auf fünf Jahre begrenzen
    for s, t in list(kurse.items()):
        kurse[s] = t[t.index >= grenze]

    gruende = {}
    for grund in nachladen.values():
        gruende[grund] = gruende.get(grund, 0) + 1
    bericht = {
        "art": "fortlaufend" if mit else "voll (kein Bestand)",
        "kurz_geladen": len(mit),                  # nur der letzte Monat
        "voll_nachgeladen": len(nachladen),        # fünf Jahre, gezielt
        "gruende": gruende,
        "nachgeladen_beispiele": sorted(s for s, g in nachladen.items()
                                        if g not in ("neu in der Liste", "kurze Historie"))[:40],
        "nachladen_gescheitert": sorted(gescheitert)[:40],
        "aus_bestand_ergaenzt": len(ergaenzt),     # Lückenregel hat gegriffen
        "ohne_antwort": len(ohne_antwort),         # Yahoo lieferte nichts, Bestand blieb stehen
    }
    return kurse, bericht


def erwarteter_us_handelstag(jetzt_utc=None):
    """Letzter Wochentag, an dem die US-Börse schon geschlossen hat (16:20 Uhr New York).
    Feiertage kennt die Funktion nicht – an und nach US-Feiertagen nennt sie einen Tag zu viel."""
    jetzt_ny = (jetzt_utc or datetime.now(timezone.utc)).astimezone(ZoneInfo("America/New_York"))
    tag = jetzt_ny.date()
    if jetzt_ny.weekday() >= 5 or jetzt_ny.time() < uhrzeit(16, 20):
        tag -= timedelta(days=1)
    while tag.weekday() >= 5:
        tag -= timedelta(days=1)
    return pd.Timestamp(tag)


# ----------------------------------------------------------------------------
# 3. Unfertige Tageskerzen entfernen
# ----------------------------------------------------------------------------
def entferne_unfertige_tage(kurse, gruppen, jetzt_utc=None):
    """Entfernt die Kerze des laufenden US-Handelstags und – bei Indizes und ETFs
    anderer Zeitzonen – alle Tage nach dem letzten US-Handelstag.
    Gibt den Bezugstag zurück (letzter abgeschlossener US-Handelstag)."""
    jetzt_utc = jetzt_utc or datetime.now(timezone.utc)
    jetzt_ny = jetzt_utc.astimezone(ZoneInfo("America/New_York"))
    heute_ny = pd.Timestamp(jetzt_ny.date())
    # Die US-Börse schließt um 16:00 Uhr New Yorker Zeit; 20 Minuten Sicherheit.
    boerse_noch_offen = jetzt_ny.weekday() < 5 and jetzt_ny.time() < uhrzeit(16, 20)

    letzte_tage = []
    for symbol, tabelle in list(kurse.items()):
        if gruppen.get(symbol) == "Markt":
            continue
        if boerse_noch_offen:
            tabelle = tabelle[tabelle.index < heute_ny]
        kurse[symbol] = tabelle
        if not tabelle.empty:
            letzte_tage.append(tabelle.index[-1])

    if not letzte_tage:
        return None
    # Bezugstag = der Tag, an dem die meisten Aktien ihren letzten Kurs haben.
    bezugstag = pd.Series(letzte_tage).mode().max()

    for symbol, tabelle in list(kurse.items()):
        if gruppen.get(symbol) == "Markt":
            kurse[symbol] = tabelle[tabelle.index <= bezugstag]
    return bezugstag


# ----------------------------------------------------------------------------
# 4. Kennzahlen rechnen
# ----------------------------------------------------------------------------
def rsi_wilder(schluss, laenge=14):
    """RSI nach Wilder (Standardrechnung der meisten Chartprogramme)."""
    veraenderung = schluss.diff()
    gewinn = veraenderung.clip(lower=0)
    verlust = -veraenderung.clip(upper=0)
    # Wilder-Glättung: gleitender Durchschnitt mit Gewicht 1/laenge
    mittel_gewinn = gewinn.ewm(alpha=1 / laenge, adjust=False, min_periods=laenge).mean()
    mittel_verlust = verlust.ewm(alpha=1 / laenge, adjust=False, min_periods=laenge).mean()
    verhaeltnis = mittel_gewinn / mittel_verlust
    rsi = 100 - 100 / (1 + verhaeltnis)
    return rsi.where(mittel_verlust != 0, 100.0)   # kein Verlust -> RSI 100


def atr_wilder(tabelle, laenge=14):
    """Average True Range nach Wilder (durchschnittliche Tagesspanne)."""
    vortag = tabelle["close"].shift(1)
    spanne = pd.concat(
        [
            tabelle["high"] - tabelle["low"],
            (tabelle["high"] - vortag).abs(),
            (tabelle["low"] - vortag).abs(),
        ],
        axis=1,
    ).max(axis=1)
    return spanne.ewm(alpha=1 / laenge, adjust=False, min_periods=laenge).mean()


def kennzahlen_fuer(tabelle):
    """Rechnet die Kennzahlen eines Werts aus seiner Kurstabelle (letzter Tag)."""
    schluss = tabelle["close"]
    n = len(schluss)
    letzter = float(schluss.iloc[-1])

    def schnitt(tage):
        return float(schluss.iloc[-tage:].mean()) if n >= tage else None

    def abstand(wert):   # Abstand des Kurses zu einer Linie in Prozent
        return round((letzter / wert - 1) * 100, 2) if wert else None

    def rendite(tage):   # Kursveränderung über so viele Handelstage in Prozent
        return round((letzter / float(schluss.iloc[-tage - 1]) - 1) * 100, 2) if n > tage else None

    sma20, sma50, sma150, sma200 = schnitt(20), schnitt(50), schnitt(150), schnitt(200)
    fenster = tabelle.iloc[-252:]                      # rund ein Jahr
    hoch52, tief52 = float(fenster["high"].max()), float(fenster["low"].min())
    rsi = rsi_wilder(schluss).iloc[-1] if n > 14 else None
    atr = atr_wilder(tabelle).iloc[-1] if n > 14 else None
    umsatz20 = float(tabelle["volume"].iloc[-20:].mean()) if n >= 20 else None

    def r(wert, stellen=2):
        return round(float(wert), stellen) if wert is not None and pd.notna(wert) else None

    return {
        "datum": tabelle.index[-1].strftime("%Y-%m-%d"),
        "tage": n,
        "close": r(letzter, 4),
        "sma20": r(sma20, 4), "sma50": r(sma50, 4), "sma150": r(sma150, 4), "sma200": r(sma200, 4),
        "abst20_pct": abstand(sma20), "abst50_pct": abstand(sma50), "abst200_pct": abstand(sma200),
        "sma50_ueber_sma200": (int(sma50 > sma200) if sma50 and sma200 else None),
        "rsi14": r(rsi, 1), "atr14": r(atr, 4),
        "atr14_pct": (round(float(atr) / letzter * 100, 2) if atr is not None and pd.notna(atr) else None),
        "hoch52w": r(hoch52, 4), "tief52w": r(tief52, 4),
        "abst_hoch52w_pct": abstand(hoch52), "abst_tief52w_pct": abstand(tief52),
        "vol20_stueck": r(umsatz20, 0),
        "umsatz20_mio": (round(umsatz20 * letzter / 1e6, 1) if umsatz20 else None),
        "rendite_3m_pct": rendite(63), "rendite_6m_pct": rendite(126), "rendite_1j_pct": rendite(252),
    }


# ----------------------------------------------------------------------------
# 5. Dateien schreiben
# ----------------------------------------------------------------------------
def schreibe_dateien(kurse, tickers, bezugstag, ausgabe, quelle, abruf=None, erwartet=None):
    """Schreibt kurse.csv.gz, kennzahlen.csv, letzte_kurse.csv und status.json.
    Gibt den Status als Wörterbuch zurück."""
    os.makedirs(ausgabe, exist_ok=True)

    aktuell, veraltet, fehlend = [], [], []
    zusatz_aktuell = 0       # aktuelle Werte außerhalb von Watchlist und Midcaps (z. B. Gruppe SP500)
    kennzahlen = []

    with gzip.open(os.path.join(ausgabe, "kurse.csv.gz"), "wt", encoding="utf-8", newline="") as f:
        schreiber = csv.writer(f)
        schreiber.writerow(["datum", "ticker"] + SPALTEN)
        for ticker, gruppe in tickers:
            tabelle = kurse.get(yahoo_symbol(ticker))
            if tabelle is None or tabelle.empty:
                fehlend.append(ticker)
                continue
            # Alle Zeilen des Werts in einem Rutsch schreiben (schnell, auch bei 1.250 Tagen)
            block = tabelle[["open", "high", "low", "close"]].astype(float).round(4)
            block.insert(0, "ticker", ticker)
            block.insert(0, "datum", tabelle.index.strftime("%Y-%m-%d"))
            block["volume"] = tabelle["volume"].fillna(0).astype("int64")
            block.to_csv(f, header=False, index=False, lineterminator="\n")
            eintrag = {"ticker": ticker, "gruppe": gruppe}
            eintrag.update(kennzahlen_fuer(tabelle))
            kennzahlen.append(eintrag)
            if gruppe != "Markt":
                (aktuell if tabelle.index[-1] == bezugstag else veraltet).append(ticker)
                if gruppe not in KERNGRUPPEN and tabelle.index[-1] == bezugstag:
                    zusatz_aktuell += 1

    if kennzahlen:
        felder = list(kennzahlen[0].keys())
        with open(os.path.join(ausgabe, "kennzahlen.csv"), "w", encoding="utf-8", newline="") as f:
            schreiber = csv.DictWriter(f, fieldnames=felder)
            schreiber.writeheader()
            schreiber.writerows(kennzahlen)
        with open(os.path.join(ausgabe, "letzte_kurse.csv"), "w", encoding="utf-8", newline="") as f:
            schreiber = csv.writer(f)
            schreiber.writerow(["ticker", "gruppe", "datum", "close"])
            for eintrag in kennzahlen:
                schreiber.writerow([eintrag["ticker"], eintrag["gruppe"], eintrag["datum"], eintrag["close"]])

    kern = [t for t, g in tickers if g in KERNGRUPPEN]
    zusatz = [t for t, g in tickers if g != "Markt" and g not in KERNGRUPPEN]
    status = {
        "erstellt_utc": datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M"),
        # dieselbe Zeit in deutscher Zeit (MEZ/MESZ) – diese Angabe gehört in Berichte und in den Tracker
        "erstellt_berlin": datetime.now(ZoneInfo("Europe/Berlin")).strftime("%Y-%m-%d %H:%M"),
        "letzter_handelstag": bezugstag.strftime("%Y-%m-%d") if bezugstag is not None else None,
        # Prüfung: Steht der erwartete US-Handelstag in den Daten? (false: US-Feiertag oder Lücke bei Yahoo)
        "erwarteter_handelstag": erwartet.strftime("%Y-%m-%d") if erwartet is not None else None,
        "stand_aktuell": bool(bezugstag is not None and erwartet is not None and bezugstag >= erwartet),
        "aktien_gesamt": len(kern),                           # Watchlist und Midcaps
        "aktien_aktuell": len(aktuell) - zusatz_aktuell,
        "zusatz_gesamt": len(zusatz),                         # übrige Aktien (z. B. Gruppe SP500)
        "zusatz_aktuell": zusatz_aktuell,
        "aktien_veraltet": sorted(veraltet),
        "fehlend": sorted(fehlend),
        "werte_in_datei": len(kennzahlen),
        "zeitraum": ZEITRAUM,
        "abruf": abruf or {},                                 # wie geladen wurde (fortlaufend oder voll)
        "quelle": quelle,
        "hinweis": "Kurse um Splits bereinigt, nicht um Dividenden. RSI und ATR nach Wilder.",
    }
    with open(os.path.join(ausgabe, "status.json"), "w", encoding="utf-8") as f:
        json.dump(status, f, ensure_ascii=False, indent=2)
    return status


# ----------------------------------------------------------------------------
# Hauptprogramm
# ----------------------------------------------------------------------------
def main(argumente=None, lader=lade_block, jetzt_utc=None):
    parser = argparse.ArgumentParser(description="Tageskurse laden und als Dateien ablegen")
    parser.add_argument("--tickers", default="tickers.csv", help="Datei mit der Tickerliste")
    parser.add_argument("--out", default="out", help="Ausgabeordner")
    parser.add_argument("--vorher", default=None, help="Kursdatei des vorigen Laufs (Bestand) für das fortlaufende Laden")
    parser.add_argument("--voll", action="store_true", help="Bestand nicht nutzen, alles voll laden")
    optionen = parser.parse_args(argumente)

    tickers = lies_tickers(optionen.tickers)
    gruppen = {yahoo_symbol(t): g for t, g in tickers}
    symbole = list(gruppen.keys())
    print(f"{len(symbole)} Werte in der Liste", flush=True)

    # Bestand lesen (Schlüssel = Yahoo-Schreibweise, z. B. BRK-B) und fortlaufend laden.
    # Geht dabei irgendetwas schief, lädt das Skript wie früher alles voll.
    bestand = {} if optionen.voll else {yahoo_symbol(t): tab for t, tab in lies_bestand(optionen.vorher).items()}
    try:
        kurse, abruf = lade_fortlaufend(symbole, bestand, lader=lader, jetzt_utc=jetzt_utc)
        if optionen.voll:
            abruf["art"] = "voll (erzwungen)"
    except Exception as fehler:
        print(f"Fortlaufendes Laden gescheitert ({fehler}) – lade alles voll.", flush=True)
        kurse = lade_alle(symbole, lader=lader)
        abruf = {"art": "voll (Notweg nach Fehler)", "kurz_geladen": 0, "voll_nachgeladen": len(symbole)}
    bezugstag = entferne_unfertige_tage(kurse, gruppen, jetzt_utc=jetzt_utc)

    try:
        import yfinance as yf
        quelle = f"Yahoo Finance über yfinance {yf.__version__}"
    except Exception:
        quelle = "Yahoo Finance über yfinance"

    status = schreibe_dateien(kurse, tickers, bezugstag, optionen.out, quelle,
                              abruf=abruf, erwartet=erwarteter_us_handelstag(jetzt_utc))
    print(json.dumps({k: v for k, v in status.items() if k != "hinweis"}, ensure_ascii=False), flush=True)

    # Abbruch, wenn zu wenige Listenwerte aktuell sind – dann wird nichts veröffentlicht.
    if status["aktien_gesamt"] == 0 or status["aktien_aktuell"] < MINDESTANTEIL * status["aktien_gesamt"]:
        print("FEHLER: zu wenige aktuelle Werte – Daten werden nicht veröffentlicht.", flush=True)
        return 1
    return 0


if __name__ == "__main__":
    sys.exit(main())
