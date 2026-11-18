#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
kurse_laden.py – lädt Tageskurse für Jans Papierdepot und legt sie als Dateien ab.

Was das Skript macht:
  1. Es liest die Tickerliste aus tickers.csv (Spalten: ticker, gruppe).
  2. Es lädt für jeden Wert die Tageskurse der letzten zwei Jahre von Yahoo Finance
     (über die Bibliothek yfinance): Eröffnung, Hoch, Tief, Schluss, Umsatz in Stück.
     Die Kurse sind um Aktiensplits bereinigt, aber NICHT um Dividenden – so passen
     sie zu den Kursen im Chart und zu den Vergleichskursen von ariva.
  3. Es entfernt unfertige Tageskerzen (laufender Handelstag).
  4. Es rechnet je Wert die Kennzahlen für den Grobfilter (Durchschnitte, RSI, ATR,
     52-Wochen-Marken, Renditen) – immer mit derselben Formel.
  5. Es schreibt vier Dateien in den Ausgabeordner:
        kurse.csv.gz      alle Tageskurse (datum,ticker,open,high,low,close,volume)
        kennzahlen.csv    eine Zeile je Wert mit den Kennzahlen
        letzte_kurse.csv  eine Zeile je Wert: letzter Handelstag und Schlusskurs
        status.json       Stand der Daten (letzter Handelstag, Anzahl, fehlende Werte)
  6. Sind weniger als 90 % der Aktien aktuell, endet das Skript mit einem Fehler.
     Dann veröffentlicht der Arbeitsablauf nichts, und der alte Stand bleibt stehen.

Aufruf:  python kurse_laden.py --tickers tickers.csv --out out
"""

import argparse
import csv
import gzip
import json
import sys
import time
from datetime import datetime, time as uhrzeit, timezone
from zoneinfo import ZoneInfo

import pandas as pd

# ----------------------------------------------------------------------------
# Einstellungen
# ----------------------------------------------------------------------------
ZEITRAUM = "2y"          # so viel Kurshistorie wird geladen (rund 500 Handelstage)
BLOCKGROESSE = 50        # so viele Werte je Abruf bei Yahoo
PAUSE_SEKUNDEN = 3       # Pause zwischen zwei Blöcken (schont die Quelle)
VERSUCHE = 3             # so oft wird ein gescheiterter Block wiederholt
MINDESTANTEIL = 0.90     # mindestens so viele Aktien müssen aktuell sein
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
def schreibe_dateien(kurse, tickers, bezugstag, ausgabe, quelle):
    """Schreibt kurse.csv.gz, kennzahlen.csv, letzte_kurse.csv und status.json.
    Gibt den Status als Wörterbuch zurück."""
    import os
    os.makedirs(ausgabe, exist_ok=True)

    aktuell, veraltet, fehlend = [], [], []
    kennzahlen = []

    with gzip.open(os.path.join(ausgabe, "kurse.csv.gz"), "wt", encoding="utf-8", newline="") as f:
        schreiber = csv.writer(f)
        schreiber.writerow(["datum", "ticker"] + SPALTEN)
        for ticker, gruppe in tickers:
            tabelle = kurse.get(yahoo_symbol(ticker))
            if tabelle is None or tabelle.empty:
                fehlend.append(ticker)
                continue
            for datum, zeile in tabelle.iterrows():
                menge = int(zeile["volume"]) if pd.notna(zeile["volume"]) else 0
                schreiber.writerow([
                    datum.strftime("%Y-%m-%d"), ticker,
                    round(float(zeile["open"]), 4), round(float(zeile["high"]), 4),
                    round(float(zeile["low"]), 4), round(float(zeile["close"]), 4), menge,
                ])
            eintrag = {"ticker": ticker, "gruppe": gruppe}
            eintrag.update(kennzahlen_fuer(tabelle))
            kennzahlen.append(eintrag)
            if gruppe != "Markt":
                (aktuell if tabelle.index[-1] == bezugstag else veraltet).append(ticker)

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

    aktien = [t for t, g in tickers if g != "Markt"]
    status = {
        "erstellt_utc": datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M"),
        "letzter_handelstag": bezugstag.strftime("%Y-%m-%d") if bezugstag is not None else None,
        "aktien_gesamt": len(aktien),
        "aktien_aktuell": len(aktuell),
        "aktien_veraltet": sorted(veraltet),
        "fehlend": sorted(fehlend),
        "werte_in_datei": len(kennzahlen),
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
    optionen = parser.parse_args(argumente)

    tickers = lies_tickers(optionen.tickers)
    gruppen = {yahoo_symbol(t): g for t, g in tickers}
    symbole = list(gruppen.keys())
    print(f"{len(symbole)} Werte in der Liste", flush=True)

    kurse = lade_alle(symbole, lader=lader)
    bezugstag = entferne_unfertige_tage(kurse, gruppen, jetzt_utc=jetzt_utc)

    try:
        import yfinance as yf
        quelle = f"Yahoo Finance über yfinance {yf.__version__}"
    except Exception:
        quelle = "Yahoo Finance über yfinance"

    status = schreibe_dateien(kurse, tickers, bezugstag, optionen.out, quelle)
    print(json.dumps({k: v for k, v in status.items() if k != "hinweis"}, ensure_ascii=False), flush=True)

    # Abbruch, wenn zu wenige Aktien aktuell sind – dann wird nichts veröffentlicht.
    if status["aktien_gesamt"] == 0 or status["aktien_aktuell"] < MINDESTANTEIL * status["aktien_gesamt"]:
        print("FEHLER: zu wenige aktuelle Werte – Daten werden nicht veröffentlicht.", flush=True)
        return 1
    return 0


if __name__ == "__main__":
    sys.exit(main())
