#!/usr/bin/env python3
"""
Scrape TradingEconomics country indicators pages for:
- GDP Annual Growth Rate
- Inflation Rate
- Unemployment Rate

Inputs:
  - countries.json (list of {"country": "...", "slug": "..."})

Outputs:
  - indicators.csv (compact table):
      country, year, GDP Annual Growth Rate, Inflation Rate, Unemployment Rate
  - indicators_full.csv (optional, includes units + update strings)

Usage:
  python scrape.py
  python scrape.py --countries countries.json --out indicators.csv
"""

from __future__ import annotations

import argparse
import csv
import json
import re
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path
from typing import Dict, Iterable, List, Optional, Tuple

import requests
from bs4 import BeautifulSoup

TE_BASE = "https://tradingeconomics.com"

TARGET_LABELS = {
    "gdp": "GDP Annual Growth Rate",
    "inflation": "Inflation Rate",
    "unemployment": "Unemployment Rate",
}

# ---------- date parsing helpers ----------

_MONTHS = {
    "Jan": 1, "Feb": 2, "Mar": 3, "Apr": 4, "May": 5, "Jun": 6,
    "Jul": 7, "Aug": 8, "Sep": 9, "Oct": 10, "Nov": 11, "Dec": 12,
}

def parse_te_ref_period(s: str) -> Optional[datetime]:
    """
    TradingEconomics indicators tables typically show reference periods like:
      - "Dec/25"
      - "Sep/25"
      - "Q3/25"  (less common on indicators table; more common on series pages)
    This converts them into a datetime so we can pick the most recent.
    """
    s = (s or "").strip()
    if not s or s.lower() in {"n/a", "-"}:
        return None

    m = re.fullmatch(r"([A-Za-z]{3})/(\d{2})", s)
    if m:
        mon = _MONTHS.get(m.group(1).title())
        yr = 2000 + int(m.group(2))
        if mon:
            return datetime(yr, mon, 1)

    q = re.fullmatch(r"Q([1-4])/(\d{2,4})", s, flags=re.IGNORECASE)
    if q:
        quarter = int(q.group(1))
        yr_raw = q.group(2)
        yr = int(yr_raw) if len(yr_raw) == 4 else 2000 + int(yr_raw)
        # map quarter to first month of quarter
        month = {1: 1, 2: 4, 3: 7, 4: 10}[quarter]
        return datetime(yr, month, 1)

    # Try a few common formats, fallback to None
    for fmt in ("%Y-%m-%d", "%b %Y", "%B %Y"):
        try:
            return datetime.strptime(s, fmt)
        except ValueError:
            pass
    return None


# ---------- scraping ----------

@dataclass
class IndicatorRow:
    label: str
    last: str
    unit: str
    updated: str

def _choose_indicators_table(soup: BeautifulSoup):
    """
    Pick the first table that looks like the country indicators table:
    header contains at least: Last, Previous, Highest, Lowest.
    """
    for table in soup.find_all("table"):
        headers = [th.get_text(strip=True) for th in table.find_all("th")]
        headers_set = set(h.lower() for h in headers)
        if {"last", "previous", "highest", "lowest"}.issubset(headers_set):
            return table
    return None

def scrape_country(slug: str, timeout: int = 30) -> Dict[str, IndicatorRow]:
    url = f"{TE_BASE}/{slug}/indicators"
    headers = {
        "User-Agent": "Mozilla/5.0 (compatible; indicators-scraper/1.0; +https://example.com)",
        "Accept-Language": "en-US,en;q=0.9",
    }
    r = requests.get(url, headers=headers, timeout=timeout)
    r.raise_for_status()

    soup = BeautifulSoup(r.text, "lxml")
    table = _choose_indicators_table(soup)
    if table is None:
        raise RuntimeError(f"Could not find indicators table on {url}")

    rows: Dict[str, IndicatorRow] = {}
    for tr in table.find_all("tr"):
        tds = tr.find_all("td")
        if not tds:
            continue

        cells = [td.get_text(" ", strip=True) for td in tds]
        # Expected layout (from TE):
        # [Indicator, Last, Previous, Highest, Lowest, Unit, ReferencePeriod]
        if len(cells) < 6:
            continue

        label = cells[0]
        last = cells[1] if len(cells) > 1 else ""
        # Some tables may omit unit; we guard with indexes.
        unit = cells[-2] if len(cells) >= 2 else ""
        updated = cells[-1] if len(cells) >= 1 else ""

        rows[label] = IndicatorRow(label=label, last=last, unit=unit, updated=updated)

    return rows

def find_best_match(rows: Dict[str, IndicatorRow], target_label: str) -> Optional[IndicatorRow]:
    """
    Match exact first; then case-insensitive; then 'contains' match.
    """
    if target_label in rows:
        return rows[target_label]

    # case-insensitive exact
    lower_map = {k.lower(): v for k, v in rows.items()}
    if target_label.lower() in lower_map:
        return lower_map[target_label.lower()]

    # contains match
    tl = target_label.lower()
    for k, v in rows.items():
        if tl in k.lower():
            return v

    return None

def most_recent_update(*updates: str) -> Tuple[str, Optional[datetime]]:
    """
    Return the most recent update string among the given update strings, plus parsed datetime (if possible).
    """
    best_s = ""
    best_dt: Optional[datetime] = None
    for s in updates:
        dt = parse_te_ref_period(s)
        if dt is None:
            continue
        if best_dt is None or dt > best_dt:
            best_dt = dt
            best_s = s
    return best_s, best_dt


# ---------- output ----------

def write_csv_compact(
    out_path: Path,
    records: List[Dict[str, str]],
    fieldnames: List[str],
) -> None:
    out_path.parent.mkdir(parents=True, exist_ok=True)
    with out_path.open("w", newline="", encoding="utf-8") as f:
        w = csv.DictWriter(f, fieldnames=fieldnames)
        w.writeheader()
        for rec in records:
            w.writerow(rec)

def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--countries", default="countries.json", help="Path to countries.json")
    ap.add_argument("--out", default="data/indicators.csv", help="Output CSV (compact)")
    ap.add_argument("--out-full", default="data/indicators_full.csv", help="Output CSV (full)")
    args = ap.parse_args()

    countries_path = Path(args.countries)
    countries = json.loads(countries_path.read_text(encoding="utf-8"))

    compact_records: List[Dict[str, str]] = []
    full_records: List[Dict[str, str]] = []

    for c in countries:
        country = c["country"]
        slug = c["slug"]

        rows = scrape_country(slug)

        gdp = find_best_match(rows, TARGET_LABELS["gdp"])
        infl = find_best_match(rows, TARGET_LABELS["inflation"])
        unemp = find_best_match(rows, TARGET_LABELS["unemployment"])

        # Keep original TE update strings (e.g., "Dec/25")
        upd_gdp = gdp.updated if gdp else ""
        upd_inf = infl.updated if infl else ""
        upd_un = unemp.updated if unemp else ""

        # "year" column = most recent update among the 3
        year_str, _ = most_recent_update(upd_gdp, upd_inf, upd_un)

        compact_records.append({
            "country": country,
            "year": year_str,
            TARGET_LABELS["gdp"]: (gdp.last if gdp else ""),
            TARGET_LABELS["inflation"]: (infl.last if infl else ""),
            TARGET_LABELS["unemployment"]: (unemp.last if unemp else ""),
        })

        full_records.append({
            "country": country,
            "slug": slug,
            "year": year_str,
            "gdp_last": (gdp.last if gdp else ""),
            "gdp_unit": (gdp.unit if gdp else ""),
            "gdp_updated": upd_gdp,
            "inflation_last": (infl.last if infl else ""),
            "inflation_unit": (infl.unit if infl else ""),
            "inflation_updated": upd_inf,
            "unemployment_last": (unemp.last if unemp else ""),
            "unemployment_unit": (unemp.unit if unemp else ""),
            "unemployment_updated": upd_un,
        })

    # Write compact
    compact_fields = ["country", "year", TARGET_LABELS["gdp"], TARGET_LABELS["inflation"], TARGET_LABELS["unemployment"]]
    write_csv_compact(Path(args.out), compact_records, compact_fields)

    # Write full
    full_fields = [
        "country", "slug", "year",
        "gdp_last", "gdp_unit", "gdp_updated",
        "inflation_last", "inflation_unit", "inflation_updated",
        "unemployment_last", "unemployment_unit", "unemployment_updated",
    ]
    write_csv_compact(Path(args.out_full), full_records, full_fields)

    print(f"Wrote: {args.out}")
    print(f"Wrote: {args.out_full}")

if __name__ == "__main__":
    main()
