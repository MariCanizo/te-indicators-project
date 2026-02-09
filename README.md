# TradingEconomics Indicators Scraper (Starter)

## Setup
```bash
python -m venv .venv
source .venv/bin/activate  # (Windows: .venv\Scripts\activate)
pip install -r requirements.txt
```

## Run
```bash
python scrape.py
```

Outputs:
- indicators.csv (compact)
- indicators_full.csv (with units + update strings)

## Countries
Edit `countries.json` to add/remove countries or fix slugs.
