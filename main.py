#!/usr/bin/env python3
"""
S&P 500 Revenue Per Share Tool

Fetches the S&P 500 list, then for each company pulls the latest annual
revenue and shares outstanding from SEC EDGAR. Calculates revenue per share
and writes a CSV.

Caching: every network response is stored on disk so re-runs are instant
and we don't hammer SEC's servers.

Note on unit normalization: SEC's XBRL companyfacts API returns values at
their TRUE scale (the actual full number) regardless of whether the original
filing presented them in thousands or millions. So we don't have to manually
multiply anything - we just need to pick the right XBRL concept name, since
companies report revenue under several different US-GAAP tags.

Usage:
    1. Update USER_AGENT below with your name + email (SEC requires this)
    2. pip install requests pandas lxml
    3. python main.py
"""

import json
import time
from io import StringIO
from pathlib import Path

import pandas as pd
import requests

# ---------------------------------------------------------------------------
# CONFIG - update USER_AGENT before running, SEC requires real contact info
# ---------------------------------------------------------------------------
USER_AGENT = "jjrpfister+sec-data-tool@gmail.com"

# All paths are anchored to the script's directory so the CSV and cache
# always land next to main.py regardless of where it's invoked from.
SCRIPT_DIR = Path(__file__).resolve().parent
CACHE_DIR = SCRIPT_DIR / "cache"
COMPANY_FACTS_DIR = CACHE_DIR / "company_facts"
TICKER_CIK_CACHE = CACHE_DIR / "ticker_cik.json"
SP500_CACHE = CACHE_DIR / "sp500.csv"
OUTPUT_CSV = SCRIPT_DIR / "sp500_revenue_per_share.csv"

# Cache TTL: SEC filings only update quarterly so 30 days is plenty
CACHE_TTL_SECONDS = 30 * 24 * 3600

# Rate limit: SEC allows up to 10 req/sec. We stay well under.
REQUEST_DELAY_SECONDS = 0.15

# US-GAAP concepts that companies use for revenue, in priority order.
# Different companies (and different eras) use different tags - we try each.
REVENUE_CONCEPTS = [
    # Standard concepts - most companies
    "RevenueFromContractWithCustomerExcludingAssessedTax",
    "Revenues",
    "SalesRevenueNet",
    "RevenueFromContractWithCustomerIncludingAssessedTax",
    "SalesRevenueGoodsNet",
    # Broker-dealers (Goldman Sachs, Morgan Stanley)
    "RevenuesNetOfInterestExpense",
    # Banks / consumer finance - approximation: gross interest + dividend income.
    # Note: this is NOT "total revenue" in the GAAP sense (which would be net
    # interest income + noninterest income), but it's the closest single XBRL
    # concept and is in the right order of magnitude for revenue-per-share.
    "InterestAndDividendIncomeOperating",
    "InterestIncomeOperating",
]


# ---------------------------------------------------------------------------
# HTTP + cache helpers
# ---------------------------------------------------------------------------
def make_session() -> requests.Session:
    s = requests.Session()
    s.headers.update({
        "User-Agent": USER_AGENT,
        "Accept-Encoding": "gzip, deflate",
    })
    return s


def cache_is_fresh(path: Path) -> bool:
    if not path.exists():
        return False
    return (time.time() - path.stat().st_mtime) < CACHE_TTL_SECONDS


# ---------------------------------------------------------------------------
# S&P 500 list (Wikipedia)
# ---------------------------------------------------------------------------
def get_sp500_tickers(session: requests.Session) -> pd.DataFrame:
    """Returns DataFrame with columns: Symbol, Security."""
    if cache_is_fresh(SP500_CACHE):
        return pd.read_csv(SP500_CACHE)

    url = "https://en.wikipedia.org/wiki/List_of_S%26P_500_companies"
    resp = session.get(url)
    resp.raise_for_status()
    tables = pd.read_html(StringIO(resp.text))
    df = tables[0][["Symbol", "Security"]].copy()

    CACHE_DIR.mkdir(parents=True, exist_ok=True)
    df.to_csv(SP500_CACHE, index=False)
    return df


# ---------------------------------------------------------------------------
# Ticker -> CIK map (SEC's official file)
# ---------------------------------------------------------------------------
def get_ticker_cik_map(session: requests.Session) -> dict[str, str]:
    """Fetch SEC's official ticker->CIK mapping. Returns {ticker: 10-digit CIK}."""
    if cache_is_fresh(TICKER_CIK_CACHE):
        return json.loads(TICKER_CIK_CACHE.read_text())

    url = "https://www.sec.gov/files/company_tickers.json"
    resp = session.get(url)
    resp.raise_for_status()
    raw = resp.json()
    # Format: {"0": {"cik_str": 320193, "ticker": "AAPL", "title": "Apple Inc."}, ...}
    mapping = {
        entry["ticker"].upper(): str(entry["cik_str"]).zfill(10)
        for entry in raw.values()
    }

    CACHE_DIR.mkdir(parents=True, exist_ok=True)
    TICKER_CIK_CACHE.write_text(json.dumps(mapping))
    return mapping


def find_cik(ticker: str, mapping: dict[str, str]) -> str | None:
    """Try a few normalizations - Wikipedia uses BRK.B, SEC may use BRK-B or BRKB."""
    candidates = [
        ticker.upper(),
        ticker.replace(".", "-").upper(),
        ticker.replace(".", "").upper(),
        ticker.replace("-", ".").upper(),
    ]
    for c in candidates:
        if c in mapping:
            return mapping[c]
    return None


# ---------------------------------------------------------------------------
# Company facts (SEC XBRL)
# ---------------------------------------------------------------------------
def fetch_company_facts(session: requests.Session, cik: str) -> dict | None:
    """Fetch and cache the full XBRL company facts JSON for one CIK."""
    COMPANY_FACTS_DIR.mkdir(parents=True, exist_ok=True)
    cache_path = COMPANY_FACTS_DIR / f"{cik}.json"

    if cache_is_fresh(cache_path):
        return json.loads(cache_path.read_text())

    url = f"https://data.sec.gov/api/xbrl/companyfacts/CIK{cik}.json"
    time.sleep(REQUEST_DELAY_SECONDS)
    resp = session.get(url)
    if resp.status_code == 404:
        return None
    resp.raise_for_status()
    data = resp.json()
    cache_path.write_text(json.dumps(data))
    return data


def latest_annual_revenue(facts: dict) -> int | None:
    """Pick the most recent non-zero FY value from a 10-K, trying each concept."""
    ns = facts.get("facts", {}).get("us-gaap", {})
    for concept in REVENUE_CONCEPTS:
        if concept not in ns:
            continue
        usd_entries = ns[concept].get("units", {}).get("USD", [])
        annual = [
            e for e in usd_entries
            if e.get("fp") == "FY"
            and "10-K" in e.get("form", "")
            and e.get("val", 0) != 0
        ]
        if not annual:
            continue
        annual.sort(key=lambda e: e.get("end", ""), reverse=True)
        return annual[0]["val"]
    return None


def latest_shares_outstanding(facts: dict) -> int | None:
    """
    Find the most recent share count, trying concepts in order of preference.

    1. us-gaap:CommonStockSharesOutstanding (point-in-time, official)
    2. dei:EntityCommonStockSharesOutstanding (entity-wide, point-in-time)
    3. us-gaap:WeightedAverageNumberOfSharesOutstandingBasic from a 10-K
       - last resort for dual-class share companies (META, ABNB, etc.) that
         don't report a single CommonStockSharesOutstanding number, since
         each share class is reported separately

    All zero values are filtered out (some companies have stale val=0 entries).
    """
    # 1 & 2: point-in-time concepts, accept any filing
    for namespace, concept in [
        ("us-gaap", "CommonStockSharesOutstanding"),
        ("dei", "EntityCommonStockSharesOutstanding"),
    ]:
        ns = facts.get("facts", {}).get(namespace, {})
        if concept not in ns:
            continue
        entries = [
            e for e in ns[concept].get("units", {}).get("shares", [])
            if e.get("val", 0) != 0
        ]
        if not entries:
            continue
        entries.sort(key=lambda e: e.get("end", ""), reverse=True)
        return entries[0]["val"]

    # 3 & 4: weighted-average shares from latest annual 10-K (basic, then diluted).
    # Diluted is typically ~0.5-2% higher than basic due to stock options, so
    # it's a slight under-estimate for rev/share but close enough.
    ns = facts.get("facts", {}).get("us-gaap", {})
    for concept in (
        "WeightedAverageNumberOfSharesOutstandingBasic",
        "WeightedAverageNumberOfDilutedSharesOutstanding",
    ):
        if concept not in ns:
            continue
        entries = ns[concept].get("units", {}).get("shares", [])
        annual = [
            e for e in entries
            if e.get("fp") == "FY"
            and "10-K" in e.get("form", "")
            and e.get("val", 0) != 0
        ]
        if annual:
            annual.sort(key=lambda e: e.get("end", ""), reverse=True)
            return annual[0]["val"]

    return None


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------
def main() -> None:
    if "example.com" in USER_AGENT:
        print("WARNING: update USER_AGENT at the top of this script with your real")
        print("name and email - SEC requires this and may block requests without it.\n")

    session = make_session()

    print("Fetching S&P 500 list from Wikipedia...")
    sp500 = get_sp500_tickers(session)
    print(f"  {len(sp500)} companies\n")

    print("Fetching SEC ticker->CIK map...")
    ticker_cik = get_ticker_cik_map(session)
    print(f"  {len(ticker_cik)} tickers in SEC's index\n")

    rows: list[dict] = []
    missing: list[tuple[str, str]] = []

    for i, row in sp500.iterrows():
        ticker = row["Symbol"]
        company = row["Security"]
        cik = find_cik(ticker, ticker_cik)

        if cik is None:
            missing.append((ticker, "no CIK in SEC index"))
            continue

        try:
            facts = fetch_company_facts(session, cik)
        except Exception as e:
            missing.append((ticker, f"fetch failed: {e}"))
            continue

        if facts is None:
            missing.append((ticker, "404 from SEC"))
            continue

        revenue = latest_annual_revenue(facts)
        shares = latest_shares_outstanding(facts)
        rps = (revenue / shares) if (revenue and shares) else None

        status = "ok" if rps is not None else "missing data"
        print(f"[{i + 1}/{len(sp500)}] {ticker:<6} {status}")

        rows.append({
            "ticker": ticker,
            "company": company,
            "cik": cik,
            "revenue": revenue,
            "shares_outstanding": shares,
            "revenue_per_share": rps,
        })

    out = pd.DataFrame(rows)
    out.to_csv(OUTPUT_CSV, index=False)

    total = len(sp500)
    full = int(out["revenue_per_share"].notna().sum())
    partial = len(out) - full
    failed = len(missing)

    print("\n" + "=" * 50)
    print("SUMMARY")
    print("=" * 50)
    print(f"Total S&P 500 companies:    {total}")
    print(f"  Success (rev + shares):   {full}")
    print(f"  Partial (missing fields): {partial}")
    print(f"  Failed (no SEC data):     {failed}")
    print(f"\nCSV written to: {OUTPUT_CSV}")

    if missing:
        print(f"\nFailed companies ({len(missing)}):")
        for t, reason in missing:
            print(f"  {t}: {reason}")


if __name__ == "__main__":
    main()
