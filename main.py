#!/usr/bin/env python3
"""
S&P 500 Financial Data Tool

Fetches the S&P 500 list, then for each company pulls financial data from
SEC EDGAR's XBRL API. Calculates per-share and margin metrics, writes a CSV,
and optionally updates a Google Sheet automatically.

Caching: every network response is stored on disk so re-runs are instant
and we don't hammer SEC's servers.

Note on unit normalization: SEC's XBRL companyfacts API returns values at
their TRUE scale (the actual full number) regardless of whether the original
filing presented them in thousands or millions. So we don't have to manually
multiply anything - we just need to pick the right XBRL concept name, since
companies report financials under several different US-GAAP tags.

Usage:
    1. Update USER_AGENT below with your name + email (SEC requires this)
    2. pip install requests pandas lxml
    3. python main.py
"""

import json
import subprocess
import time
from io import StringIO
from pathlib import Path

import pandas as pd
import requests

# ---------------------------------------------------------------------------
# CONFIG
# ---------------------------------------------------------------------------
# SEC requires a User-Agent identifying the requester. Use your real name + email.
USER_AGENT = "jjrpfister+sec-data-tool@gmail.com"

# Google Sheet to auto-update after each run. Set to "" to disable.
SHEET_ID = "1mjK2o-XrvzUh0YCuW57SZJ-dCybqezDz3OvrTP5wviM"

# ---------------------------------------------------------------------------
# PATHS
# ---------------------------------------------------------------------------
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

# ---------------------------------------------------------------------------
# COLUMN ORDER
# ---------------------------------------------------------------------------
COLUMN_ORDER = [
    # Identifiers + metadata
    "ticker", "company", "cik",
    "gics_sector", "gics_sub_industry", "headquarters",
    "date_added", "year_founded",
    # Income statement
    "revenue", "net_income", "operating_income", "gross_profit",
    # Per-share metrics
    "shares_outstanding", "revenue_per_share", "eps", "book_value_per_share",
    # Margins
    "profit_margin", "operating_margin", "gross_margin",
    # Balance sheet
    "total_assets", "total_liabilities", "stockholders_equity",
    "cash_and_equivalents", "long_term_debt",
    # Other
    "employees", "revenue_per_employee",
]

# ---------------------------------------------------------------------------
# XBRL CONCEPT LISTS
# ---------------------------------------------------------------------------
# US-GAAP concepts that companies use for revenue, in priority order.
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

NET_INCOME_CONCEPTS        = ["NetIncomeLoss"]
OPERATING_INCOME_CONCEPTS  = ["OperatingIncomeLoss"]
GROSS_PROFIT_CONCEPTS      = ["GrossProfit"]
TOTAL_ASSETS_CONCEPTS      = ["Assets"]
TOTAL_LIABILITIES_CONCEPTS = ["Liabilities"]
STOCKHOLDERS_EQUITY_CONCEPTS = [
    "StockholdersEquity",
    "StockholdersEquityAttributableToParent",
]
CASH_CONCEPTS = [
    "CashAndCashEquivalentsAtCarryingValue",
    "CashCashEquivalentsAndShortTermInvestments",
]
LONG_TERM_DEBT_CONCEPTS = ["LongTermDebtNoncurrent", "LongTermDebt"]

# Wikipedia column mapping: table header → output column name
WIKI_COLUMNS = {
    "Symbol":                "ticker",
    "Security":              "company",
    "GICS Sector":           "gics_sector",
    "GICS Sub-Industry":     "gics_sub_industry",
    "Headquarters Location": "headquarters",
    "Date added":            "date_added",
    "Founded":               "year_founded",
}


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
    """Returns DataFrame with columns matching WIKI_COLUMNS values."""
    if cache_is_fresh(SP500_CACHE):
        cached = pd.read_csv(SP500_CACHE)
        # Invalidate if the cache predates the extra columns being added
        if len(cached.columns) < len(WIKI_COLUMNS):
            SP500_CACHE.unlink()
        else:
            return cached

    url = "https://en.wikipedia.org/wiki/List_of_S%26P_500_companies"
    resp = session.get(url)
    resp.raise_for_status()
    tables = pd.read_html(StringIO(resp.text))
    df = tables[0][list(WIKI_COLUMNS.keys())].copy()
    df = df.rename(columns=WIKI_COLUMNS)

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


# ---------------------------------------------------------------------------
# XBRL extractors
# ---------------------------------------------------------------------------
def latest_annual_usd(facts: dict, concepts: list[str]) -> int | float | None:
    """
    Generic: try each concept in order, return the most recent non-zero
    FY / 10-K USD value, or None if nothing found.
    """
    ns = facts.get("facts", {}).get("us-gaap", {})
    for concept in concepts:
        if concept not in ns:
            continue
        annual = [
            e for e in ns[concept].get("units", {}).get("USD", [])
            if e.get("fp") == "FY"
            and "10-K" in e.get("form", "")
            and e.get("val", 0) != 0
        ]
        if not annual:
            continue
        annual.sort(key=lambda e: e.get("end", ""), reverse=True)
        return annual[0]["val"]
    return None


def latest_annual_revenue(facts: dict) -> int | float | None:
    return latest_annual_usd(facts, REVENUE_CONCEPTS)


def latest_shares_outstanding(facts: dict) -> int | None:
    """
    Find the most recent share count, trying concepts in order of preference.

    1. us-gaap:CommonStockSharesOutstanding (point-in-time, official)
    2. dei:EntityCommonStockSharesOutstanding (entity-wide, point-in-time)
    3. us-gaap:WeightedAverageNumberOfSharesOutstandingBasic from a 10-K
       - fallback for dual-class share companies (META, ABNB, etc.)
    4. us-gaap:WeightedAverageNumberOfDilutedSharesOutstanding — last resort

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

    # 3 & 4: weighted-average shares from latest annual 10-K (basic, then diluted)
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


def latest_employees(facts: dict) -> int | None:
    """Most recent EntityNumberOfEmployees from DEI namespace (point-in-time)."""
    entries = (
        facts.get("facts", {})
        .get("dei", {})
        .get("EntityNumberOfEmployees", {})
        .get("units", {})
        .get("pure", [])
    )
    entries = [e for e in entries if e.get("val", 0) != 0]
    if not entries:
        return None
    entries.sort(key=lambda e: e.get("end", ""), reverse=True)
    return entries[0]["val"]


# ---------------------------------------------------------------------------
# Arithmetic helpers
# ---------------------------------------------------------------------------
def _safe_div(num, denom):
    """Returns num/denom or None if either is None or denom is zero."""
    if num is None or denom is None or denom == 0:
        return None
    return num / denom


# ---------------------------------------------------------------------------
# Google Sheets
# ---------------------------------------------------------------------------
def update_google_sheet(csv_path: Path, sheet_id: str) -> None:
    """
    Push the CSV to an existing Google Sheet using the gws CLI.
    Clears the sheet first, then writes all rows. No-ops if sheet_id is empty.
    """
    if not sheet_id:
        return

    df = pd.read_csv(csv_path, dtype=str).fillna("")

    def _parse_cell(v: str):
        """Return numeric types for Sheets (so it sorts/charts correctly)."""
        if v == "":
            return ""
        try:
            f = float(v)
            return int(f) if f.is_integer() else f
        except ValueError:
            return v

    values = [list(df.columns)]
    for row in df.itertuples(index=False, name=None):
        values.append([_parse_cell(cell) for cell in row])

    body = json.dumps({"values": values})

    # Step 1: clear existing data (broad range covers any plausible dataset)
    r = subprocess.run(
        ["gws", "sheets", "spreadsheets", "values", "clear",
         "--params", json.dumps({"spreadsheetId": sheet_id, "range": "A1:ZZ10000"}),
         "--json", "{}"],
        capture_output=True, text=True,
    )
    if r.returncode != 0:
        print(f"WARNING: gws clear failed — {r.stderr.strip()}")
        return

    # Step 2: write new data starting at A1
    r = subprocess.run(
        ["gws", "sheets", "spreadsheets", "values", "update",
         "--params", json.dumps({
             "spreadsheetId": sheet_id,
             "range": "A1",
             "valueInputOption": "USER_ENTERED",
         }),
         "--json", body],
        capture_output=True, text=True,
    )
    if r.returncode != 0:
        print(f"WARNING: gws update failed — {r.stderr.strip()}")
    else:
        print(f"Google Sheet updated: https://docs.google.com/spreadsheets/d/{sheet_id}/edit")


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
        ticker = row["ticker"]
        company = row["company"]
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

        # --- Raw metrics from SEC XBRL ---
        revenue             = latest_annual_revenue(facts)
        net_income          = latest_annual_usd(facts, NET_INCOME_CONCEPTS)
        operating_income    = latest_annual_usd(facts, OPERATING_INCOME_CONCEPTS)
        gross_profit        = latest_annual_usd(facts, GROSS_PROFIT_CONCEPTS)
        total_assets        = latest_annual_usd(facts, TOTAL_ASSETS_CONCEPTS)
        total_liabilities   = latest_annual_usd(facts, TOTAL_LIABILITIES_CONCEPTS)
        stockholders_equity = latest_annual_usd(facts, STOCKHOLDERS_EQUITY_CONCEPTS)
        cash_and_equivalents= latest_annual_usd(facts, CASH_CONCEPTS)
        long_term_debt      = latest_annual_usd(facts, LONG_TERM_DEBT_CONCEPTS)
        shares              = latest_shares_outstanding(facts)
        employees           = latest_employees(facts)

        # --- Derived metrics ---
        rps                  = _safe_div(revenue, shares)
        eps                  = _safe_div(net_income, shares)
        profit_margin        = _safe_div(net_income, revenue)
        operating_margin     = _safe_div(operating_income, revenue)
        gross_margin         = _safe_div(gross_profit, revenue)
        book_value_per_share = _safe_div(stockholders_equity, shares)
        revenue_per_employee = _safe_div(revenue, employees)

        status = "ok" if rps is not None else "missing data"
        print(f"[{i + 1}/{len(sp500)}] {ticker:<6} {status}")

        rows.append({
            "ticker":              ticker,
            "company":             company,
            "cik":                 cik,
            "gics_sector":         row.get("gics_sector"),
            "gics_sub_industry":   row.get("gics_sub_industry"),
            "headquarters":        row.get("headquarters"),
            "date_added":          row.get("date_added"),
            "year_founded":        row.get("year_founded"),
            "revenue":             revenue,
            "net_income":          net_income,
            "operating_income":    operating_income,
            "gross_profit":        gross_profit,
            "shares_outstanding":  shares,
            "revenue_per_share":   rps,
            "eps":                 eps,
            "book_value_per_share":book_value_per_share,
            "profit_margin":       profit_margin,
            "operating_margin":    operating_margin,
            "gross_margin":        gross_margin,
            "total_assets":        total_assets,
            "total_liabilities":   total_liabilities,
            "stockholders_equity": stockholders_equity,
            "cash_and_equivalents":cash_and_equivalents,
            "long_term_debt":      long_term_debt,
            "employees":           employees,
            "revenue_per_employee":revenue_per_employee,
        })

    out = pd.DataFrame(rows, columns=COLUMN_ORDER)
    out.to_csv(OUTPUT_CSV, index=False)

    total   = len(sp500)
    full    = int(out["revenue_per_share"].notna().sum())
    partial = len(out) - full
    failed  = len(missing)

    print("\n" + "=" * 50)
    print("SUMMARY")
    print("=" * 50)
    print(f"Total S&P 500 companies:    {total}")
    print(f"  Success (rev + shares):   {full}")
    print(f"  Partial (missing fields): {partial}")
    print(f"  Failed (no SEC data):     {failed}")
    print()
    print("Field coverage:")
    for field in ["net_income", "operating_income", "gross_profit",
                  "total_assets", "stockholders_equity", "employees"]:
        n = int(out[field].notna().sum())
        print(f"  {field:<30} {n}/{len(out)}")
    print(f"\nCSV written to: {OUTPUT_CSV}")

    if missing:
        print(f"\nFailed companies ({len(missing)}):")
        for t, reason in missing:
            print(f"  {t}: {reason}")

    print()
    try:
        update_google_sheet(OUTPUT_CSV, SHEET_ID)
    except FileNotFoundError:
        print("NOTE: gws not found — skipping Google Sheets update.")
    except Exception as e:
        print(f"WARNING: Google Sheets update failed: {e}")


if __name__ == "__main__":
    main()
