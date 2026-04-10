# company-data-tool

A small Python script that fetches every company in the **S&P 500** and
computes **revenue per share** for each one using publicly-available data
from **SEC EDGAR**.

No AI, no paid APIs, no screen scraping of financial sites — just XBRL
data direct from the SEC, with local caching so re-runs are instant.

## Output

`sp500_revenue_per_share.csv`:

| ticker | company             | cik        | revenue       | shares_outstanding | revenue_per_share |
|--------|---------------------|------------|---------------|--------------------|-------------------|
| MMM    | 3M                  | 0000066740 | 24948000000.0 | 530279131          | 47.05             |
| ABT    | Abbott Laboratories | 0000001800 | 44328000000.0 | 1737682887         | 25.51             |
| ABBV   | AbbVie              | 0001551152 | 61160000000.0 | 1768169012         | 34.59             |
| ACN    | Accenture           | 0001467373 | 69668700000.0 | 636863200          | 109.40            |
| …      | …                   | …          | …             | …                  | …                 |

**Current coverage: 500 / 503 S&P 500 companies.** The three outliers
(APA, STZ, ERIE) report their data under non-standard XBRL concepts that
can't be picked up with a generic script.

## Data sources

| What                                | Where                                                                      |
|-------------------------------------|----------------------------------------------------------------------------|
| S&P 500 ticker list                 | [Wikipedia](https://en.wikipedia.org/wiki/List_of_S%26P_500_companies)     |
| Ticker → CIK mapping                | `https://www.sec.gov/files/company_tickers.json`                           |
| Revenue + shares outstanding (XBRL) | `https://data.sec.gov/api/xbrl/companyfacts/CIK{cik}.json`                 |

## Usage

```bash
# 1. Install dependencies
pip install requests pandas lxml

# 2. Open main.py and set USER_AGENT at the top to your real name + email.
#    SEC requires this - requests without it may be rate-limited or blocked.

# 3. Run
python main.py
```

First run takes ~4 minutes (fetches the full company-facts JSON for 503
companies at a polite ~6 requests/second). Subsequent runs are
near-instant — results are cached in `cache/` for 30 days.

## Sample summary output

```
==================================================
SUMMARY
==================================================
Total S&P 500 companies:    503
  Success (rev + shares):   500
  Partial (missing fields): 3
  Failed (no SEC data):     0

CSV written to: sp500_revenue_per_share.csv
```

## How revenue and shares are extracted

Different companies report under different XBRL concepts depending on
their industry and reporting era. The script tries concepts in priority
order and falls back as needed.

### Revenue (`us-gaap` namespace)

1. `RevenueFromContractWithCustomerExcludingAssessedTax` *(current ASC 606 standard)*
2. `Revenues`
3. `SalesRevenueNet`
4. `RevenueFromContractWithCustomerIncludingAssessedTax`
5. `SalesRevenueGoodsNet`
6. `RevenuesNetOfInterestExpense` *(broker-dealers: Goldman Sachs, Morgan Stanley)*
7. `InterestAndDividendIncomeOperating` *(commercial banks)*
8. `InterestIncomeOperating` *(consumer finance)*

Only the most recent **FY** value from a **10-K** filing is used, and
zero values are filtered out.

> Note: for banks, using interest income approximates "total revenue"
> rather than replicating GAAP net revenue (net interest income +
> noninterest income), since no single XBRL tag captures that. The
> result is in the right order of magnitude but is not audited
> "total revenue" in the GAAP sense.

### Shares outstanding

1. `us-gaap:CommonStockSharesOutstanding`
2. `dei:EntityCommonStockSharesOutstanding`
3. `us-gaap:WeightedAverageNumberOfSharesOutstandingBasic` *(10-K, fallback for dual-class companies like META, Airbnb)*
4. `us-gaap:WeightedAverageNumberOfDilutedSharesOutstanding` *(10-K, last resort)*

Zero values filtered at every step.

## Why this isn't as simple as it sounds

The original spec was "get the revenue and shares outstanding for every
S&P 500 company." The catch, as companies report financials in their
10-Ks with wildly different conventions:

- Some report in **millions** (`$24,948`), some in **thousands**
  (`$24,948,000`), some in **actual dollars** (`$24,948,000,000`).
- Same variation for shares outstanding.
- Different industries use completely different revenue line items.

The script sidesteps the unit problem entirely by using SEC's **XBRL
companyfacts API**, which returns every value at its true scale
(absolute numbers) regardless of how the underlying filing presented
them. No manual unit normalization required.

## SEC rate-limit compliance

- `User-Agent` header with real contact info *(required by SEC)*
- ~6 requests per second *(SEC limit is 10/sec)*
- Every response cached to disk for 30 days

These defaults stay well under SEC's fair-access thresholds and should
never get flagged.

## Project layout

```
company-data-tool/
├── main.py                          # the script
├── README.md                        # this file
├── sp500_revenue_per_share.csv      # output CSV (checked in for reference)
├── .gitignore
└── cache/                           # gitignored; auto-generated on run
    ├── sp500.csv
    ├── ticker_cik.json
    └── company_facts/
        └── {CIK}.json               # one file per company
```

## Dependencies

- Python 3.10+ (uses PEP 604 `int | None` syntax)
- `requests` — HTTP client
- `pandas` — Wikipedia table parsing + CSV output
- `lxml` — HTML parser pandas uses under the hood
