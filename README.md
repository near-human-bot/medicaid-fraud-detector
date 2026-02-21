# Medicaid Fraud Signal Detection Engine

Detects 9 types of fraud signals in Medicaid provider spending data by cross-referencing the HHS spending dataset (227M rows), OIG LEIE exclusion list, and CMS NPPES NPI registry.

Built with DuckDB for memory-efficient out-of-core analytics — runs on machines with as little as 2GB available RAM.

## Quick Start

```bash
# Install dependencies and download data
./setup.sh

# Run analysis
./run.sh
# Output: fraud_signals.json

# Also generate HTML report
python3 -m src.main --data-dir data/ --output fraud_signals.json --html report.html
```

## Fraud Signals Detected

| # | Signal | Severity | Description |
|---|--------|----------|-------------|
| 1 | Excluded Provider | Critical | Provider on OIG exclusion list still billing Medicaid |
| 2 | Billing Outlier | High/Medium | Provider billing exceeds 99th percentile of taxonomy+state peer group |
| 3 | Rapid Escalation | High/Medium | New entity with >200% rolling 3-month billing growth |
| 4 | Workforce Impossibility | High | Organization billing >6 claims/hour (physically impossible volume) |
| 5 | Shared Official | High/Medium | One person controls 5+ NPIs with >$1M combined billing |
| 6 | Geographic Implausibility | Medium | Home health provider with <0.1 beneficiary/claims ratio |
| 7 | Address Clustering | High/Medium | 10+ NPIs at same zip code with >$5M combined billing |
| 8 | Upcoding | High/Medium | Provider billing >80% high-complexity E&M codes vs <30% peer average |
| 9 | Concurrent Billing | High/Medium | Individual provider billing in 5+ states in same month |

## Composite Risk Score

Each flagged provider receives a composite risk score (0-100) combining:
- **Signal breadth** — how many different signal types triggered (max 30 pts)
- **Severity weight** — weighted sum of signal severities x signal-type risk (max 40 pts)
- **Overpayment ratio** — estimated overpayment as % of total billing (max 30 pts)

Risk tiers: **Critical** (75-100), **High** (50-74), **Medium** (25-49), **Low** (0-24)

## Output Formats

### JSON (`fraud_signals.json`)
Complete structured output with all provider records, signal evidence, risk scores, case narratives, and executive summary. See Output Schema below.

### HTML Report (`--html report.html`)
Visual dashboard with:
- Executive summary cards (scanned, flagged, overpayment, critical count)
- Risk tier distribution table
- Signal type summary
- Top states by flags
- Detailed provider cards with risk score bars, narratives, and FCA guidance

## Architecture

```
src/
  ingest.py    — DuckDB data loading (parquet, CSV, zip streaming)
  signals.py   — All 9 signal implementations as SQL queries
  output.py    — JSON/HTML report, risk scoring, case narratives, executive summary
  main.py      — CLI entry point
tests/
  conftest.py      — Synthetic data fixtures (triggers all 9 signals)
  test_signals.py  — 55 unit tests (all signals + risk score + narratives + HTML)
```

## Data Sources

| Dataset | Size | Format | Source |
|---------|------|--------|--------|
| Medicaid Spending | 2.9 GB | Parquet | HHS STOP |
| OIG LEIE | 15 MB | CSV | oig.hhs.gov |
| NPPES NPI Registry | 1 GB (zip) | CSV → Parquet | CMS |

The tool automatically builds a slim NPPES parquet (~177 MB) from the full 11 GB CSV, extracting only the 11 columns needed.

## Memory Management

DuckDB processes the 2.9 GB parquet file out-of-core without loading it into RAM. Default memory limit is 2 GB.

```bash
# Adjust for your hardware
./run.sh --memory-limit 8GB    # More RAM = faster
./run.sh --memory-limit 1GB    # Constrained environments
./run.sh --no-gpu              # CPU-only (default, GPU not used)
```

## Testing

```bash
pytest tests/ -v
```

All 55 tests use synthetic data fixtures that trigger each signal, verifying detection logic, severity classification, overpayment calculations, risk scoring, case narratives, HTML output, and output format compliance.

## Output Schema

See `fraud_signals.json` for the complete output. Each flagged provider includes:
- All signal evidence with severity classification
- **Composite risk score** (0-100) with tier and contributing factors
- **Plain-English case narrative** summarizing all findings
- Estimated overpayment in USD (per-signal formulas from spec)
- FCA statute references (31 U.S.C. § 3729)
- Actionable next steps for qui tam / FCA lawyers

The report also includes an **executive summary** with:
- Aggregate statistics (providers scanned, flagged, total overpayment)
- Risk tier distribution
- Top states by flags
- Highest-risk providers
- Signal type breakdown

## Requirements

- Python 3.11+
- DuckDB, Polars, PyArrow (pip-installable)
- Works on Ubuntu 22.04+ and macOS 14+ (Apple Silicon)
