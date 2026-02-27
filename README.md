# Medicaid Fraud Signal Detection Engine v3.0.0

**Based on [TheAuroraAI/medicaid-fraud-detector](https://github.com/TheAuroraAI/medicaid-fraud-detector).** The core architecture, signal design, and competition submission are their work — they deserve the majority of the competition credit. This fork extends their engine with additional signals and refinements.

Detects 19 types of fraud signals in Medicaid provider spending data by cross-referencing the HHS spending dataset (227M rows), OIG LEIE exclusion list, and CMS NPPES NPI registry.

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

| # | Signal | Severity | Description | Overpayment Methodology |
|---|--------|----------|-------------|------------------------|
| 1 | Excluded Provider | Critical | Provider on OIG exclusion list still billing Medicaid | 100% of post-exclusion payments (42 CFR 1001.1901) |
| 2 | Billing Outlier | High/Medium | Billing exceeds 99th percentile of taxonomy+state peer group | Amount exceeding peer 99th percentile |
| 3 | Rapid Escalation | High/Medium | New entity with >200% rolling 3-month billing growth | Sum of payments during >200% growth months |
| 4 | Workforce Impossibility | High | Organization billing >6 claims/hour (physically impossible) | Excess claims beyond 6/hr threshold x avg cost |
| 5 | Shared Official | High/Medium | One person controls 5+ NPIs with >$1M combined billing | 10-20% of combined network billing (DOJ data) |
| 6 | Geographic Implausibility | Medium | Home health provider with <0.1 beneficiary/claims ratio | Excess claims beyond 1:10 bene-to-claims ratio |
| 7 | Address Clustering | High/Medium | 10+ NPIs at same zip code with >$5M combined billing | 15% of combined billing (OIG ghost office data) |
| 8 | Upcoding | High/Medium | Provider billing >80% high-complexity E&M codes vs <30% peer avg | 30% uplift on excess high-level billing |
| 9 | Concurrent Billing | High/Medium | Individual provider billing in 5+ states in same month | 60% of flagged payments (phantom billing) |

## Composite Risk Score

Each flagged provider receives a composite risk score (0-100) combining:
- **Signal breadth** — how many different signal types triggered (max 30 pts)
- **Severity weight** — weighted sum of signal severities x signal-type risk (max 40 pts)
- **Overpayment ratio** — estimated overpayment as % of total billing (max 30 pts)

Risk tiers:
- **Critical** (75-100) — immediate investigation priority
- **High** (50-74) — investigate within 30 days
- **Medium** (25-49) — review within 90 days
- **Low** (0-24) — monitor and reassess

## Cross-Signal Correlation Analysis

The engine identifies providers flagged by multiple independent signals — the highest-priority investigation targets. The report includes:
- Provider distribution by signal count
- Most common signal pair co-occurrences
- Multi-signal provider NPI lists for prioritized review

## Output Formats

### JSON (`fraud_signals.json`)
Complete structured output including:
- **Methodology documentation** — per-signal methodology, thresholds, and overpayment basis
- **Cross-signal analysis** — provider overlap across signal types
- **Executive summary** — aggregate stats, tier distribution, top states, highest-risk providers
- **Provider records** — signals, risk scores, case narratives, FCA relevance
- All evidence with severity classification and statute references

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
  signals.py   — All 9 signal implementations as SQL queries + cross-signal analysis
  output.py    — JSON/HTML report, risk scoring, case narratives, executive summary, methodology
  main.py      — CLI entry point
tests/
  conftest.py      — Synthetic data fixtures (triggers all 9 signals)
  test_signals.py  — 80 unit tests covering all signals, risk scoring, narratives, HTML, edge cases
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

All 80 tests use synthetic data fixtures that trigger each signal, verifying:
- Detection logic and threshold compliance for all 9 signals
- Severity classification (critical/high/medium)
- Overpayment calculations (per-signal methodology)
- Composite risk scoring and tier assignment
- Case narrative generation
- Executive summary statistics
- HTML report output and XSS safety
- Cross-signal correlation analysis
- Edge cases (empty inputs, zero totals, negative values)
- FCA statute references and claim type mappings
- Multi-signal provider handling

## Output Schema

See `fraud_signals.json` for the complete output. Each flagged provider includes:
- All signal evidence with severity classification
- **Composite risk score** (0-100) with tier, contributing factors
- **Plain-English case narrative** summarizing all findings
- Estimated overpayment in USD (per-signal formulas)
- FCA statute references (31 U.S.C. § 3729)
- Actionable next steps for qui tam / FCA lawyers

The report also includes:
- **Methodology documentation** — per-signal description, SQL methodology, overpayment basis, and threshold
- **Cross-signal analysis** — multi-signal provider identification and pair co-occurrence
- **Executive summary** — aggregate statistics, risk tier distribution, top states, highest-risk providers

## Requirements

- Python 3.11+
- DuckDB, Polars, PyArrow (pip-installable)
- Works on Ubuntu 22.04+ and macOS 14+ (Apple Silicon)
