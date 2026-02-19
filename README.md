# Medicaid Provider Fraud Signal Detection Engine

A tool that analyzes 227 million rows of CMS Medicaid provider spending data to detect fraud signals. Cross-references billing data with the OIG LEIE exclusion list and NPPES NPI registry.

## Fraud Signals Detected

| # | Signal | Severity | Description |
|---|--------|----------|-------------|
| 1 | Excluded Provider Billing | Critical | Providers on the OIG exclusion list still receiving Medicaid payments |
| 2 | Statistical Billing Outliers | High | Providers billing >3 standard deviations above the mean |
| 3 | Bust-Out Schemes | High | Recently enrolled providers with >500% billing ramp-up in 6 months |
| 4 | Impossible Service Volume | High | Providers with >500 claims per beneficiary per month |
| 5 | Home Health Billing Abuse | High | Home health providers with claims:beneficiary ratio >50:1 |
| 6 | Shell Entity Networks | Medium | Authorized officials controlling 5+ NPIs |

## Data Sources

1. **Medicaid Provider Spending** (2.9GB Parquet, 227M rows) — queried remotely via DuckDB httpfs
2. **OIG LEIE Exclusion List** (CSV) — downloaded locally
3. **NPPES NPI Registry** (1GB zip) — downloaded and converted to slim Parquet, or queried via API

## Requirements

- Python 3.10+
- DuckDB >= 1.4.0
- pandas >= 2.0.0
- requests >= 2.28.0
- 3.7GB+ RAM (uses DuckDB with 2GB memory limit)
- 2GB+ free disk space (for OIG CSV and NPPES slim parquet)
- Internet connection (queries remote parquet file)

## Setup

```bash
# Install dependencies (if not in venv)
pip install -r requirements.txt

# Download OIG exclusion list and NPPES data
bash setup.sh
```

## Usage

```bash
# Full run (downloads data if needed, runs all signals)
bash run.sh

# Or run directly
python3 detect_fraud.py
```

## Output

Produces `fraud_signals.json` with:
- Summary statistics (providers scanned, flagged, estimated overpayment)
- Per-provider details including NPI, name, entity type, taxonomy, state
- Individual fraud signals with evidence, severity, and overpayment estimates
- False Claims Act relevance with statute references and investigation steps

## Architecture

The tool downloads the 2.9GB Medicaid spending Parquet file locally, then runs all 6 fraud signal queries against the local copy for fast, reliable analysis. DuckDB's httpfs extension is used for the initial download. Each fraud signal runs as an independent SQL query, and results are merged and enriched with NPPES provider details (via API fallback when local data unavailable).

## Memory Management

- DuckDB memory limit: 2GB
- DuckDB threads: 2
- Local parquet cached after first download (~2.9GB)
- NPPES lookups via API with in-memory caching
- Results capped at 150-200 providers per signal to control memory

## Legal Framework

Fraud signals are mapped to the False Claims Act (31 U.S.C. 3729) with specific statute references:
- **3729(a)(1)(A)**: Knowingly presenting false claims
- **3729(a)(1)(B)**: Knowingly making false records/statements
- **3729(a)(1)(C)**: Conspiracy to commit fraud
- **3729(a)(1)(G)**: Knowingly concealing obligation to pay

## License

MIT
