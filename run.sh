#!/usr/bin/env bash
# run.sh — Run the Medicaid Fraud Signal Detection Engine
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PYTHON="/opt/autonomous-ai/venv/bin/python3"

echo "=== Medicaid Provider Fraud Signal Detection Engine ==="
echo "Working directory: ${SCRIPT_DIR}"
echo "Python: ${PYTHON}"
echo ""

# Check Python and dependencies
if ! "${PYTHON}" -c "import duckdb, pandas, requests" 2>/dev/null; then
    echo "ERROR: Required packages not found. Install with:"
    echo "  ${PYTHON} -m pip install -r ${SCRIPT_DIR}/requirements.txt"
    exit 1
fi

# Run setup if OIG CSV doesn't exist
if [ ! -f "${SCRIPT_DIR}/data/UPDATED.csv" ]; then
    echo "OIG exclusion list not found. Running setup..."
    bash "${SCRIPT_DIR}/setup.sh"
fi

# Run detection
echo ""
echo "Starting fraud detection..."
echo "This will query a 2.9GB remote parquet file. Expect 10-30 minutes."
echo ""

"${PYTHON}" "${SCRIPT_DIR}/detect_fraud.py"

# Check output
OUTPUT="${SCRIPT_DIR}/fraud_signals.json"
if [ -f "${OUTPUT}" ]; then
    echo ""
    echo "=== Output Summary ==="
    "${PYTHON}" -c "
import json
with open('${OUTPUT}') as f:
    data = json.load(f)
print(f'Generated at:          {data[\"generated_at\"]}')
print(f'Providers scanned:     {data[\"total_providers_scanned\"]:,}')
print(f'Providers flagged:     {data[\"total_providers_flagged\"]:,}')
print(f'Est. overpayment:      \${data[\"total_estimated_overpayment_usd\"]:,.2f}')
print(f'Output file:           ${OUTPUT}')
print(f'Output size:           {len(json.dumps(data)):,} bytes')
"
else
    echo "ERROR: fraud_signals.json was not generated."
    exit 1
fi
