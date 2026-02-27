#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"

echo "=== Medicaid Fraud Signal Detection Engine ==="
python3.10 "$SCRIPT_DIR/src/main.py" \
    --data-dir "$SCRIPT_DIR/data" \
    --output "$SCRIPT_DIR/fraud_signals.json" \
    --html "$SCRIPT_DIR/fraud_report.html" \
    --fof-json "$SCRIPT_DIR/fof_network_fraud.json" \
    --fof-html "$SCRIPT_DIR/fof_network_fraud.html" \
    "$@"
echo "=== Analysis complete ==="
echo "Output: $SCRIPT_DIR/fraud_signals.json"
echo "Report: $SCRIPT_DIR/fraud_report.html"
echo "FOF Network Report: $SCRIPT_DIR/fof_network_fraud.json"
echo "FOF Network Report: $SCRIPT_DIR/fof_network_fraud.html"
