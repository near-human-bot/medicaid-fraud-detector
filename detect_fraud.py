#!/usr/bin/env python3
"""
Legacy entry point — redirects to the modular architecture in src/.

For the full implementation, use:
    python3 -m src.main --data-dir data/ --output fraud_signals.json [--html report.html]
"""

import sys
import os

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

if __name__ == "__main__":
    print("This file is a legacy entry point.")
    print("Use the modular architecture instead:")
    print()
    print("  python3 -m src.main --data-dir data/ --output fraud_signals.json")
    print("  python3 -m src.main --data-dir data/ --output fraud_signals.json --html report.html")
    print()
    print("See README.md for full documentation.")
    sys.exit(0)
