"""Main entry point for the Medicaid Fraud Signal Detection Engine."""

import argparse
import sys
import time
import os

# Add project root to path
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from src.ingest import load_all
from src.signals import run_all_signals
from src.output import generate_report, write_report, write_html_report, write_fof_report, write_fof_html_report


def main():
    parser = argparse.ArgumentParser(
        description="Medicaid Fraud Signal Detection Engine"
    )
    parser.add_argument(
        "--data-dir",
        required=True,
        help="Directory containing input data files",
    )
    parser.add_argument(
        "--output",
        default="fraud_signals.json",
        help="Output JSON file path (default: fraud_signals.json)",
    )
    parser.add_argument(
        "--html",
        default=None,
        help="Output HTML report path (optional)",
    )
    parser.add_argument(
        "--fof-json",
        default=None,
        help="Output path for Feeding Our Future network fraud JSON report (optional)",
    )
    parser.add_argument(
        "--fof-html",
        default=None,
        help="Output path for Feeding Our Future network fraud HTML report (optional)",
    )
    parser.add_argument(
        "--memory-limit",
        default="2GB",
        help="DuckDB memory limit (default: 2GB)",
    )
    parser.add_argument(
        "--no-gpu",
        action="store_true",
        help="CPU-only mode (default behavior, GPU not used)",
    )
    args = parser.parse_args()

    start_time = time.time()

    # Load data
    print("=" * 60)
    print("Medicaid Fraud Signal Detection Engine v3.0.0")
    print("=" * 60)

    con = load_all(args.data_dir, args.memory_limit)

    # Get total unique providers scanned
    total_providers = con.execute("""
        SELECT COUNT(DISTINCT billing_npi) FROM spending
    """).fetchone()[0]
    print(f"\nTotal unique billing providers: {total_providers:,}")

    # Run all signals
    print("\nRunning fraud signal detection...")
    signal_results = run_all_signals(con)

    # Generate report
    print("\nGenerating report...")
    report = generate_report(signal_results, con, total_providers)

    # Write JSON output
    write_report(report, args.output)

    # Write HTML output if requested
    if args.html:
        write_html_report(report, args.html)

    # Write FOF network fraud reports if requested
    if args.fof_json:
        write_fof_report(report, args.fof_json)
    if args.fof_html:
        write_fof_html_report(report, args.fof_html)

    elapsed = time.time() - start_time
    minutes = int(elapsed // 60)
    seconds = int(elapsed % 60)
    print(f"\nTotal runtime: {minutes}m {seconds}s")

    con.close()


if __name__ == "__main__":
    main()
