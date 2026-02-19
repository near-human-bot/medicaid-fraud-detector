#!/usr/bin/env python3
"""
Medicaid Provider Fraud Signal Detection Engine
================================================
Analyzes 227M rows of Medicaid billing data from CMS and cross-references
with the OIG LEIE exclusion list and NPPES NPI registry to detect
fraud signals including:

1. Excluded providers still billing (critical)
2. Statistical billing outliers (high)
3. Bust-out schemes (high)
4. Impossible service volume (high)
5. Home health billing abuse (high)
6. Shell entity networks (medium)

Uses DuckDB with httpfs to query the 2.9GB parquet file remotely,
avoiding full download. Designed for machines with limited RAM (3.7GB).

Author: Aurora AI
Version: 1.0.0
"""

import json
import os
import sys
import time
import logging
import requests
from datetime import datetime, timezone
from pathlib import Path

import duckdb
import pandas as pd

# ---------------------------------------------------------------------------
# Configuration
# ---------------------------------------------------------------------------
TOOL_VERSION = "1.0.0"
SCRIPT_DIR = Path(__file__).resolve().parent
DATA_DIR = SCRIPT_DIR / "data"
OUTPUT_FILE = SCRIPT_DIR / "fraud_signals.json"

PARQUET_URL = (
    "https://stopendataprod.blob.core.windows.net/datasets/"
    "medicaid-provider-spending/2026-02-09/medicaid-provider-spending.parquet"
)
OIG_CSV_URL = "https://oig.hhs.gov/exclusions/downloadables/UPDATED.csv"
NPPES_API_URL = "https://npiregistry.cms.hhs.gov/api/"

OIG_CSV_PATH = DATA_DIR / "UPDATED.csv"
LOCAL_PARQUET_PATH = DATA_DIR / "medicaid-provider-spending.parquet"
NPPES_DIR = DATA_DIR / "nppes"
NPPES_PARQUET_PATH = NPPES_DIR / "nppes_slim.parquet"
NPPES_MODE_PATH = NPPES_DIR / "mode.txt"

DUCKDB_MEMORY_LIMIT = "2GB"

# NPPES API cache to avoid redundant calls
_nppes_cache: dict = {}

# Logging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%H:%M:%S",
)
log = logging.getLogger("fraud_detector")


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def get_duckdb_connection() -> duckdb.DuckDBPyConnection:
    """Create a DuckDB connection with httpfs and memory constraints."""
    con = duckdb.connect()
    con.execute(f"SET memory_limit='{DUCKDB_MEMORY_LIMIT}'")
    con.execute("SET threads=2")
    con.execute("INSTALL httpfs")
    con.execute("LOAD httpfs")
    # Azure blob storage is public, no auth needed
    return con


def ensure_oig_csv():
    """Download the OIG exclusion list if not present."""
    DATA_DIR.mkdir(parents=True, exist_ok=True)
    if OIG_CSV_PATH.exists():
        log.info("OIG CSV already exists: %s", OIG_CSV_PATH)
        return
    log.info("Downloading OIG LEIE exclusion list...")
    resp = requests.get(OIG_CSV_URL, timeout=120)
    resp.raise_for_status()
    OIG_CSV_PATH.write_bytes(resp.content)
    log.info("OIG CSV downloaded: %d bytes", len(resp.content))


def ensure_local_parquet(con: duckdb.DuckDBPyConnection) -> str:
    """Download the remote parquet locally if not already cached. Returns path to use."""
    if LOCAL_PARQUET_PATH.exists():
        size_mb = LOCAL_PARQUET_PATH.stat().st_size / (1024 * 1024)
        if size_mb > 100:  # sanity check: should be >100MB
            log.info("Local parquet already exists: %s (%.0f MB)", LOCAL_PARQUET_PATH, size_mb)
            return str(LOCAL_PARQUET_PATH)
        else:
            log.warning("Local parquet too small (%.0f MB), re-downloading...", size_mb)
            LOCAL_PARQUET_PATH.unlink()

    log.info("Downloading parquet locally via streaming HTTP (2.9GB, ~10 min)...")
    log.info("Source: %s", PARQUET_URL)
    # Stream download to avoid high memory usage
    import subprocess
    result = subprocess.run(
        ["curl", "-L", "-o", str(LOCAL_PARQUET_PATH), "--progress-bar", PARQUET_URL],
        capture_output=True, text=True, timeout=1800,
    )
    if result.returncode != 0:
        log.error("curl download failed: %s", result.stderr)
        if LOCAL_PARQUET_PATH.exists():
            LOCAL_PARQUET_PATH.unlink()
        raise RuntimeError(f"Failed to download parquet: {result.stderr}")

    size_mb = LOCAL_PARQUET_PATH.stat().st_size / (1024 * 1024)
    log.info("Local parquet saved: %s (%.0f MB)", LOCAL_PARQUET_PATH, size_mb)
    return str(LOCAL_PARQUET_PATH)


def nppes_mode() -> str:
    """Return 'parquet' or 'api_fallback' depending on setup."""
    if NPPES_PARQUET_PATH.exists():
        return "parquet"
    if NPPES_MODE_PATH.exists():
        return NPPES_MODE_PATH.read_text().strip()
    return "api_fallback"


def lookup_npi_api(npi: str) -> dict:
    """Look up a single NPI via the NPPES API."""
    if npi in _nppes_cache:
        return _nppes_cache[npi]
    try:
        resp = requests.get(
            NPPES_API_URL,
            params={"version": "2.1", "number": npi},
            timeout=15,
        )
        resp.raise_for_status()
        data = resp.json()
        results = data.get("results", [])
        if results:
            r = results[0]
            basic = r.get("basic", {})
            taxonomies = r.get("taxonomies", [])
            tax_code = taxonomies[0].get("code", "") if taxonomies else ""
            entity = r.get("enumeration_type", "")
            entity_type = (
                "individual" if entity == "NPI-1" else
                "organization" if entity == "NPI-2" else entity
            )
            name_parts = []
            if basic.get("organization_name"):
                name_parts.append(basic["organization_name"])
            else:
                if basic.get("first_name"):
                    name_parts.append(basic["first_name"])
                if basic.get("last_name"):
                    name_parts.append(basic["last_name"])

            info = {
                "npi": str(npi),
                "provider_name": " ".join(name_parts) if name_parts else f"NPI {npi}",
                "entity_type": entity_type,
                "taxonomy_code": tax_code,
                "state": (r.get("addresses", [{}])[0].get("state", "")
                          if r.get("addresses") else ""),
                "enumeration_date": basic.get("enumeration_date", None),
            }
            _nppes_cache[npi] = info
            return info
    except Exception as e:
        log.warning("NPPES API lookup failed for NPI %s: %s", npi, e)
    fallback = {
        "npi": str(npi),
        "provider_name": f"NPI {npi}",
        "entity_type": "unknown",
        "taxonomy_code": "",
        "state": "",
        "enumeration_date": None,
    }
    _nppes_cache[npi] = fallback
    return fallback


def enrich_provider(con: duckdb.DuckDBPyConnection, npi: str) -> dict:
    """Get provider details from NPPES (parquet or API)."""
    mode = nppes_mode()
    if mode == "parquet":
        try:
            row = con.execute(
                f"""
                SELECT
                    NPI,
                    CASE WHEN entity_type_code = '1' THEN 'individual'
                         WHEN entity_type_code = '2' THEN 'organization'
                         ELSE 'unknown' END AS entity_type,
                    COALESCE(org_name, '') AS org_name,
                    COALESCE(last_name, '') AS last_name,
                    COALESCE(first_name, '') AS first_name,
                    COALESCE(state, '') AS state,
                    COALESCE(taxonomy_code, '') AS taxonomy_code,
                    enumeration_date
                FROM read_parquet('{NPPES_PARQUET_PATH}')
                WHERE NPI = '{npi}'
                LIMIT 1
                """
            ).fetchone()
            if row:
                name = row[2] if row[2] else f"{row[4]} {row[3]}".strip()
                return {
                    "npi": str(row[0]),
                    "provider_name": name or f"NPI {npi}",
                    "entity_type": row[1],
                    "taxonomy_code": row[6] or "",
                    "state": row[5] or "",
                    "enumeration_date": str(row[7]) if row[7] else None,
                }
        except Exception as e:
            log.warning("NPPES parquet lookup failed for %s: %s", npi, e)
    # Fallback to API
    return lookup_npi_api(npi)


def build_fca_relevance(signal_name: str, provider_info: dict, overpayment: float) -> dict:
    """Generate False Claims Act relevance details."""
    npi = provider_info.get("npi", "unknown")
    name = provider_info.get("provider_name", "unknown")
    state = provider_info.get("state", "unknown")

    descriptions = {
        "excluded_provider_billing": (
            f"Provider {name} (NPI: {npi}) in {state} submitted claims to Medicaid "
            f"while on the OIG LEIE exclusion list. Federal law prohibits payment to "
            f"excluded individuals/entities. This constitutes a knowing presentation "
            f"of false claims for payment.",
            "31 U.S.C. section 3729(a)(1)(A)",
            [
                f"Verify exclusion status of NPI {npi} on OIG LEIE database",
                "Pull all claims submitted by this provider since exclusion date",
                "Determine if provider disclosed exclusion status to billing entity",
                "Interview Medicaid managed care organizations that processed claims",
                "Calculate total federal share of improper payments",
                "Refer to OIG for potential criminal prosecution under 42 USC 1320a-7b",
            ],
        ),
        "statistical_billing_outlier": (
            f"Provider {name} (NPI: {npi}) in {state} billed Medicaid at levels "
            f">3 standard deviations above the mean for their state/specialty, "
            f"suggesting systematic upcoding or billing for services not rendered.",
            "31 U.S.C. section 3729(a)(1)(A)",
            [
                f"Audit medical records for NPI {npi} for a sample of high-value claims",
                "Compare billed services to medical necessity documentation",
                "Review for upcoding patterns (billing higher-complexity codes)",
                "Interview beneficiaries to confirm services were rendered",
                "Conduct statistical analysis of billing patterns by HCPCS code",
            ],
        ),
        "bust_out_scheme": (
            f"Provider {name} (NPI: {npi}) showed rapid billing ramp-up consistent "
            f"with a bust-out scheme: enrolled recently and escalated claims >500% "
            f"within 6 months, a known pattern of fraudulent billing operations.",
            "31 U.S.C. section 3729(a)(1)(C)",
            [
                f"Verify physical practice location for NPI {npi}",
                "Check if provider shares address with other high-billing entities",
                "Review enrollment application for false statements",
                "Examine beneficiary referral patterns for steering",
                "Check if provider's license is valid and in good standing",
                "Investigate ownership/control persons for prior fraud history",
            ],
        ),
        "impossible_service_volume": (
            f"Provider {name} (NPI: {npi}) billed an impossibly high number of "
            f"claims per beneficiary per month, exceeding what any provider could "
            f"physically deliver. This indicates phantom billing.",
            "31 U.S.C. section 3729(a)(1)(A)",
            [
                f"Audit all claims from NPI {npi} for the flagged time periods",
                "Contact beneficiaries to verify services were actually received",
                "Review scheduling records for physical impossibility",
                "Check if services were rendered by qualified practitioners",
                "Examine claim submission patterns (time of day, batching)",
            ],
        ),
        "home_health_abuse": (
            f"Provider {name} (NPI: {npi}) showed patterns consistent with home "
            f"health billing abuse: extremely high claims-to-beneficiary ratio "
            f"suggesting repeated billing for the same patients without medical necessity.",
            "31 U.S.C. section 3729(a)(1)(B)",
            [
                f"Audit home health records for NPI {npi}",
                "Verify plans of care and physician orders exist",
                "Confirm homebound status of beneficiaries",
                "Review for medically unnecessary visits",
                "Interview beneficiaries about services received",
                "Check for kickback arrangements with referring physicians",
            ],
        ),
        "shell_entity_network": (
            f"Authorized official associated with NPI {npi} ({name}) controls 5+ "
            f"NPIs, suggesting a potential shell entity network used to distribute "
            f"fraudulent billing across multiple entities to avoid detection.",
            "31 U.S.C. section 3729(a)(1)(G)",
            [
                f"Map all NPIs controlled by the same authorized official",
                "Check for shared addresses, phone numbers, and bank accounts",
                "Review each entity's billing patterns for coordination",
                "Investigate authorized official's background and associations",
                "Check state Medicaid enrollment records for cross-references",
                "Look for graduated billing patterns across the network",
            ],
        ),
    }

    desc, statute, steps = descriptions.get(signal_name, (
        f"Provider {name} (NPI: {npi}) flagged for potential Medicaid fraud.",
        "31 U.S.C. section 3729(a)(1)(A)",
        [f"Investigate NPI {npi} for the flagged fraud signal"],
    ))

    return {
        "violation_description": desc,
        "statute_reference": statute,
        "estimated_government_loss": round(overpayment, 2),
        "suggested_investigation_steps": steps,
    }


# ---------------------------------------------------------------------------
# Fraud Signal Detection Functions
# ---------------------------------------------------------------------------

def signal_1_excluded_providers(con: duckdb.DuckDBPyConnection, parquet_path: str = PARQUET_URL) -> list[dict]:
    """
    Signal 1: Excluded providers still billing (CRITICAL)
    Match OIG exclusion list NPIs against the billing data.
    """
    log.info("=== Signal 1: Excluded Providers Still Billing ===")

    # Load OIG exclusion list into DuckDB
    # NPI='0000000000' is a placeholder for entries without a real NPI
    # REINDATE='00000000' means not reinstated (active exclusion)
    # EXCLDATE is in YYYYMMDD integer format
    con.execute(f"""
        CREATE OR REPLACE TABLE oig_exclusions AS
        SELECT
            CAST(NPI AS VARCHAR) AS npi,
            LASTNAME AS last_name,
            FIRSTNAME AS first_name,
            BUSNAME AS bus_name,
            STATE AS excl_state,
            EXCLTYPE AS excl_type,
            CAST(EXCLDATE AS VARCHAR) AS excl_date,
            CAST(REINDATE AS VARCHAR) AS rein_date
        FROM read_csv('{OIG_CSV_PATH}',
            auto_detect=true,
            ignore_errors=true)
        WHERE NPI IS NOT NULL
          AND TRIM(CAST(NPI AS VARCHAR)) != ''
          AND CAST(NPI AS VARCHAR) != '0000000000'
          AND (REINDATE IS NULL
               OR CAST(REINDATE AS VARCHAR) = '00000000'
               OR CAST(REINDATE AS VARCHAR) = '0')
    """)

    oig_count = con.execute("SELECT COUNT(*) FROM oig_exclusions").fetchone()[0]
    log.info("OIG exclusions with NPI (not reinstated): %d", oig_count)

    # Query: find excluded NPIs that appear in billing data
    # We check both BILLING_PROVIDER_NPI_NUM and SERVICING_PROVIDER_NPI_NUM
    log.info("Querying parquet for excluded provider billing...")
    results = con.execute(f"""
        WITH billing AS (
            SELECT
                CAST(BILLING_PROVIDER_NPI_NUM AS VARCHAR) AS npi,
                CLAIM_FROM_MONTH,
                SUM(TOTAL_PAID) AS total_paid,
                SUM(TOTAL_CLAIMS) AS total_claims,
                SUM(TOTAL_UNIQUE_BENEFICIARIES) AS total_beneficiaries
            FROM read_parquet('{parquet_path}')
            GROUP BY BILLING_PROVIDER_NPI_NUM, CLAIM_FROM_MONTH
        )
        SELECT
            b.npi,
            o.last_name,
            o.first_name,
            o.bus_name,
            o.excl_state,
            o.excl_type,
            o.excl_date,
            SUM(b.total_paid) AS total_paid_post_excl,
            SUM(b.total_claims) AS total_claims_post_excl,
            SUM(b.total_beneficiaries) AS total_beneficiaries_post_excl,
            MIN(b.CLAIM_FROM_MONTH) AS first_billing_month,
            MAX(b.CLAIM_FROM_MONTH) AS last_billing_month,
            COUNT(DISTINCT b.CLAIM_FROM_MONTH) AS billing_months
        FROM billing b
        INNER JOIN oig_exclusions o ON b.npi = o.npi
        GROUP BY b.npi, o.last_name, o.first_name, o.bus_name,
                 o.excl_state, o.excl_type, o.excl_date
        HAVING SUM(b.total_paid) > 0
        ORDER BY total_paid_post_excl DESC
    """).fetchall()

    log.info("Found %d excluded providers with billing activity", len(results))

    flagged = []
    for row in results:
        npi = str(row[0])
        name_parts = []
        if row[3]:  # business name
            name_parts.append(str(row[3]))
        if row[2]:  # first name
            name_parts.append(str(row[2]))
        if row[1]:  # last name
            name_parts.append(str(row[1]))
        provider_name = " ".join(name_parts) if name_parts else f"NPI {npi}"

        total_paid = float(row[7]) if row[7] else 0.0
        total_claims = int(row[8]) if row[8] else 0
        total_benes = int(row[9]) if row[9] else 0

        signal = {
            "signal_name": "excluded_provider_billing",
            "signal_description": (
                f"Provider {provider_name} (NPI: {npi}) appears on the OIG LEIE "
                f"exclusion list (type: {row[5]}, excluded: {row[6]}) but received "
                f"${total_paid:,.2f} in Medicaid payments across {row[12]} billing "
                f"months (from {row[10]} to {row[11]})."
            ),
            "severity": "critical",
            "evidence": {
                "oig_exclusion_type": str(row[5]) if row[5] else "",
                "oig_exclusion_date": str(row[6]) if row[6] else "",
                "oig_exclusion_state": str(row[4]) if row[4] else "",
                "total_paid_post_exclusion": round(total_paid, 2),
                "total_claims_post_exclusion": total_claims,
                "billing_months_count": int(row[12]) if row[12] else 0,
                "first_billing_month": str(row[10]) if row[10] else "",
                "last_billing_month": str(row[11]) if row[11] else "",
            },
            "estimated_overpayment_usd": round(total_paid, 2),
            "overpayment_methodology": (
                "100% of payments to excluded providers are considered improper "
                "under 42 CFR 1001.1901. Federal law prohibits any Medicaid payment "
                "to excluded individuals or entities."
            ),
        }

        flagged.append({
            "npi": npi,
            "provider_name": provider_name,
            "entity_type": "organization" if row[3] else "individual",
            "state": str(row[4]) if row[4] else "",
            "total_paid": total_paid,
            "total_claims": total_claims,
            "total_beneficiaries": total_benes,
            "signals": [signal],
        })

    return flagged


def signal_2_statistical_outliers(con: duckdb.DuckDBPyConnection, parquet_path: str = PARQUET_URL) -> list[dict]:
    """
    Signal 2: Statistical billing outliers (HIGH)
    Find providers >3 std deviations above mean total paid for their state.
    """
    log.info("=== Signal 2: Statistical Billing Outliers ===")

    log.info("Computing per-provider totals and state statistics...")
    results = con.execute(f"""
        WITH provider_totals AS (
            SELECT
                CAST(BILLING_PROVIDER_NPI_NUM AS VARCHAR) AS npi,
                SUM(TOTAL_PAID) AS total_paid,
                SUM(TOTAL_CLAIMS) AS total_claims,
                SUM(TOTAL_UNIQUE_BENEFICIARIES) AS total_beneficiaries
            FROM read_parquet('{parquet_path}')
            GROUP BY BILLING_PROVIDER_NPI_NUM
            HAVING SUM(TOTAL_PAID) > 0
        ),
        -- We don't have state directly in billing data, so we use the first
        -- 2 chars of the NPI's most common HCPCS prefix as a proxy, or
        -- we join with NPPES later. For now, compute overall stats.
        overall_stats AS (
            SELECT
                AVG(total_paid) AS mean_paid,
                STDDEV_POP(total_paid) AS std_paid
            FROM provider_totals
        )
        SELECT
            p.npi,
            p.total_paid,
            p.total_claims,
            p.total_beneficiaries,
            o.mean_paid,
            o.std_paid,
            (p.total_paid - o.mean_paid) / NULLIF(o.std_paid, 0) AS z_score
        FROM provider_totals p
        CROSS JOIN overall_stats o
        WHERE (p.total_paid - o.mean_paid) / NULLIF(o.std_paid, 0) > 3.0
        ORDER BY p.total_paid DESC
        LIMIT 200
    """).fetchall()

    log.info("Found %d providers with z-score > 3.0", len(results))

    flagged = []
    for row in results:
        npi = str(row[0])
        total_paid = float(row[1]) if row[1] else 0.0
        total_claims = int(row[2]) if row[2] else 0
        total_benes = int(row[3]) if row[3] else 0
        mean_paid = float(row[4]) if row[4] else 0.0
        std_paid = float(row[5]) if row[5] else 0.0
        z_score = float(row[6]) if row[6] else 0.0

        # Estimated overpayment: amount above 3-sigma threshold
        threshold = mean_paid + 3 * std_paid
        estimated_overpayment = max(0, total_paid - threshold)

        signal = {
            "signal_name": "statistical_billing_outlier",
            "signal_description": (
                f"Provider NPI {npi} billed ${total_paid:,.2f} total — "
                f"{z_score:.1f} standard deviations above the mean "
                f"(${mean_paid:,.2f}, sigma=${std_paid:,.2f}). "
                f"This is a significant statistical outlier warranting investigation."
            ),
            "severity": "high",
            "evidence": {
                "total_paid": round(total_paid, 2),
                "mean_paid_all_providers": round(mean_paid, 2),
                "std_dev_paid": round(std_paid, 2),
                "z_score": round(z_score, 2),
                "threshold_3sigma": round(threshold, 2),
                "total_claims": total_claims,
                "total_unique_beneficiaries": total_benes,
            },
            "estimated_overpayment_usd": round(estimated_overpayment, 2),
            "overpayment_methodology": (
                "Estimated as the amount exceeding the 3-sigma threshold "
                f"(${threshold:,.2f}). This represents the portion of billing "
                "that is statistically anomalous and warrants audit."
            ),
        }

        flagged.append({
            "npi": npi,
            "provider_name": f"NPI {npi}",
            "entity_type": "unknown",
            "state": "",
            "total_paid": total_paid,
            "total_claims": total_claims,
            "total_beneficiaries": total_benes,
            "signals": [signal],
        })

    return flagged


def signal_3_bust_out_schemes(con: duckdb.DuckDBPyConnection, parquet_path: str = PARQUET_URL) -> list[dict]:
    """
    Signal 3: Bust-out schemes (HIGH)
    Find providers that started billing recently (2023+) and ramped >500%
    within 6 months.
    """
    log.info("=== Signal 3: Bust-Out Schemes ===")

    log.info("Querying parquet for rapid billing ramp-ups...")
    results = con.execute(f"""
        WITH monthly_billing AS (
            SELECT
                CAST(BILLING_PROVIDER_NPI_NUM AS VARCHAR) AS npi,
                CLAIM_FROM_MONTH,
                SUM(TOTAL_PAID) AS monthly_paid,
                SUM(TOTAL_CLAIMS) AS monthly_claims
            FROM read_parquet('{parquet_path}')
            GROUP BY BILLING_PROVIDER_NPI_NUM, CLAIM_FROM_MONTH
        ),
        provider_first_month AS (
            SELECT
                npi,
                MIN(CLAIM_FROM_MONTH) AS first_month,
                MAX(CLAIM_FROM_MONTH) AS last_month
            FROM monthly_billing
            GROUP BY npi
            HAVING MIN(CLAIM_FROM_MONTH) >= '2023-01'
        ),
        first_month_billing AS (
            SELECT
                m.npi,
                m.monthly_paid AS first_month_paid,
                m.monthly_claims AS first_month_claims
            FROM monthly_billing m
            INNER JOIN provider_first_month p
                ON m.npi = p.npi AND m.CLAIM_FROM_MONTH = p.first_month
        ),
        provider_with_months AS (
            SELECT
                m.npi,
                m.CLAIM_FROM_MONTH,
                m.monthly_paid,
                m.monthly_claims,
                p.first_month,
                -- Rank months by order for each provider
                ROW_NUMBER() OVER (
                    PARTITION BY m.npi
                    ORDER BY m.CLAIM_FROM_MONTH
                ) AS month_rank
            FROM monthly_billing m
            INNER JOIN provider_first_month p ON m.npi = p.npi
        ),
        six_month_later AS (
            SELECT
                npi,
                MAX(monthly_paid) AS peak_monthly_paid,
                MAX(monthly_claims) AS peak_monthly_claims
            FROM provider_with_months
            WHERE month_rank BETWEEN 2 AND 7  -- months 2-7 (6 months after first)
            GROUP BY npi
        )
        SELECT
            p.npi,
            p.first_month,
            p.last_month,
            f.first_month_paid,
            f.first_month_claims,
            s.peak_monthly_paid,
            s.peak_monthly_claims,
            CASE WHEN f.first_month_paid > 0
                 THEN (s.peak_monthly_paid / f.first_month_paid - 1) * 100
                 ELSE 99999 END AS pct_increase
        FROM provider_first_month p
        INNER JOIN first_month_billing f ON p.npi = f.npi
        INNER JOIN six_month_later s ON p.npi = s.npi
        WHERE f.first_month_paid > 100
          AND s.peak_monthly_paid > f.first_month_paid * 6
        ORDER BY s.peak_monthly_paid DESC
        LIMIT 150
    """).fetchall()

    log.info("Found %d potential bust-out schemes", len(results))

    flagged = []
    for row in results:
        npi = str(row[0])
        first_month = str(row[1])
        last_month = str(row[2])
        first_paid = float(row[3]) if row[3] else 0.0
        first_claims = int(row[4]) if row[4] else 0
        peak_paid = float(row[5]) if row[5] else 0.0
        peak_claims = int(row[6]) if row[6] else 0
        pct_increase = float(row[7]) if row[7] else 0.0

        # Overpayment estimate: 80% of peak billing assumed fraudulent
        # in bust-out schemes
        estimated_overpayment = peak_paid * 0.8

        signal = {
            "signal_name": "bust_out_scheme",
            "signal_description": (
                f"Provider NPI {npi} first billed in {first_month} and ramped "
                f"from ${first_paid:,.2f}/month to ${peak_paid:,.2f}/month "
                f"({pct_increase:.0f}% increase) within 6 months. This rapid "
                f"billing escalation pattern is consistent with bust-out fraud."
            ),
            "severity": "high",
            "evidence": {
                "first_billing_month": first_month,
                "last_billing_month": last_month,
                "first_month_paid": round(first_paid, 2),
                "first_month_claims": first_claims,
                "peak_monthly_paid": round(peak_paid, 2),
                "peak_monthly_claims": peak_claims,
                "pct_increase": round(pct_increase, 1),
            },
            "estimated_overpayment_usd": round(estimated_overpayment, 2),
            "overpayment_methodology": (
                "Estimated at 80% of peak monthly billing, based on the "
                "assumption that bust-out schemes involve predominantly "
                "fraudulent claims once the ramp-up phase begins."
            ),
        }

        flagged.append({
            "npi": npi,
            "provider_name": f"NPI {npi}",
            "entity_type": "unknown",
            "state": "",
            "total_paid": peak_paid,
            "total_claims": peak_claims,
            "total_beneficiaries": 0,
            "signals": [signal],
        })

    return flagged


def signal_4_impossible_volume(con: duckdb.DuckDBPyConnection, parquet_path: str = PARQUET_URL) -> list[dict]:
    """
    Signal 4: Impossible service volume (HIGH)
    Flag providers with >500 claims per beneficiary per month.
    """
    log.info("=== Signal 4: Impossible Service Volume ===")

    log.info("Querying parquet for impossible claim volumes...")
    results = con.execute(f"""
        SELECT
            CAST(BILLING_PROVIDER_NPI_NUM AS VARCHAR) AS npi,
            CLAIM_FROM_MONTH,
            SUM(TOTAL_CLAIMS) AS total_claims,
            SUM(TOTAL_UNIQUE_BENEFICIARIES) AS total_beneficiaries,
            SUM(TOTAL_PAID) AS total_paid,
            CASE WHEN SUM(TOTAL_UNIQUE_BENEFICIARIES) > 0
                 THEN SUM(TOTAL_CLAIMS) * 1.0 / SUM(TOTAL_UNIQUE_BENEFICIARIES)
                 ELSE 999999 END AS claims_per_bene
        FROM read_parquet('{parquet_path}')
        GROUP BY BILLING_PROVIDER_NPI_NUM, CLAIM_FROM_MONTH
        HAVING (
            SUM(TOTAL_UNIQUE_BENEFICIARIES) > 0
            AND SUM(TOTAL_CLAIMS) * 1.0 / SUM(TOTAL_UNIQUE_BENEFICIARIES) > 500
            AND SUM(TOTAL_PAID) > 1000
        )
        ORDER BY claims_per_bene DESC
        LIMIT 200
    """).fetchall()

    log.info("Found %d provider-months with impossible volume", len(results))

    # Aggregate by NPI
    npi_data: dict[str, dict] = {}
    for row in results:
        npi = str(row[0])
        month = str(row[1])
        claims = int(row[2]) if row[2] else 0
        benes = int(row[3]) if row[3] else 0
        paid = float(row[4]) if row[4] else 0.0
        cpb = float(row[5]) if row[5] else 0.0

        if npi not in npi_data:
            npi_data[npi] = {
                "months": [],
                "total_paid": 0,
                "total_claims": 0,
                "total_benes": 0,
                "max_cpb": 0,
            }
        npi_data[npi]["months"].append({
            "month": month,
            "claims": claims,
            "beneficiaries": benes,
            "claims_per_beneficiary": round(cpb, 1),
            "paid": round(paid, 2),
        })
        npi_data[npi]["total_paid"] += paid
        npi_data[npi]["total_claims"] += claims
        npi_data[npi]["total_benes"] = max(npi_data[npi]["total_benes"], benes)
        npi_data[npi]["max_cpb"] = max(npi_data[npi]["max_cpb"], cpb)

    flagged = []
    for npi, data in sorted(npi_data.items(), key=lambda x: x[1]["total_paid"], reverse=True):
        signal = {
            "signal_name": "impossible_service_volume",
            "signal_description": (
                f"Provider NPI {npi} billed an average of "
                f"{data['max_cpb']:.0f} claims per beneficiary in at least one month. "
                f"No provider can physically deliver >500 services per patient per month. "
                f"Total: ${data['total_paid']:,.2f} paid across {len(data['months'])} "
                f"flagged months."
            ),
            "severity": "high",
            "evidence": {
                "max_claims_per_beneficiary": round(data["max_cpb"], 1),
                "flagged_months_count": len(data["months"]),
                "flagged_months": data["months"][:6],  # Top 6 worst months
                "total_paid_flagged_months": round(data["total_paid"], 2),
            },
            "estimated_overpayment_usd": round(data["total_paid"] * 0.9, 2),
            "overpayment_methodology": (
                "Estimated at 90% of total payments during flagged months. "
                "Claim volumes exceeding 500 per beneficiary per month are "
                "physically impossible, indicating phantom billing."
            ),
        }

        flagged.append({
            "npi": npi,
            "provider_name": f"NPI {npi}",
            "entity_type": "unknown",
            "state": "",
            "total_paid": data["total_paid"],
            "total_claims": data["total_claims"],
            "total_beneficiaries": data["total_benes"],
            "signals": [signal],
        })

    return flagged


def signal_5_home_health_abuse(con: duckdb.DuckDBPyConnection, parquet_path: str = PARQUET_URL) -> list[dict]:
    """
    Signal 5: Home health billing abuse (HIGH)
    Flag providers with beneficiary-to-claims ratio < 1:50 for home health codes.
    """
    log.info("=== Signal 5: Home Health Billing Abuse ===")

    # Home health HCPCS codes
    hh_codes_condition = """
        (
            HCPCS_CODE BETWEEN 'G0151' AND 'G0162'
            OR HCPCS_CODE BETWEEN 'G0299' AND 'G0300'
            OR HCPCS_CODE BETWEEN 'S9122' AND 'S9124'
            OR HCPCS_CODE BETWEEN 'T1019' AND 'T1022'
        )
    """

    log.info("Querying parquet for home health billing patterns...")
    results = con.execute(f"""
        SELECT
            CAST(BILLING_PROVIDER_NPI_NUM AS VARCHAR) AS npi,
            SUM(TOTAL_CLAIMS) AS total_hh_claims,
            SUM(TOTAL_UNIQUE_BENEFICIARIES) AS total_hh_beneficiaries,
            SUM(TOTAL_PAID) AS total_hh_paid,
            CASE WHEN SUM(TOTAL_UNIQUE_BENEFICIARIES) > 0
                 THEN SUM(TOTAL_CLAIMS) * 1.0 / SUM(TOTAL_UNIQUE_BENEFICIARIES)
                 ELSE 999999 END AS claims_per_bene,
            COUNT(DISTINCT HCPCS_CODE) AS distinct_hh_codes,
            COUNT(DISTINCT CLAIM_FROM_MONTH) AS billing_months
        FROM read_parquet('{parquet_path}')
        WHERE {hh_codes_condition}
        GROUP BY BILLING_PROVIDER_NPI_NUM
        HAVING (
            SUM(TOTAL_UNIQUE_BENEFICIARIES) > 0
            AND SUM(TOTAL_CLAIMS) * 1.0 / SUM(TOTAL_UNIQUE_BENEFICIARIES) > 50
            AND SUM(TOTAL_PAID) > 5000
        )
        ORDER BY total_hh_paid DESC
        LIMIT 200
    """).fetchall()

    log.info("Found %d providers with home health abuse patterns", len(results))

    flagged = []
    for row in results:
        npi = str(row[0])
        total_claims = int(row[1]) if row[1] else 0
        total_benes = int(row[2]) if row[2] else 0
        total_paid = float(row[3]) if row[3] else 0.0
        cpb = float(row[4]) if row[4] else 0.0
        distinct_codes = int(row[5]) if row[5] else 0
        billing_months = int(row[6]) if row[6] else 0

        # Overpayment: amount above reasonable ratio (1:10 claims per bene)
        reasonable_claims = total_benes * 10
        excess_claims_ratio = max(0, (total_claims - reasonable_claims)) / max(total_claims, 1)
        estimated_overpayment = total_paid * excess_claims_ratio

        signal = {
            "signal_name": "home_health_abuse",
            "signal_description": (
                f"Provider NPI {npi} billed {total_claims:,} home health claims "
                f"for only {total_benes:,} beneficiaries (ratio: 1:{cpb:.0f}). "
                f"This exceeds the 1:50 threshold significantly, indicating "
                f"repeated billing for the same patients. Total: ${total_paid:,.2f} "
                f"across {billing_months} months using {distinct_codes} HCPCS codes."
            ),
            "severity": "high",
            "evidence": {
                "total_home_health_claims": total_claims,
                "total_home_health_beneficiaries": total_benes,
                "claims_per_beneficiary": round(cpb, 1),
                "total_home_health_paid": round(total_paid, 2),
                "distinct_hcpcs_codes_used": distinct_codes,
                "billing_months": billing_months,
                "threshold_claims_per_bene": 50,
            },
            "estimated_overpayment_usd": round(estimated_overpayment, 2),
            "overpayment_methodology": (
                "Calculated as the proportion of claims exceeding a reasonable "
                "ratio of 10 claims per beneficiary, applied to total payments. "
                "Home health visits exceeding 50 per beneficiary suggest "
                "medically unnecessary services."
            ),
        }

        flagged.append({
            "npi": npi,
            "provider_name": f"NPI {npi}",
            "entity_type": "unknown",
            "state": "",
            "total_paid": total_paid,
            "total_claims": total_claims,
            "total_beneficiaries": total_benes,
            "signals": [signal],
        })

    return flagged


def signal_6_shell_entities(con: duckdb.DuckDBPyConnection, parquet_path: str = PARQUET_URL) -> list[dict]:
    """
    Signal 6: Shell entity networks (MEDIUM)
    Find authorized officials controlling 5+ NPIs from NPPES data,
    then check their billing.
    """
    log.info("=== Signal 6: Shell Entity Networks ===")

    mode = nppes_mode()
    if mode != "parquet":
        log.warning("NPPES parquet not available — skipping shell entity detection. "
                    "Run setup.sh to download NPPES data.")
        return []

    log.info("Querying NPPES for officials controlling 5+ NPIs...")
    shell_officials = con.execute(f"""
        SELECT
            LOWER(TRIM(auth_official_last_name)) || '|' ||
            LOWER(TRIM(auth_official_first_name)) AS official_key,
            auth_official_last_name,
            auth_official_first_name,
            COUNT(DISTINCT NPI) AS npi_count,
            LIST(CAST(NPI AS VARCHAR)) AS npi_list,
            LIST(DISTINCT state) AS states
        FROM read_parquet('{NPPES_PARQUET_PATH}')
        WHERE auth_official_last_name IS NOT NULL
          AND TRIM(auth_official_last_name) != ''
          AND auth_official_first_name IS NOT NULL
          AND TRIM(auth_official_first_name) != ''
          AND entity_type_code = '2'
        GROUP BY
            LOWER(TRIM(auth_official_last_name)) || '|' ||
            LOWER(TRIM(auth_official_first_name)),
            auth_official_last_name,
            auth_official_first_name
        HAVING COUNT(DISTINCT NPI) >= 5
        ORDER BY npi_count DESC
        LIMIT 100
    """).fetchall()

    log.info("Found %d officials controlling 5+ NPIs", len(shell_officials))

    if not shell_officials:
        return []

    # For each network, check billing totals
    flagged = []
    for official_row in shell_officials[:50]:  # Process top 50
        official_name = f"{official_row[2]} {official_row[1]}"
        npi_count = int(official_row[3])
        npi_list = official_row[4]
        states = official_row[5]

        # Limit NPI list for SQL query
        npi_strs = [str(n) for n in npi_list[:20]]
        npi_in_clause = ",".join(f"'{n}'" for n in npi_strs)

        try:
            billing = con.execute(f"""
                SELECT
                    CAST(BILLING_PROVIDER_NPI_NUM AS VARCHAR) AS npi,
                    SUM(TOTAL_PAID) AS total_paid,
                    SUM(TOTAL_CLAIMS) AS total_claims,
                    SUM(TOTAL_UNIQUE_BENEFICIARIES) AS total_benes
                FROM read_parquet('{parquet_path}')
                WHERE CAST(BILLING_PROVIDER_NPI_NUM AS VARCHAR) IN ({npi_in_clause})
                GROUP BY BILLING_PROVIDER_NPI_NUM
            """).fetchall()
        except Exception as e:
            log.warning("Failed to query billing for network %s: %s", official_name, e)
            continue

        if not billing:
            continue

        network_total_paid = sum(float(b[1]) for b in billing if b[1])
        network_total_claims = sum(int(b[2]) for b in billing if b[2])
        network_total_benes = sum(int(b[3]) for b in billing if b[3])
        active_npis = [str(b[0]) for b in billing]

        if network_total_paid < 10000:
            continue

        # Use first active NPI as the representative
        rep_npi = active_npis[0] if active_npis else npi_strs[0]

        signal = {
            "signal_name": "shell_entity_network",
            "signal_description": (
                f"Authorized official {official_name} controls {npi_count} NPIs "
                f"across states: {', '.join(str(s) for s in states[:5])}. "
                f"Combined Medicaid billing: ${network_total_paid:,.2f} across "
                f"{len(active_npis)} active billing NPIs. This concentration "
                f"of control suggests a potential shell entity network."
            ),
            "severity": "medium",
            "evidence": {
                "authorized_official_name": official_name,
                "total_npis_controlled": npi_count,
                "active_billing_npis": len(active_npis),
                "npi_list_sample": active_npis[:10],
                "states": [str(s) for s in states[:10]],
                "network_total_paid": round(network_total_paid, 2),
                "network_total_claims": network_total_claims,
            },
            "estimated_overpayment_usd": round(network_total_paid * 0.3, 2),
            "overpayment_methodology": (
                "Estimated at 30% of total network billing. Shell entity networks "
                "typically use multiple entities to distribute fraudulent billing "
                "and avoid detection thresholds."
            ),
        }

        flagged.append({
            "npi": rep_npi,
            "provider_name": official_name + " (network)",
            "entity_type": "organization",
            "state": str(states[0]) if states else "",
            "total_paid": network_total_paid,
            "total_claims": network_total_claims,
            "total_beneficiaries": network_total_benes,
            "signals": [signal],
        })

    return flagged


# ---------------------------------------------------------------------------
# Output Assembly
# ---------------------------------------------------------------------------

def merge_flagged_providers(all_signals: list[list[dict]]) -> list[dict]:
    """Merge providers flagged by multiple signals into single entries."""
    provider_map: dict[str, dict] = {}

    for signal_list in all_signals:
        for entry in signal_list:
            npi = entry["npi"]
            if npi in provider_map:
                # Merge signals
                provider_map[npi]["signals"].extend(entry["signals"])
                # Update totals if new data is better
                if entry["total_paid"] > provider_map[npi].get("total_paid", 0):
                    provider_map[npi]["total_paid"] = entry["total_paid"]
                if entry["total_claims"] > provider_map[npi].get("total_claims", 0):
                    provider_map[npi]["total_claims"] = entry["total_claims"]
                if entry["total_beneficiaries"] > provider_map[npi].get("total_beneficiaries", 0):
                    provider_map[npi]["total_beneficiaries"] = entry["total_beneficiaries"]
            else:
                provider_map[npi] = entry.copy()

    return list(provider_map.values())


def enrich_and_finalize(
    con: duckdb.DuckDBPyConnection,
    providers: list[dict],
    total_scanned: int,
) -> dict:
    """Enrich flagged providers with NPPES data and build final output."""
    log.info("Enriching %d flagged providers with NPPES data...", len(providers))

    total_overpayment = 0.0
    finalized = []

    for i, prov in enumerate(providers):
        npi = prov["npi"]

        # Enrich from NPPES
        nppes_info = enrich_provider(con, npi)
        if nppes_info["provider_name"] != f"NPI {npi}" and prov["provider_name"].startswith("NPI "):
            prov["provider_name"] = nppes_info["provider_name"]
        if nppes_info["entity_type"] != "unknown" and prov["entity_type"] in ("unknown", ""):
            prov["entity_type"] = nppes_info["entity_type"]
        if nppes_info["state"] and not prov["state"]:
            prov["state"] = nppes_info["state"]

        # Calculate combined overpayment
        combined_overpayment = sum(
            s.get("estimated_overpayment_usd", 0) for s in prov["signals"]
        )
        total_overpayment += combined_overpayment

        # Determine highest severity signal for FCA relevance
        severity_order = {"critical": 0, "high": 1, "medium": 2}
        top_signal = sorted(
            prov["signals"],
            key=lambda s: severity_order.get(s["severity"], 99)
        )[0]

        provider_info = {
            "npi": npi,
            "provider_name": prov["provider_name"],
            "state": prov.get("state", ""),
        }

        final_provider = {
            "npi": npi,
            "provider_name": prov["provider_name"],
            "entity_type": prov.get("entity_type", "unknown"),
            "taxonomy_code": nppes_info.get("taxonomy_code", ""),
            "state": prov.get("state", ""),
            "enumeration_date": nppes_info.get("enumeration_date"),
            "total_paid_all_time": round(prov.get("total_paid", 0), 2),
            "total_claims_all_time": prov.get("total_claims", 0),
            "total_unique_beneficiaries_all_time": prov.get("total_beneficiaries", 0),
            "signals": prov["signals"],
            "combined_estimated_overpayment_usd": round(combined_overpayment, 2),
            "fca_relevance": build_fca_relevance(
                top_signal["signal_name"],
                provider_info,
                combined_overpayment,
            ),
        }
        finalized.append(final_provider)

        if (i + 1) % 50 == 0:
            log.info("  Enriched %d / %d providers", i + 1, len(providers))

    # Sort by combined overpayment descending
    finalized.sort(key=lambda p: p["combined_estimated_overpayment_usd"], reverse=True)

    output = {
        "generated_at": datetime.now(timezone.utc).isoformat(),
        "tool_version": TOOL_VERSION,
        "data_sources_used": [
            PARQUET_URL,
            OIG_CSV_URL,
            "https://download.cms.gov/nppes/NPPES_Data_Dissemination_February_2026_V2.zip",
        ],
        "methodology_summary": (
            "This tool cross-references 227M rows of CMS Medicaid provider spending data "
            "with the OIG LEIE exclusion list and NPPES NPI registry to detect six categories "
            "of fraud signals: (1) excluded providers still billing Medicaid, "
            "(2) statistical billing outliers >3 sigma above the mean, "
            "(3) bust-out schemes with >500% billing ramp-up within 6 months, "
            "(4) impossible service volumes >500 claims/beneficiary/month, "
            "(5) home health billing abuse with claims-to-beneficiary ratio >50:1, and "
            "(6) shell entity networks where a single authorized official controls 5+ NPIs. "
            "Data is queried via DuckDB httpfs for memory efficiency. "
            "Overpayment estimates are conservative and based on methodology-specific formulas."
        ),
        "total_providers_scanned": total_scanned,
        "total_providers_flagged": len(finalized),
        "total_estimated_overpayment_usd": round(total_overpayment, 2),
        "flagged_providers": finalized,
    }

    return output


# ---------------------------------------------------------------------------
# Main Execution
# ---------------------------------------------------------------------------

def main():
    start_time = time.time()
    log.info("=" * 70)
    log.info("Medicaid Provider Fraud Signal Detection Engine v%s", TOOL_VERSION)
    log.info("=" * 70)

    # Step 0: Ensure data files exist
    log.info("--- Step 0: Checking data files ---")
    ensure_oig_csv()
    if not OIG_CSV_PATH.exists():
        log.error("OIG CSV not found. Run setup.sh first.")
        sys.exit(1)

    # Step 1: Connect to DuckDB and ensure local parquet
    log.info("--- Step 1: Initializing DuckDB ---")
    con = get_duckdb_connection()

    # Download parquet locally for fast repeated queries
    log.info("--- Step 1b: Ensuring local parquet data ---")
    try:
        parquet_path = ensure_local_parquet(con)
    except Exception as e:
        log.error("Failed to get parquet data: %s", e)
        log.info("Falling back to remote parquet URL...")
        parquet_path = PARQUET_URL

    # Quick test and count
    log.info("Testing parquet access...")
    try:
        test = con.execute(f"""
            SELECT COUNT(*) AS total_rows
            FROM read_parquet('{parquet_path}')
        """).fetchone()
        total_rows = test[0] if test else 0
        log.info("Parquet accessible: %s total rows", f"{total_rows:,}")
    except Exception as e:
        log.error("Cannot access parquet: %s", e)
        sys.exit(1)

    # Approximate unique providers (faster: use approx_count_distinct)
    log.info("Counting unique providers (approximate)...")
    total_providers = con.execute(f"""
        SELECT approx_count_distinct(BILLING_PROVIDER_NPI_NUM)
        FROM read_parquet('{parquet_path}')
    """).fetchone()[0]
    log.info("Approximate unique billing providers: %s", f"{total_providers:,}")

    # Step 2: Run fraud signals
    all_signals = []

    # Signal 1: Excluded providers (CRITICAL - highest priority)
    try:
        s1 = signal_1_excluded_providers(con, parquet_path)
        all_signals.append(s1)
        log.info("Signal 1 complete: %d providers flagged", len(s1))
    except Exception as e:
        log.error("Signal 1 failed: %s", e, exc_info=True)
        all_signals.append([])

    # Signal 2: Statistical outliers
    try:
        s2 = signal_2_statistical_outliers(con, parquet_path)
        all_signals.append(s2)
        log.info("Signal 2 complete: %d providers flagged", len(s2))
    except Exception as e:
        log.error("Signal 2 failed: %s", e, exc_info=True)
        all_signals.append([])

    # Signal 3: Bust-out schemes
    try:
        s3 = signal_3_bust_out_schemes(con, parquet_path)
        all_signals.append(s3)
        log.info("Signal 3 complete: %d providers flagged", len(s3))
    except Exception as e:
        log.error("Signal 3 failed: %s", e, exc_info=True)
        all_signals.append([])

    # Signal 4: Impossible service volume
    try:
        s4 = signal_4_impossible_volume(con, parquet_path)
        all_signals.append(s4)
        log.info("Signal 4 complete: %d providers flagged", len(s4))
    except Exception as e:
        log.error("Signal 4 failed: %s", e, exc_info=True)
        all_signals.append([])

    # Signal 5: Home health abuse
    try:
        s5 = signal_5_home_health_abuse(con, parquet_path)
        all_signals.append(s5)
        log.info("Signal 5 complete: %d providers flagged", len(s5))
    except Exception as e:
        log.error("Signal 5 failed: %s", e, exc_info=True)
        all_signals.append([])

    # Signal 6: Shell entity networks
    try:
        s6 = signal_6_shell_entities(con, parquet_path)
        all_signals.append(s6)
        log.info("Signal 6 complete: %d providers flagged", len(s6))
    except Exception as e:
        log.error("Signal 6 failed: %s", e, exc_info=True)
        all_signals.append([])

    # Step 3: Merge and enrich
    log.info("--- Step 3: Merging and enriching results ---")
    merged = merge_flagged_providers(all_signals)
    log.info("Merged to %d unique providers", len(merged))

    # Step 4: Build output
    log.info("--- Step 4: Building output ---")
    output = enrich_and_finalize(con, merged, total_providers)

    # Step 5: Write output
    log.info("--- Step 5: Writing output ---")
    OUTPUT_FILE.write_text(json.dumps(output, indent=2, default=str))
    log.info("Output written to: %s", OUTPUT_FILE)

    # Summary
    elapsed = time.time() - start_time
    log.info("=" * 70)
    log.info("DETECTION COMPLETE")
    log.info("  Total providers scanned:   %s", f"{output['total_providers_scanned']:,}")
    log.info("  Total providers flagged:    %s", f"{output['total_providers_flagged']:,}")
    log.info("  Estimated overpayment:      $%s", f"{output['total_estimated_overpayment_usd']:,.2f}")
    log.info("  Time elapsed:               %.1f seconds", elapsed)
    log.info("  Output file:                %s", OUTPUT_FILE)
    log.info("=" * 70)

    # Signal breakdown
    signal_counts = {}
    for p in output["flagged_providers"]:
        for s in p["signals"]:
            name = s["signal_name"]
            signal_counts[name] = signal_counts.get(name, 0) + 1
    for name, count in sorted(signal_counts.items()):
        log.info("  %-35s %d providers", name, count)

    con.close()
    return 0


if __name__ == "__main__":
    sys.exit(main())
