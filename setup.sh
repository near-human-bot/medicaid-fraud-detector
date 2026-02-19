#!/usr/bin/env bash
# setup.sh — Download OIG exclusion list and prepare NPPES data
# Memory-conscious: only downloads what's needed

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
DATA_DIR="${SCRIPT_DIR}/data"
mkdir -p "${DATA_DIR}"

echo "=== Medicaid Fraud Signal Detection Engine — Setup ==="
echo "Data directory: ${DATA_DIR}"
echo ""

# ---- 1. Download OIG LEIE Exclusion List (small CSV, ~15MB) ----
OIG_CSV="${DATA_DIR}/UPDATED.csv"
if [ -f "${OIG_CSV}" ]; then
    echo "[OIG] Exclusion list already downloaded: ${OIG_CSV}"
else
    echo "[OIG] Downloading OIG LEIE Exclusion List..."
    curl -L -o "${OIG_CSV}" \
        "https://oig.hhs.gov/exclusions/downloadables/UPDATED.csv"
    echo "[OIG] Downloaded: $(wc -l < "${OIG_CSV}") lines"
fi

# ---- 2. NPPES NPI Registry ----
# The full NPPES file is ~1GB zipped, ~8GB unzipped. Too large for our disk.
# Strategy: Download the zip, extract only the main CSV, then use DuckDB
# to read only the columns we need and create a small filtered parquet.
# If disk is too tight, we fall back to the NPPES API for individual lookups.

NPPES_DIR="${DATA_DIR}/nppes"
NPPES_PARQUET="${NPPES_DIR}/nppes_slim.parquet"
NPPES_ZIP="${NPPES_DIR}/nppes.zip"

if [ -f "${NPPES_PARQUET}" ]; then
    echo "[NPPES] Slim parquet already exists: ${NPPES_PARQUET}"
else
    mkdir -p "${NPPES_DIR}"

    # Check available disk space (need ~2GB temporarily)
    AVAIL_KB=$(df --output=avail "${DATA_DIR}" | tail -1 | tr -d ' ')
    AVAIL_GB=$((AVAIL_KB / 1024 / 1024))
    echo "[NPPES] Available disk: ${AVAIL_GB}GB"

    if [ "${AVAIL_GB}" -lt 3 ]; then
        echo "[NPPES] WARNING: Less than 3GB free. Will use NPPES API for lookups instead."
        echo "api_fallback" > "${NPPES_DIR}/mode.txt"
    else
        echo "[NPPES] Downloading NPPES zip (~1GB)..."
        curl -L -o "${NPPES_ZIP}" \
            "https://download.cms.gov/nppes/NPPES_Data_Dissemination_February_2026_V2.zip" \
            --progress-bar

        echo "[NPPES] Extracting main CSV from zip..."
        # Find the main CSV filename inside the zip
        MAIN_CSV=$(unzip -l "${NPPES_ZIP}" | grep -oP 'npidata_pfile_\d+-\d+\.csv' | head -1)

        if [ -z "${MAIN_CSV}" ]; then
            echo "[NPPES] Could not find main CSV in zip. Listing contents:"
            unzip -l "${NPPES_ZIP}" | head -20
            echo "[NPPES] Falling back to API mode."
            echo "api_fallback" > "${NPPES_DIR}/mode.txt"
            rm -f "${NPPES_ZIP}"
        else
            echo "[NPPES] Found main CSV: ${MAIN_CSV}"
            # Extract just that file
            unzip -o -j "${NPPES_ZIP}" "${MAIN_CSV}" -d "${NPPES_DIR}/"

            echo "[NPPES] Creating slim parquet with only needed columns..."
            /opt/autonomous-ai/venv/bin/python3 -c "
import duckdb
con = duckdb.connect()
con.execute(\"SET memory_limit='1500MB'\")
# Read only the columns we need from the large CSV
con.execute(\"\"\"
    COPY (
        SELECT
            NPI,
            \"Entity Type Code\" AS entity_type_code,
            \"Provider Organization Name (Legal Business Name)\" AS org_name,
            \"Provider Last Name (Legal Name)\" AS last_name,
            \"Provider First Name\" AS first_name,
            \"Provider Business Mailing Address State Name\" AS state,
            \"Provider Business Mailing Address Postal Code\" AS postal_code,
            \"Healthcare Provider Taxonomy Code_1\" AS taxonomy_code,
            \"Provider Enumeration Date\" AS enumeration_date,
            \"Authorized Official Last Name\" AS auth_official_last_name,
            \"Authorized Official First Name\" AS auth_official_first_name
        FROM read_csv('${NPPES_DIR}/${MAIN_CSV}',
            auto_detect=true,
            ignore_errors=true,
            parallel=false)
        WHERE NPI IS NOT NULL
    ) TO '${NPPES_PARQUET}' (FORMAT PARQUET, COMPRESSION ZSTD)
\"\"\")
print('Slim parquet created successfully')
con.close()
"
            echo "[NPPES] Cleaning up large files..."
            rm -f "${NPPES_DIR}/${MAIN_CSV}"
            rm -f "${NPPES_ZIP}"
            echo "parquet" > "${NPPES_DIR}/mode.txt"
        fi
    fi
fi

echo ""
echo "=== Setup Complete ==="
echo "OIG CSV: $(ls -lh "${OIG_CSV}" 2>/dev/null || echo 'not found')"
echo "NPPES:   $(cat "${NPPES_DIR}/mode.txt" 2>/dev/null || echo 'not configured')"
echo ""
echo "Run detect_fraud.py to start analysis."
