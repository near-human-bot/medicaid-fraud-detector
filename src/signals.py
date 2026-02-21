"""All 9 fraud signal implementations using DuckDB SQL."""

import duckdb


def signal_excluded_provider(con: duckdb.DuckDBPyConnection) -> list[dict]:
    """Signal 1: Excluded Provider Still Billing.
    
    A billing or servicing NPI in spending data matches an NPI in LEIE where
    exclusion date is before the claim month and reinstatement date is empty
    or after the claim month.
    """
    results = con.execute("""
        WITH excluded_billing AS (
            SELECT
                s.billing_npi AS npi,
                l.excl_date,
                l.excl_type,
                l.lastname,
                l.firstname,
                l.busname,
                SUM(CASE WHEN s.claim_month >= l.excl_date THEN s.total_paid ELSE 0 END) AS total_paid_after_exclusion,
                SUM(CASE WHEN s.claim_month >= l.excl_date THEN s.total_claims ELSE 0 END) AS total_claims_after_exclusion,
                MIN(s.claim_month) AS first_claim_after_exclusion,
                MAX(s.claim_month) AS last_claim_after_exclusion
            FROM spending s
            JOIN leie l ON s.billing_npi = l.npi
            WHERE l.npi IS NOT NULL
              AND TRIM(l.npi) != ''
              AND l.npi != '0000000000'
              AND l.excl_date IS NOT NULL
              AND l.excl_date < s.claim_month
              AND (l.rein_date IS NULL OR l.rein_date > s.claim_month)
            GROUP BY s.billing_npi, l.excl_date, l.excl_type, l.lastname, l.firstname, l.busname
        ),
        excluded_servicing AS (
            SELECT
                s.servicing_npi AS npi,
                l.excl_date,
                l.excl_type,
                l.lastname,
                l.firstname,
                l.busname,
                SUM(CASE WHEN s.claim_month >= l.excl_date THEN s.total_paid ELSE 0 END) AS total_paid_after_exclusion,
                SUM(CASE WHEN s.claim_month >= l.excl_date THEN s.total_claims ELSE 0 END) AS total_claims_after_exclusion,
                MIN(s.claim_month) AS first_claim_after_exclusion,
                MAX(s.claim_month) AS last_claim_after_exclusion
            FROM spending s
            JOIN leie l ON s.servicing_npi = l.npi
            WHERE l.npi IS NOT NULL
              AND TRIM(l.npi) != ''
              AND l.npi != '0000000000'
              AND l.excl_date IS NOT NULL
              AND l.excl_date < s.claim_month
              AND (l.rein_date IS NULL OR l.rein_date > s.claim_month)
            GROUP BY s.servicing_npi, l.excl_date, l.excl_type, l.lastname, l.firstname, l.busname
        ),
        combined AS (
            SELECT * FROM excluded_billing
            UNION ALL
            SELECT * FROM excluded_servicing
        )
        SELECT
            npi,
            excl_date,
            excl_type,
            lastname,
            firstname,
            busname,
            SUM(total_paid_after_exclusion) AS total_paid_after_exclusion,
            SUM(total_claims_after_exclusion) AS total_claims_after_exclusion,
            MIN(first_claim_after_exclusion) AS first_claim_after_exclusion,
            MAX(last_claim_after_exclusion) AS last_claim_after_exclusion
        FROM combined
        GROUP BY npi, excl_date, excl_type, lastname, firstname, busname
        ORDER BY total_paid_after_exclusion DESC
    """).fetchall()

    columns = ["npi", "excl_date", "excl_type", "lastname", "firstname", "busname",
                "total_paid_after_exclusion", "total_claims_after_exclusion",
                "first_claim_after_exclusion", "last_claim_after_exclusion"]

    signals = []
    for row in results:
        d = dict(zip(columns, row))
        signals.append({
            "signal_type": "excluded_provider",
            "severity": "critical",
            "npi": d["npi"],
            "evidence": {
                "exclusion_date": str(d["excl_date"]),
                "exclusion_type": d["excl_type"],
                "provider_name": f"{d['firstname'] or ''} {d['lastname'] or ''} {d['busname'] or ''}".strip(),
                "total_paid_after_exclusion": float(d["total_paid_after_exclusion"] or 0),
                "total_claims_after_exclusion": int(d["total_claims_after_exclusion"] or 0),
                "first_claim_after_exclusion": str(d["first_claim_after_exclusion"]),
                "last_claim_after_exclusion": str(d["last_claim_after_exclusion"]),
            },
            "estimated_overpayment_usd": float(d["total_paid_after_exclusion"] or 0),
        })
    return signals


def signal_billing_outlier(con: duckdb.DuckDBPyConnection) -> list[dict]:
    """Signal 2: Billing Volume Outlier.
    
    For each billing NPI, compute total paid. Group by taxonomy+state.
    Flag providers above 99th percentile of their peer group.
    """
    results = con.execute("""
        WITH provider_totals AS (
            SELECT
                s.billing_npi AS npi,
                SUM(s.total_paid) AS provider_total_paid,
                SUM(s.total_claims) AS provider_total_claims,
                SUM(s.unique_beneficiaries) AS provider_total_beneficiaries
            FROM spending s
            GROUP BY s.billing_npi
        ),
        provider_with_nppes AS (
            SELECT
                pt.*,
                n.taxonomy_code,
                n.state,
                n.entity_type_code,
                COALESCE(n.org_name, n.first_name || ' ' || n.last_name) AS provider_name
            FROM provider_totals pt
            LEFT JOIN nppes n ON pt.npi = n.npi
        ),
        peer_stats AS (
            SELECT
                taxonomy_code,
                state,
                PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY provider_total_paid) AS peer_median,
                PERCENTILE_CONT(0.99) WITHIN GROUP (ORDER BY provider_total_paid) AS peer_p99,
                COUNT(*) AS peer_count
            FROM provider_with_nppes
            WHERE taxonomy_code IS NOT NULL AND state IS NOT NULL
            GROUP BY taxonomy_code, state
            HAVING COUNT(*) >= 5
        )
        SELECT
            p.npi,
            p.provider_name,
            p.entity_type_code,
            p.taxonomy_code,
            p.state,
            p.provider_total_paid,
            p.provider_total_claims,
            ps.peer_median,
            ps.peer_p99,
            ps.peer_count,
            p.provider_total_paid / NULLIF(ps.peer_median, 0) AS ratio_to_median
        FROM provider_with_nppes p
        JOIN peer_stats ps ON p.taxonomy_code = ps.taxonomy_code AND p.state = ps.state
        WHERE p.provider_total_paid > ps.peer_p99
        ORDER BY p.provider_total_paid DESC
    """).fetchall()

    columns = ["npi", "provider_name", "entity_type_code", "taxonomy_code", "state",
                "provider_total_paid", "provider_total_claims", "peer_median", "peer_p99",
                "peer_count", "ratio_to_median"]

    signals = []
    for row in results:
        d = dict(zip(columns, row))
        ratio = float(d["ratio_to_median"] or 0)
        severity = "high" if ratio > 5 else "medium"
        overpayment = max(0, float(d["provider_total_paid"] or 0) - float(d["peer_p99"] or 0))
        signals.append({
            "signal_type": "billing_outlier",
            "severity": severity,
            "npi": d["npi"],
            "evidence": {
                "total_paid": float(d["provider_total_paid"] or 0),
                "taxonomy_code": d["taxonomy_code"],
                "state": d["state"],
                "peer_median": float(d["peer_median"] or 0),
                "peer_99th_percentile": float(d["peer_p99"] or 0),
                "ratio_to_median": round(ratio, 2),
                "peer_count": int(d["peer_count"] or 0),
            },
            "estimated_overpayment_usd": round(overpayment, 2),
        })
    return signals


def signal_rapid_escalation(con: duckdb.DuckDBPyConnection) -> list[dict]:
    """Signal 3: Rapid Billing Escalation (New Entity).
    
    Providers enumerated within 24 months before their first billing month.
    Flag if any rolling 3-month average growth rate exceeds 200%.
    """
    results = con.execute("""
        WITH provider_first_bill AS (
            SELECT
                billing_npi AS npi,
                MIN(claim_month) AS first_bill_month
            FROM spending
            GROUP BY billing_npi
        ),
        new_providers AS (
            SELECT
                pfb.npi,
                pfb.first_bill_month,
                n.enumeration_date,
                COALESCE(n.org_name, n.first_name || ' ' || n.last_name) AS provider_name,
                n.entity_type_code,
                n.taxonomy_code,
                n.state
            FROM provider_first_bill pfb
            JOIN nppes n ON pfb.npi = n.npi
            WHERE n.enumeration_date IS NOT NULL
              AND n.enumeration_date >= pfb.first_bill_month - INTERVAL '24 months'
              AND n.enumeration_date < pfb.first_bill_month
        ),
        monthly_paid AS (
            SELECT
                s.billing_npi AS npi,
                s.claim_month,
                SUM(s.total_paid) AS month_paid,
                ROW_NUMBER() OVER (PARTITION BY s.billing_npi ORDER BY s.claim_month) AS month_num
            FROM spending s
            JOIN new_providers np ON s.billing_npi = np.npi
            GROUP BY s.billing_npi, s.claim_month
        ),
        first_12 AS (
            SELECT * FROM monthly_paid WHERE month_num <= 12
        ),
        with_growth AS (
            SELECT
                npi,
                claim_month,
                month_paid,
                month_num,
                LAG(month_paid) OVER (PARTITION BY npi ORDER BY month_num) AS prev_paid,
                CASE
                    WHEN LAG(month_paid) OVER (PARTITION BY npi ORDER BY month_num) > 0
                    THEN (month_paid - LAG(month_paid) OVER (PARTITION BY npi ORDER BY month_num))
                         / LAG(month_paid) OVER (PARTITION BY npi ORDER BY month_num) * 100.0
                    ELSE NULL
                END AS growth_pct
            FROM first_12
        ),
        rolling_3mo AS (
            SELECT
                npi,
                claim_month,
                month_paid,
                month_num,
                growth_pct,
                AVG(growth_pct) OVER (
                    PARTITION BY npi ORDER BY month_num
                    ROWS BETWEEN 2 PRECEDING AND CURRENT ROW
                ) AS rolling_3mo_avg_growth
            FROM with_growth
        ),
        flagged AS (
            SELECT
                npi,
                MAX(rolling_3mo_avg_growth) AS peak_3mo_growth
            FROM rolling_3mo
            WHERE rolling_3mo_avg_growth > 200
            GROUP BY npi
        )
        SELECT
            f.npi,
            np.provider_name,
            np.entity_type_code,
            np.taxonomy_code,
            np.state,
            np.enumeration_date,
            np.first_bill_month,
            f.peak_3mo_growth,
            (SELECT LIST(month_paid ORDER BY month_num)
             FROM first_12 m WHERE m.npi = f.npi) AS monthly_amounts,
            (SELECT SUM(mp.month_paid)
             FROM monthly_paid mp
             JOIN with_growth wg ON mp.npi = wg.npi AND mp.month_num = wg.month_num
             WHERE mp.npi = f.npi AND wg.growth_pct > 200) AS overpayment_in_growth_months
        FROM flagged f
        JOIN new_providers np ON f.npi = np.npi
        ORDER BY f.peak_3mo_growth DESC
    """).fetchall()

    columns = ["npi", "provider_name", "entity_type_code", "taxonomy_code", "state",
                "enumeration_date", "first_bill_month", "peak_3mo_growth",
                "monthly_amounts", "overpayment_in_growth_months"]

    signals = []
    for row in results:
        d = dict(zip(columns, row))
        peak = float(d["peak_3mo_growth"] or 0)
        severity = "high" if peak > 500 else "medium"
        signals.append({
            "signal_type": "rapid_escalation",
            "severity": severity,
            "npi": d["npi"],
            "evidence": {
                "enumeration_date": str(d["enumeration_date"]),
                "first_billing_month": str(d["first_bill_month"]),
                "peak_3_month_growth_rate": round(peak, 2),
                "monthly_amounts_first_12": [float(x) for x in (d["monthly_amounts"] or [])],
            },
            "estimated_overpayment_usd": float(d["overpayment_in_growth_months"] or 0),
        })
    return signals


def signal_workforce_impossibility(con: duckdb.DuckDBPyConnection) -> list[dict]:
    """Signal 4: Workforce Impossibility.
    
    For organizations (entity_type=2), compute max claims in any single month.
    If implied claims-per-hour > 6 (claims / 22 days / 8 hours), flag.
    """
    results = con.execute("""
        WITH org_monthly AS (
            SELECT
                s.billing_npi AS npi,
                s.claim_month,
                SUM(s.total_claims) AS month_claims,
                SUM(s.total_paid) AS month_paid
            FROM spending s
            JOIN nppes n ON s.billing_npi = n.npi
            WHERE n.entity_type_code = '2'
            GROUP BY s.billing_npi, s.claim_month
        ),
        peak_months AS (
            SELECT
                npi,
                claim_month AS peak_month,
                month_claims AS peak_claims,
                month_paid AS peak_month_paid,
                month_claims / 22.0 / 8.0 AS claims_per_hour,
                ROW_NUMBER() OVER (PARTITION BY npi ORDER BY month_claims DESC) AS rn
            FROM org_monthly
        )
        SELECT
            pm.npi,
            COALESCE(n.org_name, n.first_name || ' ' || n.last_name) AS provider_name,
            n.entity_type_code,
            n.taxonomy_code,
            n.state,
            pm.peak_month,
            pm.peak_claims,
            pm.peak_month_paid,
            pm.claims_per_hour
        FROM peak_months pm
        JOIN nppes n ON pm.npi = n.npi
        WHERE pm.rn = 1
          AND pm.claims_per_hour > 6.0
        ORDER BY pm.claims_per_hour DESC
    """).fetchall()

    columns = ["npi", "provider_name", "entity_type_code", "taxonomy_code", "state",
                "peak_month", "peak_claims", "peak_month_paid", "claims_per_hour"]

    signals = []
    for row in results:
        d = dict(zip(columns, row))
        peak_claims = int(d["peak_claims"] or 0)
        max_reasonable = 6 * 8 * 22  # 1056
        excess = max(0, peak_claims - max_reasonable)
        peak_paid = float(d["peak_month_paid"] or 0)
        cost_per_claim = peak_paid / peak_claims if peak_claims > 0 else 0
        overpayment = excess * cost_per_claim

        signals.append({
            "signal_type": "workforce_impossibility",
            "severity": "high",
            "npi": d["npi"],
            "evidence": {
                "peak_month": str(d["peak_month"]),
                "peak_claims_count": peak_claims,
                "implied_claims_per_hour": round(float(d["claims_per_hour"] or 0), 2),
                "total_paid_peak_month": round(peak_paid, 2),
            },
            "estimated_overpayment_usd": round(overpayment, 2),
        })
    return signals


def signal_shared_official(con: duckdb.DuckDBPyConnection) -> list[dict]:
    """Signal 5: Shared Authorized Official Across Multiple NPIs.
    
    Group NPIs by authorized official name. Flag if 5+ NPIs with combined
    total paid > $1M.
    """
    results = con.execute("""
        WITH official_npis AS (
            SELECT
                UPPER(TRIM(auth_official_last)) AS official_last,
                UPPER(TRIM(auth_official_first)) AS official_first,
                npi,
                org_name,
                state
            FROM nppes
            WHERE auth_official_last IS NOT NULL
              AND TRIM(auth_official_last) != ''
              AND auth_official_first IS NOT NULL
              AND TRIM(auth_official_first) != ''
        ),
        official_groups AS (
            SELECT
                official_last,
                official_first,
                COUNT(DISTINCT npi) AS npi_count,
                LIST(DISTINCT npi) AS npi_list,
                LIST(DISTINCT org_name) AS org_names
            FROM official_npis
            GROUP BY official_last, official_first
            HAVING COUNT(DISTINCT npi) >= 5
        ),
        npi_paid AS (
            SELECT
                billing_npi AS npi,
                SUM(total_paid) AS total_paid
            FROM spending
            GROUP BY billing_npi
        ),
        official_paid AS (
            SELECT
                og.official_last,
                og.official_first,
                og.npi_count,
                og.npi_list,
                og.org_names,
                SUM(COALESCE(np.total_paid, 0)) AS combined_total_paid
            FROM official_groups og
            CROSS JOIN UNNEST(og.npi_list) AS t(npi_val)
            LEFT JOIN npi_paid np ON t.npi_val = np.npi
            GROUP BY og.official_last, og.official_first, og.npi_count, og.npi_list, og.org_names
        )
        SELECT *
        FROM official_paid
        WHERE combined_total_paid > 1000000
        ORDER BY combined_total_paid DESC
    """).fetchall()

    columns = ["official_last", "official_first", "npi_count", "npi_list",
                "org_names", "combined_total_paid"]

    signals = []
    for row in results:
        d = dict(zip(columns, row))
        combined = float(d["combined_total_paid"] or 0)
        severity = "high" if combined > 5_000_000 else "medium"

        # Build per-NPI breakdown
        npi_list = d["npi_list"] if isinstance(d["npi_list"], list) else []

        # Overpayment estimate: 20% of combined billing for networks with 5+ entities
        # Conservative — based on DOJ settlement data showing shell networks average 20-40% fraud
        npi_count = int(d["npi_count"] or 0)
        overpayment = combined * 0.2 if npi_count >= 10 else combined * 0.1

        signals.append({
            "signal_type": "shared_official",
            "severity": severity,
            "npi": npi_list[0] if npi_list else "UNKNOWN",
            "evidence": {
                "authorized_official_name": f"{d['official_first']} {d['official_last']}",
                "npi_count": npi_count,
                "controlled_npis": npi_list,
                "organization_names": d["org_names"] if isinstance(d["org_names"], list) else [],
                "combined_total_paid": round(combined, 2),
            },
            "estimated_overpayment_usd": round(overpayment, 2),
        })
    return signals


def signal_geographic_implausibility(con: duckdb.DuckDBPyConnection) -> list[dict]:
    """Signal 6: Geographic Implausibility.
    
    For home health HCPCS codes, check if beneficiary/claims ratio < 0.1
    when claims > 100 in any single month.
    """
    # Home health HCPCS code ranges
    home_health_codes = """
        ('G0151','G0152','G0153','G0154','G0155','G0156','G0157','G0158','G0159','G0160','G0161','G0162',
         'G0299','G0300',
         'S9122','S9123','S9124',
         'T1019','T1020','T1021','T1022')
    """

    results = con.execute(f"""
        WITH home_health_monthly AS (
            SELECT
                s.billing_npi AS npi,
                s.hcpcs_code,
                s.claim_month,
                SUM(s.total_claims) AS month_claims,
                SUM(s.unique_beneficiaries) AS month_beneficiaries,
                SUM(s.total_paid) AS month_paid
            FROM spending s
            WHERE s.hcpcs_code IN {home_health_codes}
            GROUP BY s.billing_npi, s.hcpcs_code, s.claim_month
            HAVING SUM(s.total_claims) > 100
        ),
        flagged AS (
            SELECT
                h.*,
                h.month_beneficiaries * 1.0 / NULLIF(h.month_claims, 0) AS bene_claims_ratio
            FROM home_health_monthly h
            WHERE h.month_beneficiaries * 1.0 / NULLIF(h.month_claims, 0) < 0.1
        )
        SELECT
            f.npi,
            COALESCE(n.org_name, n.first_name || ' ' || n.last_name) AS provider_name,
            n.entity_type_code,
            n.taxonomy_code,
            n.state,
            f.hcpcs_code,
            f.claim_month,
            f.month_claims,
            f.month_beneficiaries,
            f.bene_claims_ratio,
            f.month_paid
        FROM flagged f
        LEFT JOIN nppes n ON f.npi = n.npi
        ORDER BY f.month_claims DESC
    """).fetchall()

    columns = ["npi", "provider_name", "entity_type_code", "taxonomy_code", "state",
                "hcpcs_code", "claim_month", "month_claims", "month_beneficiaries",
                "bene_claims_ratio", "month_paid"]

    signals = []
    seen_npis = set()
    for row in results:
        d = dict(zip(columns, row))
        npi = d["npi"]
        if npi in seen_npis:
            continue
        seen_npis.add(npi)

        # Overpayment: excess claims beyond reasonable ratio (1:10 bene:claims)
        month_claims = int(d["month_claims"] or 0)
        month_benes = int(d["month_beneficiaries"] or 0)
        month_paid = float(d["month_paid"] or 0)
        reasonable_claims = month_benes * 10
        excess_ratio = max(0, (month_claims - reasonable_claims)) / max(month_claims, 1)
        overpayment = month_paid * excess_ratio

        signals.append({
            "signal_type": "geographic_implausibility",
            "severity": "medium",
            "npi": npi,
            "evidence": {
                "state": d["state"],
                "flagged_hcpcs_codes": [d["hcpcs_code"]],
                "worst_month": str(d["claim_month"]),
                "claims_count": month_claims,
                "unique_beneficiaries": month_benes,
                "beneficiary_claims_ratio": round(float(d["bene_claims_ratio"] or 0), 4),
                "total_paid_worst_month": round(month_paid, 2),
            },
            "estimated_overpayment_usd": round(overpayment, 2),
        })
    return signals


def signal_address_clustering(con: duckdb.DuckDBPyConnection) -> list[dict]:
    """Signal 7: Address Clustering.

    Multiple unrelated NPIs registered at the same zip code with
    unusually high combined billing. Flags zip codes where 10+ NPIs
    share the same zip and combined billing exceeds $5M — potential
    ghost office or mill operation.
    """
    results = con.execute("""
        WITH provider_totals AS (
            SELECT
                billing_npi AS npi,
                SUM(total_paid) AS total_paid,
                SUM(total_claims) AS total_claims
            FROM spending
            GROUP BY billing_npi
        ),
        zip_clusters AS (
            SELECT
                n.zip_code,
                n.state,
                COUNT(DISTINCT n.npi) AS npi_count,
                LIST(DISTINCT n.npi) AS npi_list,
                LIST(DISTINCT COALESCE(n.org_name, n.first_name || ' ' || n.last_name)) AS provider_names,
                SUM(pt.total_paid) AS combined_paid,
                SUM(pt.total_claims) AS combined_claims
            FROM nppes n
            JOIN provider_totals pt ON n.npi = pt.npi
            WHERE n.zip_code IS NOT NULL
              AND TRIM(n.zip_code) != ''
            GROUP BY n.zip_code, n.state
            HAVING COUNT(DISTINCT n.npi) >= 10
               AND SUM(pt.total_paid) > 5000000
        )
        SELECT * FROM zip_clusters
        ORDER BY combined_paid DESC
    """).fetchall()

    columns = ["zip_code", "state", "npi_count", "npi_list", "provider_names",
               "combined_paid", "combined_claims"]

    signals = []
    for row in results:
        d = dict(zip(columns, row))
        npi_list = d["npi_list"] if isinstance(d["npi_list"], list) else []
        npi_count = int(d["npi_count"] or 0)
        combined_paid = float(d["combined_paid"] or 0)
        severity = "high" if npi_count >= 20 else "medium"
        # Overpayment: 15% of combined billing for address clusters
        # Ghost offices typically submit 15-30% fraudulent claims (OIG data)
        overpayment = combined_paid * 0.15

        signals.append({
            "signal_type": "address_clustering",
            "severity": severity,
            "npi": npi_list[0] if npi_list else "UNKNOWN",
            "evidence": {
                "zip_code": d["zip_code"],
                "state": d["state"],
                "npi_count": npi_count,
                "clustered_npis": npi_list[:20],
                "provider_names": (d["provider_names"] if isinstance(d["provider_names"], list) else [])[:20],
                "combined_total_paid": round(combined_paid, 2),
                "combined_total_claims": int(d["combined_claims"] or 0),
            },
            "estimated_overpayment_usd": round(overpayment, 2),
        })
    return signals


def signal_upcoding(con: duckdb.DuckDBPyConnection) -> list[dict]:
    """Signal 8: Upcoding Detection.

    Provider consistently bills highest-level E&M codes (99215, 99205)
    at rates far exceeding their peers in the same taxonomy+state.
    A provider billing >80% high-complexity codes when peers average <30%
    is likely upcoding.
    """
    results = con.execute("""
        WITH provider_em AS (
            SELECT
                s.billing_npi AS npi,
                SUM(s.total_claims) AS total_em_claims,
                SUM(CASE WHEN s.hcpcs_code IN ('99215','99205','99223','99233','99245','99255')
                    THEN s.total_claims ELSE 0 END) AS high_level_claims,
                SUM(s.total_paid) AS total_paid
            FROM spending s
            WHERE s.hcpcs_code IN (
                '99201','99202','99203','99204','99205',
                '99211','99212','99213','99214','99215',
                '99221','99222','99223',
                '99231','99232','99233',
                '99241','99242','99243','99244','99245',
                '99251','99252','99253','99254','99255'
            )
            GROUP BY s.billing_npi
            HAVING SUM(s.total_claims) >= 50
        ),
        with_nppes AS (
            SELECT
                pe.*,
                n.taxonomy_code,
                n.state,
                COALESCE(n.org_name, n.first_name || ' ' || n.last_name) AS provider_name,
                pe.high_level_claims * 100.0 / NULLIF(pe.total_em_claims, 0) AS high_pct
            FROM provider_em pe
            JOIN nppes n ON pe.npi = n.npi
        ),
        peer_avg AS (
            SELECT
                taxonomy_code,
                state,
                AVG(high_pct) AS avg_high_pct,
                COUNT(*) AS peer_count
            FROM with_nppes
            WHERE taxonomy_code IS NOT NULL AND state IS NOT NULL
            GROUP BY taxonomy_code, state
            HAVING COUNT(*) >= 3
        )
        SELECT
            wn.npi,
            wn.provider_name,
            wn.taxonomy_code,
            wn.state,
            wn.total_em_claims,
            wn.high_level_claims,
            wn.total_paid,
            wn.high_pct,
            pa.avg_high_pct AS peer_avg_high_pct,
            pa.peer_count
        FROM with_nppes wn
        JOIN peer_avg pa ON wn.taxonomy_code = pa.taxonomy_code AND wn.state = pa.state
        WHERE wn.high_pct > 80.0
          AND pa.avg_high_pct < 30.0
        ORDER BY wn.high_pct DESC
    """).fetchall()

    columns = ["npi", "provider_name", "taxonomy_code", "state", "total_em_claims",
               "high_level_claims", "total_paid", "high_pct", "peer_avg_high_pct", "peer_count"]

    signals = []
    for row in results:
        d = dict(zip(columns, row))
        high_pct = float(d["high_pct"] or 0)
        peer_avg = float(d["peer_avg_high_pct"] or 0)
        excess_pct = max(0, high_pct - peer_avg) / 100.0
        overpayment = float(d["total_paid"] or 0) * excess_pct * 0.3  # conservative 30% uplift

        signals.append({
            "signal_type": "upcoding",
            "severity": "high" if high_pct > 90 else "medium",
            "npi": d["npi"],
            "evidence": {
                "total_em_claims": int(d["total_em_claims"] or 0),
                "high_level_claims": int(d["high_level_claims"] or 0),
                "high_level_percentage": round(high_pct, 2),
                "peer_avg_high_level_percentage": round(peer_avg, 2),
                "total_paid": round(float(d["total_paid"] or 0), 2),
                "peer_count": int(d["peer_count"] or 0),
            },
            "estimated_overpayment_usd": round(overpayment, 2),
        })
    return signals


def signal_concurrent_billing(con: duckdb.DuckDBPyConnection) -> list[dict]:
    """Signal 9: Concurrent Billing Across States.

    A single NPI billing in 5+ states in the same month — physically
    impossible for an individual practitioner. Potential identity theft
    or phantom billing.
    """
    results = con.execute("""
        WITH npi_state_months AS (
            SELECT
                s.billing_npi AS npi,
                s.claim_month,
                COUNT(DISTINCT n_serv.state) AS state_count,
                LIST(DISTINCT n_serv.state) AS states,
                SUM(s.total_paid) AS month_paid,
                SUM(s.total_claims) AS month_claims
            FROM spending s
            LEFT JOIN nppes n_serv ON s.servicing_npi = n_serv.npi
            WHERE n_serv.state IS NOT NULL
            GROUP BY s.billing_npi, s.claim_month
            HAVING COUNT(DISTINCT n_serv.state) >= 5
        ),
        flagged AS (
            SELECT
                npi,
                MAX(state_count) AS max_states_in_month,
                COUNT(*) AS months_flagged,
                SUM(month_paid) AS total_paid_flagged,
                SUM(month_claims) AS total_claims_flagged
            FROM npi_state_months
            GROUP BY npi
        )
        SELECT
            f.npi,
            COALESCE(n.org_name, n.first_name || ' ' || n.last_name) AS provider_name,
            n.entity_type_code,
            n.taxonomy_code,
            n.state AS home_state,
            f.max_states_in_month,
            f.months_flagged,
            f.total_paid_flagged,
            f.total_claims_flagged
        FROM flagged f
        LEFT JOIN nppes n ON f.npi = n.npi
        ORDER BY f.max_states_in_month DESC
    """).fetchall()

    columns = ["npi", "provider_name", "entity_type_code", "taxonomy_code", "home_state",
               "max_states_in_month", "months_flagged", "total_paid_flagged", "total_claims_flagged"]

    signals = []
    for row in results:
        d = dict(zip(columns, row))
        entity = d["entity_type_code"]
        # Only flag individuals — orgs legitimately operate multi-state
        if entity == '2':
            continue
        max_states = int(d["max_states_in_month"] or 0)
        total_paid_flagged = float(d["total_paid_flagged"] or 0)
        severity = "high" if max_states >= 8 else "medium"
        # Overpayment: individual can only legitimately practice in 1-2 states
        # Excess states billing is likely phantom — estimate 60% of flagged payments
        overpayment = total_paid_flagged * 0.6

        signals.append({
            "signal_type": "concurrent_billing",
            "severity": severity,
            "npi": d["npi"],
            "evidence": {
                "home_state": d["home_state"],
                "max_states_in_single_month": max_states,
                "months_flagged": int(d["months_flagged"] or 0),
                "total_paid_in_flagged_months": round(total_paid_flagged, 2),
                "total_claims_in_flagged_months": int(d["total_claims_flagged"] or 0),
            },
            "estimated_overpayment_usd": round(overpayment, 2),
        })
    return signals


def compute_cross_signal_correlations(signal_results: dict[str, list[dict]]) -> dict:
    """Analyze cross-signal correlations to identify multi-signal providers.

    Returns statistics about providers flagged by multiple signal types,
    which are the highest-priority investigation targets.
    """
    npi_signals: dict[str, set[str]] = {}
    for signal_type, signals in signal_results.items():
        for sig in signals:
            npi = sig["npi"]
            if npi not in npi_signals:
                npi_signals[npi] = set()
            npi_signals[npi].add(signal_type)

    # Count providers by number of signal types
    multi_signal_counts: dict[int, int] = {}
    multi_signal_npis: dict[int, list[str]] = {}
    for npi, types in npi_signals.items():
        n = len(types)
        multi_signal_counts[n] = multi_signal_counts.get(n, 0) + 1
        if n >= 2:
            if n not in multi_signal_npis:
                multi_signal_npis[n] = []
            multi_signal_npis[n].append(npi)

    # Find which signal pairs co-occur most
    pair_counts: dict[tuple[str, str], int] = {}
    for npi, types in npi_signals.items():
        type_list = sorted(types)
        for i in range(len(type_list)):
            for j in range(i + 1, len(type_list)):
                pair = (type_list[i], type_list[j])
                pair_counts[pair] = pair_counts.get(pair, 0) + 1

    top_pairs = sorted(pair_counts.items(), key=lambda x: x[1], reverse=True)[:10]

    return {
        "total_unique_providers_flagged": len(npi_signals),
        "providers_by_signal_count": {str(k): v for k, v in sorted(multi_signal_counts.items())},
        "multi_signal_providers": {
            str(k): v[:10] for k, v in sorted(multi_signal_npis.items(), reverse=True)
        },
        "top_signal_pairs": [
            {"pair": list(pair), "count": count}
            for pair, count in top_pairs
        ],
    }


def run_all_signals(con: duckdb.DuckDBPyConnection) -> dict[str, list[dict]]:
    """Run all 9 signals and return results grouped by type."""
    print("\n[1/9] Signal: Excluded Provider Still Billing...")
    excluded = signal_excluded_provider(con)
    print(f"  Found {len(excluded)} flags")

    print("[2/9] Signal: Billing Volume Outlier...")
    outlier = signal_billing_outlier(con)
    print(f"  Found {len(outlier)} flags")

    print("[3/9] Signal: Rapid Billing Escalation...")
    escalation = signal_rapid_escalation(con)
    print(f"  Found {len(escalation)} flags")

    print("[4/9] Signal: Workforce Impossibility...")
    workforce = signal_workforce_impossibility(con)
    print(f"  Found {len(workforce)} flags")

    print("[5/9] Signal: Shared Authorized Official...")
    official = signal_shared_official(con)
    print(f"  Found {len(official)} flags")

    print("[6/9] Signal: Geographic Implausibility...")
    geo = signal_geographic_implausibility(con)
    print(f"  Found {len(geo)} flags")

    print("[7/9] Signal: Address Clustering...")
    clustering = signal_address_clustering(con)
    print(f"  Found {len(clustering)} flags")

    print("[8/9] Signal: Upcoding Detection...")
    upcoding = signal_upcoding(con)
    print(f"  Found {len(upcoding)} flags")

    print("[9/9] Signal: Concurrent Billing Across States...")
    concurrent = signal_concurrent_billing(con)
    print(f"  Found {len(concurrent)} flags")

    return {
        "excluded_provider": excluded,
        "billing_outlier": outlier,
        "rapid_escalation": escalation,
        "workforce_impossibility": workforce,
        "shared_official": official,
        "geographic_implausibility": geo,
        "address_clustering": clustering,
        "upcoding": upcoding,
        "concurrent_billing": concurrent,
    }
