"""All 9 fraud signal implementations using DuckDB SQL.

Optimized to use pre-materialized tables:
  - provider_totals: per-NPI billing totals (avoids repeated GROUP BY over 227M rows)
  - provider_monthly: per-NPI per-month totals (avoids repeated monthly aggregations)
"""

import duckdb

# ---------------------------------------------------------------------------
# COVID-era awareness: March 2020 through December 2021 (PHE period)
# ---------------------------------------------------------------------------
COVID_START = "2020-03"
COVID_END = "2021-12"
COVID_HCPCS = frozenset({
    "87635", "U0003", "U0004",      # COVID testing
    "99441", "99442", "99443",       # Telehealth E/M
    "0202U", "0223U", "0225U",       # COVID molecular
    "86328", "86769",                # Antibody testing
    "J0878",                          # COVID monoclonal treatment
})


def _is_covid_era(month_str: str) -> bool:
    """Check if a YYYY-MM or YYYY-MM-DD month string falls within COVID PHE period."""
    ym = str(month_str)[:7]
    return COVID_START <= ym <= COVID_END


def signal_excluded_provider(con: duckdb.DuckDBPyConnection) -> list[dict]:
    """Signal 1: Excluded Provider Still Billing.

    A billing or servicing NPI in spending data matches an NPI in LEIE where
    exclusion date is before the claim month and reinstatement date is empty
    or after the claim month.

    Optimized: single scan of spending joining both billing_npi and
    servicing_npi against LEIE via UNION ALL inside the scan.
    """
    results = con.execute("""
        WITH spending_npis AS (
            SELECT billing_npi AS npi, claim_month, total_paid, total_claims
            FROM spending s
            WHERE EXISTS (SELECT 1 FROM leie l WHERE l.npi = s.billing_npi
                          AND l.npi IS NOT NULL AND TRIM(l.npi) != '' AND l.npi != '0000000000')
            UNION ALL
            SELECT servicing_npi AS npi, claim_month, total_paid, total_claims
            FROM spending s
            WHERE EXISTS (SELECT 1 FROM leie l WHERE l.npi = s.servicing_npi
                          AND l.npi IS NOT NULL AND TRIM(l.npi) != '' AND l.npi != '0000000000')
        ),
        matched AS (
            SELECT
                sn.npi,
                l.excl_date,
                l.excl_type,
                l.lastname,
                l.firstname,
                l.busname,
                SUM(CASE WHEN sn.claim_month >= l.excl_date THEN sn.total_paid ELSE 0 END) AS total_paid_after_exclusion,
                SUM(CASE WHEN sn.claim_month >= l.excl_date THEN sn.total_claims ELSE 0 END) AS total_claims_after_exclusion,
                MIN(CASE WHEN sn.claim_month >= l.excl_date THEN sn.claim_month ELSE NULL END) AS first_claim_after_exclusion,
                MAX(CASE WHEN sn.claim_month >= l.excl_date THEN sn.claim_month ELSE NULL END) AS last_claim_after_exclusion
            FROM spending_npis sn
            JOIN leie l ON sn.npi = l.npi
            WHERE l.excl_date IS NOT NULL
              AND l.excl_date < sn.claim_month
              AND (l.rein_date IS NULL OR l.rein_date > sn.claim_month)
            GROUP BY sn.npi, l.excl_date, l.excl_type, l.lastname, l.firstname, l.busname
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
        FROM matched
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

    Optimized: reads from pre-materialized provider_totals table.
    """
    results = con.execute("""
        WITH provider_with_nppes AS (
            SELECT
                pt.npi,
                pt.total_paid AS provider_total_paid,
                pt.total_claims AS provider_total_claims,
                pt.total_beneficiaries AS provider_total_beneficiaries,
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

    Optimized: uses provider_monthly instead of re-aggregating spending.
    """
    results = con.execute("""
        WITH provider_first_bill AS (
            SELECT
                npi,
                MIN(claim_month) AS first_bill_month
            FROM provider_monthly
            GROUP BY npi
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
              AND n.enumeration_date >= CAST(pfb.first_bill_month AS DATE) - INTERVAL '24 months'
              AND n.enumeration_date < CAST(pfb.first_bill_month AS DATE)
        ),
        monthly_paid AS (
            SELECT
                pm.npi,
                pm.claim_month,
                pm.month_paid,
                ROW_NUMBER() OVER (PARTITION BY pm.npi ORDER BY pm.claim_month) AS month_num
            FROM provider_monthly pm
            JOIN new_providers np ON pm.npi = np.npi
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
        evidence = {
            "enumeration_date": str(d["enumeration_date"]),
            "first_billing_month": str(d["first_bill_month"]),
            "peak_3_month_growth_rate": round(peak, 2),
            "monthly_amounts_first_12": [float(x) for x in (d["monthly_amounts"] or [])],
        }
        # COVID-era: new providers enrolling during pandemic often had legitimate spikes
        if _is_covid_era(str(d["first_bill_month"])):
            severity = "low"
            evidence["covid_era_flag"] = True
        signals.append({
            "signal_type": "rapid_escalation",
            "severity": severity,
            "npi": d["npi"],
            "evidence": evidence,
            "estimated_overpayment_usd": float(d["overpayment_in_growth_months"] or 0),
        })
    return signals


def signal_workforce_impossibility(con: duckdb.DuckDBPyConnection) -> list[dict]:
    """Signal 4: Workforce Impossibility.

    For organizations (entity_type=2), compute peak monthly claims scaled
    by the number of distinct servicing NPIs that month (proxy for workforce
    size). Flag if claims-per-worker-per-hour > 6 (22 workdays × 8 hrs).

    This prevents penalizing large but legitimate organizations — a clinic
    with 50 workers can legitimately handle 50× more claims than a solo
    practitioner. The signal only fires when the per-worker rate is
    physically impossible, not merely high in absolute terms.

    Uses org_worker_monthly (pre-materialized in ingest) to avoid a
    full 227M-row scan at query time.
    """
    results = con.execute("""
        WITH org_monthly AS (
            SELECT
                pm.npi,
                pm.claim_month,
                pm.month_claims,
                pm.month_paid,
                -- Distinct servicing NPIs = proxy for workforce size.
                -- Fall back to 1 if no servicing NPI data (solo-like billing).
                GREATEST(COALESCE(wm.distinct_workers, 1), 1) AS distinct_workers
            FROM provider_monthly pm
            JOIN nppes n ON pm.npi = n.npi
            LEFT JOIN org_worker_monthly wm
                   ON pm.npi = wm.npi AND pm.claim_month = wm.claim_month
            WHERE n.entity_type_code = '2'
        ),
        peak_months AS (
            SELECT
                npi,
                claim_month AS peak_month,
                month_claims AS peak_claims,
                month_paid AS peak_month_paid,
                distinct_workers,
                -- Per-worker hourly rate: total claims / workers / 22 days / 8 hrs
                month_claims / distinct_workers / 22.0 / 8.0 AS claims_per_worker_hour,
                ROW_NUMBER() OVER (
                    PARTITION BY npi
                    ORDER BY month_claims / distinct_workers DESC
                ) AS rn
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
            pm.distinct_workers,
            pm.claims_per_worker_hour
        FROM peak_months pm
        JOIN nppes n ON pm.npi = n.npi
        WHERE pm.rn = 1
          AND pm.claims_per_worker_hour > 6.0
        ORDER BY pm.claims_per_worker_hour DESC
    """).fetchall()

    columns = ["npi", "provider_name", "entity_type_code", "taxonomy_code", "state",
                "peak_month", "peak_claims", "peak_month_paid", "distinct_workers",
                "claims_per_worker_hour"]

    signals = []
    for row in results:
        d = dict(zip(columns, row))
        peak_claims = int(d["peak_claims"] or 0)
        workers = int(d["distinct_workers"] or 1)
        max_reasonable = workers * 6 * 8 * 22  # 6 claims/hr × 8 hrs × 22 days × workers
        excess = max(0, peak_claims - max_reasonable)
        peak_paid = float(d["peak_month_paid"] or 0)
        cost_per_claim = peak_paid / peak_claims if peak_claims > 0 else 0
        overpayment = excess * cost_per_claim
        rate = float(d["claims_per_worker_hour"] or 0)
        severity = "high" if rate > 20 else "medium"
        evidence = {
            "peak_month": str(d["peak_month"]),
            "peak_claims_count": peak_claims,
            "distinct_workers_in_month": workers,
            "implied_claims_per_worker_hour": round(rate, 2),
            "total_paid_peak_month": round(peak_paid, 2),
        }
        # COVID-era: telehealth legitimately allows higher patient volumes
        if _is_covid_era(str(d["peak_month"])):
            severity = "low"
            evidence["covid_era_flag"] = True

        signals.append({
            "signal_type": "workforce_impossibility",
            "severity": severity,
            "npi": d["npi"],
            "evidence": evidence,
            "estimated_overpayment_usd": round(overpayment, 2),
        })
    return signals


def signal_shared_official(con: duckdb.DuckDBPyConnection) -> list[dict]:
    """Signal 5: Shared Authorized Official Across Multiple NPIs.

    Group NPIs by authorized official name. Flag if 5–50 NPIs with combined
    total paid > $1M AND at least 3 NPIs share the same state.

    The upper bound of 50 NPIs excludes large legitimate health systems and
    state agencies (Quest Diagnostics, State of Missouri, etc.) that have a
    single corporate officer on record for all NPIs.

    The geographic concentration requirement (≥3 NPIs in same state) filters
    out name-collision false positives — unrelated providers sharing a common
    name like "Michael Morris" are typically scattered across different states.

    Optimized: reads from pre-materialized provider_totals table.
    """
    results = con.execute("""
        WITH official_with_paid AS (
            SELECT
                UPPER(TRIM(n.auth_official_last)) AS official_last,
                UPPER(TRIM(n.auth_official_first)) AS official_first,
                n.npi,
                n.org_name,
                n.state,
                COALESCE(pt.total_paid, 0) AS total_paid
            FROM nppes n
            LEFT JOIN provider_totals pt ON n.npi = pt.npi
            WHERE n.auth_official_last IS NOT NULL
              AND TRIM(n.auth_official_last) != ''
              AND n.auth_official_first IS NOT NULL
              AND TRIM(n.auth_official_first) != ''
        ),
        -- Per-official state concentration: how many NPIs share the most-common state
        state_concentration AS (
            SELECT
                official_last,
                official_first,
                MAX(state_npi_count) AS max_state_npis
            FROM (
                SELECT
                    official_last,
                    official_first,
                    state,
                    COUNT(DISTINCT npi) AS state_npi_count
                FROM official_with_paid
                WHERE state IS NOT NULL AND TRIM(state) != ''
                GROUP BY official_last, official_first, state
            )
            GROUP BY official_last, official_first
        )
        SELECT
            owp.official_last,
            owp.official_first,
            COUNT(DISTINCT owp.npi) AS npi_count,
            LIST(DISTINCT owp.npi) AS npi_list,
            LIST(DISTINCT owp.org_name) AS org_names,
            SUM(owp.total_paid) AS combined_total_paid,
            sc.max_state_npis
        FROM official_with_paid owp
        JOIN state_concentration sc
          ON owp.official_last = sc.official_last
         AND owp.official_first = sc.official_first
        GROUP BY owp.official_last, owp.official_first, sc.max_state_npis
        HAVING COUNT(DISTINCT owp.npi) BETWEEN 5 AND 50
          AND SUM(owp.total_paid) > 1000000
          AND sc.max_state_npis >= 3
        ORDER BY combined_total_paid DESC
    """).fetchall()

    columns = ["official_last", "official_first", "npi_count", "npi_list",
                "org_names", "combined_total_paid", "max_state_npis"]

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

    A billing provider registered in state X whose services are overwhelmingly
    rendered in states other than X is operating in a geographically implausible
    way — either the NPI registration is fraudulent, the provider is billing
    from a jurisdiction where they have no actual presence, or services are
    fabricated altogether.

    Uses serv_state_monthly (billing_npi × claim_month × servicing-provider state)
    to measure where services actually occur, compared to the billing NPI's
    own registered state in NPPES.

    Threshold: <10% of claims serviced in home state, across ≥500 total claims
    and ≥2 distinct non-home service states. Minimum $50,000 total paid.

    Individuals only (entity_type='1'): multi-state organizations (home health
    chains, fiscal intermediaries) legitimately operate across states and are
    excluded to avoid large volumes of false positives.
    """
    results = con.execute("""
        WITH billing_home AS (
            SELECT npi, state AS home_state
            FROM nppes
            WHERE state IS NOT NULL AND TRIM(state) != ''
              AND entity_type_code = '1'
        ),
        provider_geo AS (
            SELECT
                ssm.npi,
                bh.home_state,
                SUM(ssm.month_claims) AS total_claims,
                SUM(ssm.month_paid)   AS total_paid,
                SUM(CASE WHEN ssm.state = bh.home_state
                         THEN ssm.month_claims ELSE 0 END) AS home_state_claims,
                COUNT(DISTINCT CASE WHEN ssm.state != bh.home_state
                                    THEN ssm.state END) AS foreign_state_count
            FROM serv_state_monthly ssm
            JOIN billing_home bh ON ssm.npi = bh.npi
            GROUP BY ssm.npi, bh.home_state
            HAVING SUM(ssm.month_claims) >= 500
               AND SUM(ssm.month_paid)   >= 50000
               AND COUNT(DISTINCT CASE WHEN ssm.state != bh.home_state
                                       THEN ssm.state END) >= 2
        )
        SELECT
            pg.npi,
            COALESCE(n.org_name, n.first_name || ' ' || n.last_name) AS provider_name,
            n.entity_type_code,
            n.taxonomy_code,
            pg.home_state,
            pg.total_claims,
            pg.total_paid,
            pg.home_state_claims,
            pg.foreign_state_count,
            pg.home_state_claims * 100.0 / NULLIF(pg.total_claims, 0) AS home_state_pct
        FROM provider_geo pg
        LEFT JOIN nppes n ON pg.npi = n.npi
        WHERE pg.home_state_claims * 100.0 / NULLIF(pg.total_claims, 0) < 10.0
        ORDER BY pg.total_paid DESC
    """).fetchall()

    columns = ["npi", "provider_name", "entity_type_code", "taxonomy_code", "home_state",
               "total_claims", "total_paid", "home_state_claims", "foreign_state_count",
               "home_state_pct"]

    signals = []
    for row in results:
        d = dict(zip(columns, row))
        total_claims = int(d["total_claims"] or 0)
        home_claims = int(d["home_state_claims"] or 0)
        total_paid = float(d["total_paid"] or 0)
        home_pct = float(d["home_state_pct"] or 0)
        foreign_states = int(d["foreign_state_count"] or 0)

        # Overpayment: billing outside home state may be phantom — conservatively
        # flag the foreign-state proportion of payments
        foreign_fraction = max(0, 1.0 - home_pct / 100.0)
        overpayment = total_paid * foreign_fraction * 0.4  # 40% of foreign billing

        severity = "high" if home_pct < 2.0 and foreign_states >= 5 else "medium"

        signals.append({
            "signal_type": "geographic_implausibility",
            "severity": severity,
            "npi": d["npi"],
            "evidence": {
                "registered_state": d["home_state"],
                "home_state_claims": home_claims,
                "total_claims": total_claims,
                "home_state_pct": round(home_pct, 2),
                "foreign_states_count": foreign_states,
                "total_paid": round(total_paid, 2),
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

    Optimized: reads from pre-materialized provider_totals table.
    """
    results = con.execute("""
        WITH zip_clusters AS (
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

    Optimized: reads from pre-materialized spending_em table.
    """
    results = con.execute("""
        WITH provider_em AS (
            SELECT
                s.billing_npi AS npi,
                SUM(s.total_claims) AS total_em_claims,
                SUM(CASE WHEN s.hcpcs_code IN ('99215','99205','99223','99233','99245','99255')
                    THEN s.total_claims ELSE 0 END) AS high_level_claims,
                SUM(s.total_paid) AS total_paid
            FROM spending_em s
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

    Optimized: reads from pre-materialized serv_state_monthly table
    (avoids the heavy spending × nppes join at query time).
    """
    results = con.execute("""
        WITH npi_state_months AS (
            SELECT
                npi,
                claim_month,
                COUNT(DISTINCT state) AS state_count,
                LIST(DISTINCT state) AS states,
                SUM(month_paid) AS month_paid,
                SUM(month_claims) AS month_claims
            FROM serv_state_monthly
            GROUP BY npi, claim_month
            HAVING COUNT(DISTINCT state) >= 5
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


def signal_burst_enrollment_network(con: duckdb.DuckDBPyConnection) -> list[dict]:
    """Signal 10: Burst Enrollment Network.

    Detects clusters of organizations registered within the same quarter,
    sharing taxonomy code and state, with significant combined billing.
    This is the shell-company incorporation pattern: a fraud ring registers
    many entities in rapid succession before ramping up billing.
    """
    results = con.execute("""
        WITH org_enum AS (
            SELECT
                n.npi,
                n.org_name,
                n.state,
                n.zip_code,
                n.taxonomy_code,
                n.enumeration_date,
                DATE_TRUNC('quarter', n.enumeration_date) AS enum_quarter,
                COALESCE(pt.total_paid, 0) AS total_paid,
                COALESCE(pt.total_claims, 0) AS total_claims,
                COALESCE(pt.total_beneficiaries, 0) AS total_beneficiaries
            FROM nppes n
            LEFT JOIN provider_totals pt ON n.npi = pt.npi
            WHERE n.entity_type_code = '2'
              AND n.enumeration_date IS NOT NULL
              AND n.taxonomy_code IS NOT NULL
              AND COALESCE(pt.total_paid, 0) > 0
        ),
        quarter_clusters AS (
            SELECT
                taxonomy_code,
                state,
                enum_quarter,
                COUNT(*) AS npi_count,
                LIST(DISTINCT npi) AS npi_list,
                LIST(DISTINCT org_name) AS org_names,
                SUM(total_paid) AS combined_paid,
                SUM(total_claims) AS combined_claims,
                SUM(total_beneficiaries) AS combined_beneficiaries,
                MIN(enumeration_date) AS earliest_enum,
                MAX(enumeration_date) AS latest_enum,
                DATEDIFF('day', MIN(enumeration_date), MAX(enumeration_date)) AS span_days
            FROM org_enum
            GROUP BY taxonomy_code, state, enum_quarter
            HAVING COUNT(*) >= 4
               AND SUM(total_paid) > 500000
        )
        SELECT * FROM quarter_clusters
        ORDER BY combined_paid DESC
    """).fetchall()

    columns = ["taxonomy_code", "state", "enum_quarter", "npi_count", "npi_list",
               "org_names", "combined_paid", "combined_claims", "combined_beneficiaries",
               "earliest_enum", "latest_enum", "span_days"]

    signals = []
    for row in results:
        d = dict(zip(columns, row))
        npi_list = d["npi_list"] if isinstance(d["npi_list"], list) else []
        npi_count = int(d["npi_count"] or 0)
        combined_paid = float(d["combined_paid"] or 0)
        severity = "high" if npi_count >= 8 or combined_paid > 5_000_000 else "medium"
        overpayment = combined_paid * 0.25
        evidence = {
            "taxonomy_code": d["taxonomy_code"],
            "state": d["state"],
            "npi_count": npi_count,
            "enrolled_npis": npi_list[:20],
            "organization_names": (d["org_names"] if isinstance(d["org_names"], list) else [])[:20],
            "earliest_enumeration": str(d["earliest_enum"]),
            "latest_enumeration": str(d["latest_enum"]),
            "enrollment_span_days": int(d["span_days"] or 0),
            "combined_total_paid": round(combined_paid, 2),
            "combined_total_claims": int(d["combined_claims"] or 0),
            "combined_total_beneficiaries": int(d["combined_beneficiaries"] or 0),
        }
        # COVID-era: many legitimate providers enrolled during pandemic
        if _is_covid_era(str(d["enum_quarter"])):
            severity = "low"
            evidence["covid_era_flag"] = True

        signals.append({
            "signal_type": "burst_enrollment_network",
            "severity": severity,
            "npi": npi_list[0] if npi_list else "UNKNOWN",
            "evidence": evidence,
            "estimated_overpayment_usd": round(overpayment, 2),
        })
    return signals


def signal_coordinated_billing_ramp(con: duckdb.DuckDBPyConnection) -> list[dict]:
    """Signal 11: Coordinated Billing Ramp.

    Detects networks (via shared authorized official) where 3+ NPIs all
    have their peak billing month within a 3-month window — a coordination
    fingerprint that individual escalation analysis misses.
    """
    results = con.execute("""
        WITH official_networks AS (
            SELECT
                UPPER(TRIM(n.auth_official_last)) || '|' || UPPER(TRIM(n.auth_official_first)) AS network_key,
                UPPER(TRIM(n.auth_official_first)) AS official_first,
                UPPER(TRIM(n.auth_official_last)) AS official_last,
                n.npi,
                n.org_name
            FROM nppes n
            WHERE n.auth_official_last IS NOT NULL
              AND TRIM(n.auth_official_last) != ''
              AND n.auth_official_first IS NOT NULL
              AND TRIM(n.auth_official_first) != ''
        ),
        npi_peaks AS (
            SELECT
                on_net.network_key,
                on_net.official_first,
                on_net.official_last,
                pm.npi,
                pm.claim_month AS peak_month,
                pm.month_paid AS peak_paid,
                ROW_NUMBER() OVER (PARTITION BY pm.npi ORDER BY pm.month_paid DESC) AS rn
            FROM provider_monthly pm
            JOIN official_networks on_net ON pm.npi = on_net.npi
        ),
        network_peak_analysis AS (
            SELECT
                network_key,
                official_first,
                official_last,
                COUNT(DISTINCT npi) AS npis_in_network,
                MIN(CASE WHEN rn = 1 THEN peak_month END) AS earliest_peak,
                MAX(CASE WHEN rn = 1 THEN peak_month END) AS latest_peak,
                DATEDIFF('month',
                    MIN(CASE WHEN rn = 1 THEN peak_month END),
                    MAX(CASE WHEN rn = 1 THEN peak_month END)
                ) AS peak_spread_months,
                SUM(CASE WHEN rn = 1 THEN peak_paid ELSE 0 END) AS combined_peak_paid,
                LIST(DISTINCT CASE WHEN rn = 1 THEN npi END) AS npi_list
            FROM npi_peaks
            GROUP BY network_key, official_first, official_last
            HAVING COUNT(DISTINCT npi) >= 3
        ),
        with_totals AS (
            SELECT
                npa.*,
                (SELECT SUM(pt.total_paid) FROM provider_totals pt
                 JOIN official_networks on2 ON pt.npi = on2.npi
                 WHERE on2.network_key = npa.network_key) AS network_total_paid,
                (SELECT LIST(DISTINCT on2.org_name) FROM official_networks on2
                 WHERE on2.network_key = npa.network_key) AS org_names
            FROM network_peak_analysis npa
            WHERE npa.peak_spread_months <= 3
              AND npa.combined_peak_paid > 200000
        )
        SELECT * FROM with_totals
        ORDER BY network_total_paid DESC
    """).fetchall()

    columns = ["network_key", "official_first", "official_last", "npis_in_network",
               "earliest_peak", "latest_peak", "peak_spread_months", "combined_peak_paid",
               "npi_list", "network_total_paid", "org_names"]

    signals = []
    for row in results:
        d = dict(zip(columns, row))
        npi_list = d["npi_list"] if isinstance(d["npi_list"], list) else []
        # Filter out None values from the CASE expression
        npi_list = [x for x in npi_list if x is not None]
        npis = int(d["npis_in_network"] or 0)
        spread = int(d["peak_spread_months"] or 0)
        network_paid = float(d["network_total_paid"] or 0)

        if spread <= 1 and npis >= 5 and network_paid > 2_000_000:
            severity = "critical"
        elif spread <= 3 and npis >= 3:
            severity = "high"
        else:
            severity = "medium"

        overpayment = network_paid * 0.3

        signals.append({
            "signal_type": "coordinated_billing_ramp",
            "severity": severity,
            "npi": npi_list[0] if npi_list else "UNKNOWN",
            "evidence": {
                "authorized_official_name": f"{d['official_first']} {d['official_last']}",
                "npis_in_network": npis,
                "network_npis": npi_list[:20],
                "organization_names": (d["org_names"] if isinstance(d["org_names"], list) else [])[:20],
                "earliest_peak_month": str(d["earliest_peak"]),
                "latest_peak_month": str(d["latest_peak"]),
                "peak_spread_months": spread,
                "combined_peak_paid": round(float(d["combined_peak_paid"] or 0), 2),
                "network_total_paid": round(network_paid, 2),
            },
            "estimated_overpayment_usd": round(overpayment, 2),
        })
    return signals


def signal_phantom_servicing_hub(con: duckdb.DuckDBPyConnection) -> list[dict]:
    """Signal 12: Phantom Servicing Hub.

    Detects a single servicing_npi that appears across 5+ distinct
    billing_npi entities — indicating a phantom referral hub, kickback
    arrangement, or fabricated servicing relationship.

    Optimized: reads from pre-materialized servicing_hub_totals table.
    """
    results = con.execute("""
        WITH hub_agg AS (
            SELECT
                servicing_npi,
                COUNT(DISTINCT billing_npi) AS distinct_billing_npis,
                SUM(total_paid) AS total_paid,
                SUM(total_claims) AS total_claims,
                SUM(total_beneficiaries) AS total_beneficiaries
            FROM servicing_hub_totals
            GROUP BY servicing_npi
            HAVING COUNT(DISTINCT billing_npi) >= 5
               AND SUM(total_paid) > 500000
        ),
        hub_details AS (
            SELECT
                ha.*,
                ha.total_beneficiaries * 1.0 / NULLIF(ha.total_claims, 0) AS bene_claim_ratio,
                COALESCE(n.org_name, n.first_name || ' ' || n.last_name) AS servicing_provider_name,
                n.entity_type_code,
                n.taxonomy_code,
                n.state,
                (SELECT LIST(DISTINCT sht.billing_npi)
                 FROM servicing_hub_totals sht
                 WHERE sht.servicing_npi = ha.servicing_npi) AS billing_npi_list
            FROM hub_agg ha
            LEFT JOIN nppes n ON ha.servicing_npi = n.npi
        )
        SELECT * FROM hub_details
        ORDER BY total_paid DESC
    """).fetchall()

    columns = ["servicing_npi", "distinct_billing_npis", "total_paid", "total_claims",
               "total_beneficiaries", "bene_claim_ratio", "servicing_provider_name",
               "entity_type_code", "taxonomy_code", "state", "billing_npi_list"]

    signals = []
    for row in results:
        d = dict(zip(columns, row))
        billing_count = int(d["distinct_billing_npis"] or 0)
        total_paid = float(d["total_paid"] or 0)
        bene_ratio = float(d["bene_claim_ratio"] or 0)
        billing_npi_list = d["billing_npi_list"] if isinstance(d["billing_npi_list"], list) else []

        if billing_count >= 15 or (billing_count >= 10 and bene_ratio < 0.1):
            severity = "critical"
        elif billing_count >= 10 or total_paid > 2_000_000:
            severity = "high"
        else:
            severity = "medium"

        overpayment = total_paid * 0.35

        signals.append({
            "signal_type": "phantom_servicing_hub",
            "severity": severity,
            "npi": d["servicing_npi"],
            "evidence": {
                "servicing_provider_name": d["servicing_provider_name"] or "Unknown",
                "taxonomy_code": d["taxonomy_code"],
                "state": d["state"],
                "distinct_billing_npis": billing_count,
                "billing_npi_list": billing_npi_list[:20],
                "total_paid_through_hub": round(total_paid, 2),
                "total_claims": int(d["total_claims"] or 0),
                "total_beneficiaries": int(d["total_beneficiaries"] or 0),
                "beneficiary_claim_ratio": round(bene_ratio, 4),
            },
            "estimated_overpayment_usd": round(overpayment, 2),
        })
    return signals


def signal_network_beneficiary_dilution(con: duckdb.DuckDBPyConnection) -> list[dict]:
    """Signal 13: Network Beneficiary Dilution.

    For networks defined by shared authorized official, computes the
    network-wide beneficiary-to-claims ratio. Flags networks where
    combined unique beneficiaries are very low relative to combined claims,
    suggesting beneficiary recycling across shell entities.
    """
    results = con.execute("""
        WITH official_networks AS (
            SELECT
                UPPER(TRIM(n.auth_official_last)) || '|' || UPPER(TRIM(n.auth_official_first)) AS network_key,
                UPPER(TRIM(n.auth_official_first)) AS official_first,
                UPPER(TRIM(n.auth_official_last)) AS official_last,
                n.npi,
                n.org_name
            FROM nppes n
            WHERE n.auth_official_last IS NOT NULL
              AND TRIM(n.auth_official_last) != ''
              AND n.auth_official_first IS NOT NULL
              AND TRIM(n.auth_official_first) != ''
        ),
        network_totals AS (
            SELECT
                on_net.network_key,
                on_net.official_first,
                on_net.official_last,
                COUNT(DISTINCT on_net.npi) AS npi_count,
                LIST(DISTINCT on_net.npi) AS npi_list,
                LIST(DISTINCT on_net.org_name) AS org_names,
                SUM(pt.total_paid) AS combined_paid,
                SUM(pt.total_claims) AS combined_claims,
                SUM(pt.total_beneficiaries) AS combined_beneficiaries,
                SUM(pt.total_beneficiaries) * 1.0 / NULLIF(SUM(pt.total_claims), 0) AS network_bene_ratio,
                SUM(pt.total_claims) * 1.0 / NULLIF(SUM(pt.total_beneficiaries), 0) AS claims_per_bene
            FROM official_networks on_net
            JOIN provider_totals pt ON on_net.npi = pt.npi
            GROUP BY on_net.network_key, on_net.official_first, on_net.official_last
            HAVING COUNT(DISTINCT on_net.npi) >= 3
               AND SUM(pt.total_paid) > 500000
               AND SUM(pt.total_claims) > 0
        ),
        peer_stats AS (
            SELECT
                PERCENTILE_CONT(0.1) WITHIN GROUP (ORDER BY network_bene_ratio) AS p10_bene_ratio,
                PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY network_bene_ratio) AS median_bene_ratio,
                PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY claims_per_bene) AS median_claims_per_bene
            FROM network_totals
        )
        SELECT
            nt.*,
            ps.p10_bene_ratio,
            ps.median_bene_ratio,
            ps.median_claims_per_bene
        FROM network_totals nt
        CROSS JOIN peer_stats ps
        WHERE nt.claims_per_bene > 50
           OR nt.network_bene_ratio < ps.p10_bene_ratio
        ORDER BY nt.combined_paid DESC
    """).fetchall()

    columns = ["network_key", "official_first", "official_last", "npi_count",
               "npi_list", "org_names", "combined_paid", "combined_claims",
               "combined_beneficiaries", "network_bene_ratio", "claims_per_bene",
               "p10_bene_ratio", "median_bene_ratio", "median_claims_per_bene"]

    signals = []
    for row in results:
        d = dict(zip(columns, row))
        npi_list = d["npi_list"] if isinstance(d["npi_list"], list) else []
        npi_count = int(d["npi_count"] or 0)
        combined_paid = float(d["combined_paid"] or 0)
        combined_claims = int(d["combined_claims"] or 0)
        combined_benes = int(d["combined_beneficiaries"] or 0)
        claims_per_bene = float(d["claims_per_bene"] or 0)
        bene_ratio = float(d["network_bene_ratio"] or 0)
        median_cpb = float(d["median_claims_per_bene"] or 1)

        if claims_per_bene > 100 and combined_paid > 2_000_000:
            severity = "critical"
        elif claims_per_bene > 50 or (bene_ratio < 0.02 and npi_count >= 5):
            severity = "high"
        else:
            severity = "medium"

        # Overpayment: excess claims above peer median rate × avg cost per claim
        if combined_benes > 0 and median_cpb > 0:
            expected_claims = combined_benes * median_cpb
            excess_claims = max(0, combined_claims - expected_claims)
            cost_per_claim = combined_paid / combined_claims if combined_claims > 0 else 0
            overpayment = min(excess_claims * cost_per_claim, combined_paid * 0.8)
        else:
            overpayment = combined_paid * 0.5

        signals.append({
            "signal_type": "network_beneficiary_dilution",
            "severity": severity,
            "npi": npi_list[0] if npi_list else "UNKNOWN",
            "evidence": {
                "authorized_official_name": f"{d['official_first']} {d['official_last']}",
                "npi_count": npi_count,
                "network_npis": npi_list[:20],
                "organization_names": (d["org_names"] if isinstance(d["org_names"], list) else [])[:20],
                "combined_total_paid": round(combined_paid, 2),
                "combined_total_claims": combined_claims,
                "combined_total_beneficiaries": combined_benes,
                "claims_per_beneficiary": round(claims_per_bene, 2),
                "network_beneficiary_ratio": round(bene_ratio, 4),
                "peer_median_claims_per_bene": round(median_cpb, 2),
            },
            "estimated_overpayment_usd": round(overpayment, 2),
        })
    return signals


def signal_caregiver_density_anomaly(con: duckdb.DuckDBPyConnection) -> list[dict]:
    """Signal 14: Family Caregiver Density Anomaly.

    Detects zip codes where home health / PCA billing is anomalously
    concentrated among individual providers with very few beneficiaries —
    the pattern of family members billing as caregivers for a relative.

    Reads from pre-materialized hh_zip_totals table (built at ingest time)
    so the 227M-row spending table is never scanned at query time.

    Optionally enriches with Census ACS ZCTA demographics if the
    census_zcta table exists, comparing actual billing to demographic
    expectations (elderly + disabled population).
    """
    # Check if census enrichment is available
    has_census = False
    try:
        con.execute("SELECT 1 FROM census_zcta LIMIT 1")
        has_census = True
    except Exception:
        pass

    if has_census:
        query = """
            WITH zip_agg AS (
                SELECT
                    zip_code,
                    state,
                    COUNT(DISTINCT npi) AS provider_count,
                    COUNT(DISTINCT CASE WHEN entity_type_code = '1' THEN npi END) AS individual_provider_count,
                    SUM(hh_paid) AS total_hh_paid,
                    SUM(hh_claims) AS total_hh_claims,
                    SUM(hh_beneficiaries) AS total_hh_beneficiaries,
                    LIST(DISTINCT npi) AS npi_list,
                    LIST(DISTINCT provider_name) AS provider_names
                FROM hh_zip_totals
                GROUP BY zip_code, state
                HAVING SUM(hh_paid) >= 100000
            ),
            state_medians AS (
                SELECT
                    state,
                    PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY total_hh_paid) AS median_hh_paid,
                    COUNT(*) AS zips_in_state
                FROM zip_agg
                GROUP BY state
                HAVING COUNT(*) >= 3
            ),
            with_census AS (
                SELECT
                    za.*,
                    sm.median_hh_paid,
                    sm.zips_in_state,
                    za.total_hh_paid / NULLIF(sm.median_hh_paid, 0) AS ratio_to_state_median,
                    za.individual_provider_count * 1.0 / NULLIF(za.provider_count, 0) AS individual_ratio,
                    za.total_hh_beneficiaries * 1.0 / NULLIF(za.individual_provider_count, 0) AS benes_per_individual,
                    c.total_population,
                    c.population_65_plus,
                    c.disability_count,
                    (c.population_65_plus + c.disability_count) AS vulnerable_population,
                    za.total_hh_paid * 1.0 / NULLIF(c.population_65_plus + c.disability_count, 0) AS paid_per_vulnerable
                FROM zip_agg za
                JOIN state_medians sm ON za.state = sm.state
                LEFT JOIN census_zcta c ON LEFT(za.zip_code, 5) = c.zcta
            ),
            census_state_medians AS (
                SELECT
                    state,
                    PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY paid_per_vulnerable) AS median_paid_per_vulnerable
                FROM with_census
                WHERE paid_per_vulnerable IS NOT NULL AND paid_per_vulnerable > 0
                GROUP BY state
                HAVING COUNT(*) >= 3
            )
            SELECT
                wc.*,
                csm.median_paid_per_vulnerable,
                wc.paid_per_vulnerable / NULLIF(csm.median_paid_per_vulnerable, 0) AS census_ratio
            FROM with_census wc
            LEFT JOIN census_state_medians csm ON wc.state = csm.state
            WHERE wc.ratio_to_state_median > 3.0
              AND wc.individual_ratio > 0.5
              AND wc.benes_per_individual < 5.0
            ORDER BY wc.total_hh_paid DESC
        """
    else:
        query = """
            WITH zip_agg AS (
                SELECT
                    zip_code,
                    state,
                    COUNT(DISTINCT npi) AS provider_count,
                    COUNT(DISTINCT CASE WHEN entity_type_code = '1' THEN npi END) AS individual_provider_count,
                    SUM(hh_paid) AS total_hh_paid,
                    SUM(hh_claims) AS total_hh_claims,
                    SUM(hh_beneficiaries) AS total_hh_beneficiaries,
                    LIST(DISTINCT npi) AS npi_list,
                    LIST(DISTINCT provider_name) AS provider_names
                FROM hh_zip_totals
                GROUP BY zip_code, state
                HAVING SUM(hh_paid) >= 100000
            ),
            state_medians AS (
                SELECT
                    state,
                    PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY total_hh_paid) AS median_hh_paid,
                    COUNT(*) AS zips_in_state
                FROM zip_agg
                GROUP BY state
                HAVING COUNT(*) >= 3
            )
            SELECT
                za.*,
                sm.median_hh_paid,
                sm.zips_in_state,
                za.total_hh_paid / NULLIF(sm.median_hh_paid, 0) AS ratio_to_state_median,
                za.individual_provider_count * 1.0 / NULLIF(za.provider_count, 0) AS individual_ratio,
                za.total_hh_beneficiaries * 1.0 / NULLIF(za.individual_provider_count, 0) AS benes_per_individual,
                NULL::INTEGER AS total_population,
                NULL::INTEGER AS population_65_plus,
                NULL::INTEGER AS disability_count,
                NULL::INTEGER AS vulnerable_population,
                NULL::DOUBLE AS paid_per_vulnerable,
                NULL::DOUBLE AS median_paid_per_vulnerable,
                NULL::DOUBLE AS census_ratio
            FROM zip_agg za
            JOIN state_medians sm ON za.state = sm.state
            WHERE za.total_hh_paid / NULLIF(sm.median_hh_paid, 0) > 3.0
              AND za.individual_provider_count * 1.0 / NULLIF(za.provider_count, 0) > 0.5
              AND za.total_hh_beneficiaries * 1.0 / NULLIF(za.individual_provider_count, 0) < 5.0
            ORDER BY za.total_hh_paid DESC
        """

    results = con.execute(query).fetchall()

    columns = [
        "zip_code", "state", "provider_count", "individual_provider_count",
        "total_hh_paid", "total_hh_claims", "total_hh_beneficiaries",
        "npi_list", "provider_names",
        "median_hh_paid", "zips_in_state", "ratio_to_state_median",
        "individual_ratio", "benes_per_individual",
        "total_population", "population_65_plus", "disability_count",
        "vulnerable_population", "paid_per_vulnerable",
        "median_paid_per_vulnerable", "census_ratio",
    ]

    signals = []
    for row in results:
        d = dict(zip(columns, row))
        npi_list = d["npi_list"] if isinstance(d["npi_list"], list) else []
        total_hh_paid = float(d["total_hh_paid"] or 0)
        ratio = float(d["ratio_to_state_median"] or 0)
        median = float(d["median_hh_paid"] or 0)
        benes_per_indiv = float(d["benes_per_individual"] or 0)
        census_ratio = float(d["census_ratio"] or 0) if d["census_ratio"] else None

        # Severity: high if billing > 5x state median or census_ratio > 5
        if ratio > 5.0 or (census_ratio and census_ratio > 5.0) or total_hh_paid > 500000:
            severity = "high"
        else:
            severity = "medium"

        # Overpayment: excess above state median × 40%
        overpayment = max(0, total_hh_paid - median) * 0.4

        evidence = {
            "zip_code": d["zip_code"],
            "state": d["state"],
            "provider_count": int(d["provider_count"] or 0),
            "individual_provider_count": int(d["individual_provider_count"] or 0),
            "individual_provider_ratio": round(float(d["individual_ratio"] or 0), 2),
            "total_hh_paid": round(total_hh_paid, 2),
            "total_hh_claims": int(d["total_hh_claims"] or 0),
            "total_hh_beneficiaries": int(d["total_hh_beneficiaries"] or 0),
            "beneficiaries_per_individual_provider": round(benes_per_indiv, 2),
            "state_median_hh_paid": round(median, 2),
            "ratio_to_state_median": round(ratio, 2),
            "flagged_npis": npi_list[:20],
            "provider_names": (d["provider_names"] if isinstance(d["provider_names"], list) else [])[:20],
        }

        if census_ratio is not None:
            evidence["census_vulnerable_population"] = int(d["vulnerable_population"] or 0)
            evidence["paid_per_vulnerable_person"] = round(float(d["paid_per_vulnerable"] or 0), 2)
            evidence["census_ratio_to_state_median"] = round(census_ratio, 2)

        # Emit one signal per flagged NPI in the zip
        for npi in npi_list:
            signals.append({
                "signal_type": "caregiver_density_anomaly",
                "severity": severity,
                "npi": npi,
                "evidence": evidence,
                "estimated_overpayment_usd": round(overpayment / len(npi_list), 2),
            })

    return signals


def signal_repetitive_service_abuse(con: duckdb.DuckDBPyConnection) -> list[dict]:
    """Signal 15: Repetitive Service Abuse / Therapy Mill.

    Flags providers billing the same HCPCS code at an impossibly high
    claims-per-beneficiary ratio compared to national peers for that code.
    """
    query = """
        WITH code_level AS (
            SELECT
                npi,
                hcpcs_code,
                total_claims,
                total_beneficiaries,
                total_paid,
                total_claims * 1.0 / NULLIF(total_beneficiaries, 0) AS claims_per_bene
            FROM provider_code_totals
            WHERE total_beneficiaries > 0
              AND total_claims > 200
        ),
        code_peers AS (
            SELECT
                hcpcs_code,
                PERCENTILE_CONT(0.99) WITHIN GROUP (ORDER BY claims_per_bene) AS p99_claims_per_bene,
                PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY claims_per_bene) AS median_claims_per_bene,
                COUNT(*) AS peer_count
            FROM code_level
            GROUP BY hcpcs_code
            HAVING COUNT(*) >= 10
        )
        SELECT
            cl.npi,
            cl.hcpcs_code,
            cl.total_claims,
            cl.total_beneficiaries,
            cl.total_paid,
            cl.claims_per_bene,
            cp.p99_claims_per_bene,
            cp.median_claims_per_bene,
            cp.peer_count,
            COALESCE(n.org_name, n.first_name || ' ' || n.last_name) AS provider_name,
            n.state,
            n.taxonomy_code
        FROM code_level cl
        JOIN code_peers cp ON cl.hcpcs_code = cp.hcpcs_code
        LEFT JOIN nppes n ON cl.npi = n.npi
        WHERE cl.claims_per_bene > cp.p99_claims_per_bene
        ORDER BY cl.claims_per_bene DESC
    """
    results = con.execute(query).fetchall()
    columns = [
        "npi", "hcpcs_code", "total_claims", "total_beneficiaries", "total_paid",
        "claims_per_bene", "p99_claims_per_bene", "median_claims_per_bene",
        "peer_count", "provider_name", "state", "taxonomy_code",
    ]

    signals = []
    for row in results:
        d = dict(zip(columns, row))
        claims_per_bene = float(d["claims_per_bene"] or 0)
        p99 = float(d["p99_claims_per_bene"] or 0)
        total_claims = int(d["total_claims"] or 0)
        total_benes = int(d["total_beneficiaries"] or 0)
        total_paid = float(d["total_paid"] or 0)

        severity = "high" if p99 > 0 and claims_per_bene > p99 * 3 else "medium"

        excess_claims = max(0, total_claims - (p99 * total_benes))
        cost_per_claim = total_paid / max(total_claims, 1)
        overpayment = excess_claims * cost_per_claim * 0.8

        signals.append({
            "signal_type": "repetitive_service_abuse",
            "severity": severity,
            "npi": d["npi"],
            "evidence": {
                "hcpcs_code": d["hcpcs_code"],
                "total_claims": total_claims,
                "total_beneficiaries": total_benes,
                "claims_per_beneficiary": round(claims_per_bene, 1),
                "peer_99th_percentile_claims_per_bene": round(p99, 1),
                "peer_median_claims_per_bene": round(float(d["median_claims_per_bene"] or 0), 1),
                "peer_count": int(d["peer_count"] or 0),
                "total_paid": round(total_paid, 2),
                "state": d["state"],
                "taxonomy_code": d["taxonomy_code"],
            },
            "estimated_overpayment_usd": round(overpayment, 2),
        })

    return signals


def signal_billing_monoculture(con: duckdb.DuckDBPyConnection) -> list[dict]:
    """Signal 16: Billing Monoculture.

    Flags providers where >85% of all claims come from a single HCPCS code.
    Legitimate practices bill diverse code sets; fraud mills obsess on one
    high-reimbursement code.
    """
    query = """
        WITH provider_grand AS (
            SELECT
                npi,
                SUM(total_claims) AS grand_total_claims,
                SUM(total_paid) AS grand_total_paid
            FROM provider_code_totals
            GROUP BY npi
            HAVING SUM(total_claims) > 500
        ),
        code_shares AS (
            SELECT
                pct.npi,
                pct.hcpcs_code,
                pct.total_claims,
                pct.total_paid,
                pg.grand_total_claims,
                pg.grand_total_paid,
                pct.total_claims * 100.0 / NULLIF(pg.grand_total_claims, 0) AS code_share_pct,
                ROW_NUMBER() OVER (PARTITION BY pct.npi ORDER BY pct.total_claims DESC) AS rn
            FROM provider_code_totals pct
            JOIN provider_grand pg ON pct.npi = pg.npi
        )
        SELECT
            cs.npi,
            cs.hcpcs_code AS dominant_code,
            cs.code_share_pct,
            cs.total_claims AS dominant_code_claims,
            cs.total_paid AS dominant_code_paid,
            cs.grand_total_claims,
            cs.grand_total_paid,
            COALESCE(n.org_name, n.first_name || ' ' || n.last_name) AS provider_name,
            n.state,
            n.taxonomy_code
        FROM code_shares cs
        LEFT JOIN nppes n ON cs.npi = n.npi
        WHERE cs.rn = 1
          AND cs.code_share_pct > 85.0
        ORDER BY cs.code_share_pct DESC
    """
    results = con.execute(query).fetchall()
    columns = [
        "npi", "dominant_code", "code_share_pct", "dominant_code_claims",
        "dominant_code_paid", "grand_total_claims", "grand_total_paid",
        "provider_name", "state", "taxonomy_code",
    ]

    signals = []
    for row in results:
        d = dict(zip(columns, row))
        share = float(d["code_share_pct"] or 0)
        grand_paid = float(d["grand_total_paid"] or 0)

        # COVID-era: skip if dominant code is a COVID HCPCS — not suspicious
        if d["dominant_code"] in COVID_HCPCS:
            continue

        severity = "high" if share > 95 and grand_paid > 500000 else "medium"

        excess_share = (share - 85.0) / 100.0
        overpayment = grand_paid * excess_share * 0.25

        signals.append({
            "signal_type": "billing_monoculture",
            "severity": severity,
            "npi": d["npi"],
            "evidence": {
                "dominant_hcpcs_code": d["dominant_code"],
                "dominant_code_share_pct": round(share, 1),
                "dominant_code_claims": int(d["dominant_code_claims"] or 0),
                "dominant_code_paid": round(float(d["dominant_code_paid"] or 0), 2),
                "total_claims_all_codes": int(d["grand_total_claims"] or 0),
                "total_paid_all_codes": round(grand_paid, 2),
                "state": d["state"],
                "taxonomy_code": d["taxonomy_code"],
            },
            "estimated_overpayment_usd": round(overpayment, 2),
        })

    return signals


def signal_billing_bust_out(con: duckdb.DuckDBPyConnection) -> list[dict]:
    """Signal 17: Bust-Out Pattern.

    Detects the full fraud lifecycle: rapid billing escalation followed by
    abrupt cessation (<10% of peak within 3 months). Signal 3 catches the
    ramp; this catches the complete ramp-and-abandon signature.
    """
    query = """
        WITH monthly_ranked AS (
            SELECT
                npi,
                claim_month,
                month_paid,
                month_claims,
                ROW_NUMBER() OVER (PARTITION BY npi ORDER BY month_paid DESC) AS rank_by_paid,
                COUNT(*) OVER (PARTITION BY npi) AS total_months
            FROM provider_monthly
        ),
        peak_months AS (
            SELECT npi, claim_month AS peak_month, month_paid AS peak_paid,
                   month_claims AS peak_claims, total_months
            FROM monthly_ranked
            WHERE rank_by_paid = 1
              AND month_paid > 50000
              AND total_months >= 6
        ),
        post_peak AS (
            SELECT
                pm.npi, pm.peak_month, pm.peak_paid, pm.peak_claims, pm.total_months,
                SUM(mo.month_paid) AS post_3mo_paid,
                COUNT(mo.claim_month) AS post_3mo_months
            FROM peak_months pm
            JOIN provider_monthly mo
                ON pm.npi = mo.npi
                AND mo.claim_month > pm.peak_month
                AND mo.claim_month <= CAST(pm.peak_month AS DATE) + INTERVAL '3 months'
            GROUP BY pm.npi, pm.peak_month, pm.peak_paid, pm.peak_claims, pm.total_months
            HAVING COUNT(mo.claim_month) >= 1
        ),
        flagged AS (
            SELECT *,
                post_3mo_paid / NULLIF(post_3mo_months, 0) AS avg_post_peak_monthly,
                (post_3mo_paid / NULLIF(post_3mo_months, 0))
                    / NULLIF(peak_paid, 0) * 100.0 AS post_peak_pct_of_peak
            FROM post_peak
            WHERE (post_3mo_paid / NULLIF(post_3mo_months, 0))
                  / NULLIF(peak_paid, 0) < 0.10
        ),
        pre_peak AS (
            SELECT
                f.npi, f.peak_month, f.peak_paid, f.peak_claims, f.total_months,
                f.post_3mo_paid, f.post_3mo_months, f.post_peak_pct_of_peak,
                SUM(mo.month_paid) / NULLIF(COUNT(mo.claim_month), 0) AS avg_pre_3mo_paid
            FROM flagged f
            JOIN provider_monthly mo
                ON f.npi = mo.npi
                AND mo.claim_month >= CAST(f.peak_month AS DATE) - INTERVAL '3 months'
                AND mo.claim_month < f.peak_month
            GROUP BY f.npi, f.peak_month, f.peak_paid, f.peak_claims,
                     f.total_months, f.post_3mo_paid, f.post_3mo_months, f.post_peak_pct_of_peak
            HAVING COUNT(mo.claim_month) >= 2
               AND SUM(mo.month_paid) / NULLIF(COUNT(mo.claim_month), 0) < f.peak_paid * 0.5
        )
        SELECT
            pp.npi, pp.peak_month, pp.peak_paid, pp.peak_claims, pp.total_months,
            pp.post_3mo_paid, pp.post_peak_pct_of_peak, pp.avg_pre_3mo_paid,
            COALESCE(n.org_name, n.first_name || ' ' || n.last_name) AS provider_name,
            n.state, n.taxonomy_code, n.entity_type_code
        FROM pre_peak pp
        LEFT JOIN nppes n ON pp.npi = n.npi
        ORDER BY pp.peak_paid DESC
    """
    results = con.execute(query).fetchall()
    columns = [
        "npi", "peak_month", "peak_paid", "peak_claims", "total_months",
        "post_3mo_paid", "post_peak_pct_of_peak", "avg_pre_3mo_paid",
        "provider_name", "state", "taxonomy_code", "entity_type_code",
    ]

    signals = []
    for row in results:
        d = dict(zip(columns, row))
        peak_paid = float(d["peak_paid"] or 0)
        avg_pre = float(d["avg_pre_3mo_paid"] or 0)

        severity = "high" if peak_paid > 500000 else "medium"

        overpayment = (avg_pre * 3 + peak_paid) * 0.4

        peak_month_str = str(d["peak_month"])[:10] if d["peak_month"] else "unknown"
        evidence = {
            "peak_month": peak_month_str,
            "peak_paid": round(peak_paid, 2),
            "peak_claims": int(d["peak_claims"] or 0),
            "avg_pre_3_months_paid": round(avg_pre, 2),
            "post_peak_3_month_paid": round(float(d["post_3mo_paid"] or 0), 2),
            "post_peak_pct_of_peak": round(float(d["post_peak_pct_of_peak"] or 0), 2),
            "total_billing_months": int(d["total_months"] or 0),
            "state": d["state"],
            "taxonomy_code": d["taxonomy_code"],
        }
        # COVID-era: many providers ramped for COVID then legitimately wound down
        if _is_covid_era(peak_month_str):
            severity = "low"
            evidence["covid_era_flag"] = True

        signals.append({
            "signal_type": "billing_bust_out",
            "severity": severity,
            "npi": d["npi"],
            "evidence": evidence,
            "estimated_overpayment_usd": round(overpayment, 2),
        })

    return signals


def signal_reimbursement_rate_anomaly(con: duckdb.DuckDBPyConnection) -> list[dict]:
    """Signal 18: Reimbursement Rate Anomaly.

    Flags providers receiving >3x the national median per-claim payment for
    the same HCPCS code. Catches modifier abuse, place-of-service fraud,
    or billing rate manipulation.
    """
    query = """
        WITH code_rates AS (
            SELECT
                npi, hcpcs_code, total_paid, total_claims,
                total_paid * 1.0 / NULLIF(total_claims, 0) AS avg_rate_per_claim
            FROM provider_code_totals
            WHERE total_claims > 100
        ),
        national_stats AS (
            SELECT
                hcpcs_code,
                PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY avg_rate_per_claim) AS median_rate,
                PERCENTILE_CONT(0.99) WITHIN GROUP (ORDER BY avg_rate_per_claim) AS p99_rate,
                COUNT(*) AS peer_count
            FROM code_rates
            GROUP BY hcpcs_code
            HAVING COUNT(*) >= 10
               AND PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY avg_rate_per_claim) > 0
        )
        SELECT
            cr.npi, cr.hcpcs_code, cr.total_paid, cr.total_claims,
            cr.avg_rate_per_claim, ns.median_rate, ns.p99_rate, ns.peer_count,
            cr.avg_rate_per_claim / NULLIF(ns.median_rate, 0) AS rate_ratio_to_median,
            COALESCE(n.org_name, n.first_name || ' ' || n.last_name) AS provider_name,
            n.state, n.taxonomy_code
        FROM code_rates cr
        JOIN national_stats ns ON cr.hcpcs_code = ns.hcpcs_code
        LEFT JOIN nppes n ON cr.npi = n.npi
        WHERE cr.avg_rate_per_claim > ns.median_rate * 3.0
        ORDER BY rate_ratio_to_median DESC
    """
    results = con.execute(query).fetchall()
    columns = [
        "npi", "hcpcs_code", "total_paid", "total_claims",
        "avg_rate_per_claim", "median_rate", "p99_rate", "peer_count",
        "rate_ratio_to_median", "provider_name", "state", "taxonomy_code",
    ]

    signals = []
    for row in results:
        d = dict(zip(columns, row))
        # COVID-era: skip COVID HCPCS — CMS set unusual rates for these
        if d["hcpcs_code"] in COVID_HCPCS:
            continue

        ratio = float(d["rate_ratio_to_median"] or 0)
        avg_rate = float(d["avg_rate_per_claim"] or 0)
        median_rate = float(d["median_rate"] or 0)
        p99_rate = float(d["p99_rate"] or 0)
        total_claims = int(d["total_claims"] or 0)
        total_paid = float(d["total_paid"] or 0)

        severity = "high" if ratio > 10 or (ratio > 5 and avg_rate > p99_rate) else "medium"

        excess_per_claim = max(0, avg_rate - median_rate)
        overpayment = excess_per_claim * total_claims * 0.7

        signals.append({
            "signal_type": "reimbursement_rate_anomaly",
            "severity": severity,
            "npi": d["npi"],
            "evidence": {
                "hcpcs_code": d["hcpcs_code"],
                "total_claims": total_claims,
                "total_paid": round(total_paid, 2),
                "avg_rate_per_claim": round(avg_rate, 2),
                "national_median_rate": round(median_rate, 2),
                "national_p99_rate": round(p99_rate, 2),
                "peer_count": int(d["peer_count"] or 0),
                "rate_ratio_to_median": round(ratio, 1),
                "state": d["state"],
                "taxonomy_code": d["taxonomy_code"],
            },
            "estimated_overpayment_usd": round(overpayment, 2),
        })

    return signals


def signal_phantom_servicing_spread(con: duckdb.DuckDBPyConnection) -> list[dict]:
    """Signal 19: Phantom Servicing Spread.

    Flags servicing NPIs appearing across 5+ billing entities with impossibly
    low beneficiary-to-claims ratio (claims/bene >100 or ratio below p10 of
    hub peers). Complements Signal 12 which fires on breadth alone.
    """
    query = """
        WITH hub_spread AS (
            SELECT
                servicing_npi,
                COUNT(DISTINCT billing_npi) AS distinct_billing_npis,
                SUM(total_paid) AS total_paid,
                SUM(total_claims) AS total_claims,
                SUM(total_beneficiaries) AS total_beneficiaries,
                SUM(total_beneficiaries) * 1.0 / NULLIF(SUM(total_claims), 0) AS bene_claim_ratio,
                SUM(total_claims) * 1.0 / NULLIF(SUM(total_beneficiaries), 0) AS claims_per_bene
            FROM servicing_hub_totals
            GROUP BY servicing_npi
            HAVING COUNT(DISTINCT billing_npi) >= 5
               AND SUM(total_paid) > 200000
        ),
        baseline AS (
            SELECT
                PERCENTILE_CONT(0.1) WITHIN GROUP (ORDER BY bene_claim_ratio) AS p10_bene_ratio,
                PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY bene_claim_ratio) AS median_bene_ratio
            FROM hub_spread
            WHERE bene_claim_ratio IS NOT NULL AND bene_claim_ratio > 0
        )
        SELECT
            hs.servicing_npi,
            hs.distinct_billing_npis,
            hs.total_paid,
            hs.total_claims,
            hs.total_beneficiaries,
            hs.bene_claim_ratio,
            hs.claims_per_bene,
            bl.p10_bene_ratio,
            bl.median_bene_ratio,
            COALESCE(n.org_name, n.first_name || ' ' || n.last_name) AS provider_name,
            n.taxonomy_code,
            n.state
        FROM hub_spread hs
        CROSS JOIN baseline bl
        LEFT JOIN nppes n ON hs.servicing_npi = n.npi
        WHERE hs.claims_per_bene > 100
           OR (bl.p10_bene_ratio IS NOT NULL AND hs.bene_claim_ratio < bl.p10_bene_ratio)
        ORDER BY hs.total_paid DESC
    """
    results = con.execute(query).fetchall()
    columns = [
        "servicing_npi", "distinct_billing_npis", "total_paid", "total_claims",
        "total_beneficiaries", "bene_claim_ratio", "claims_per_bene",
        "p10_bene_ratio", "median_bene_ratio", "provider_name",
        "taxonomy_code", "state",
    ]

    signals = []
    for row in results:
        d = dict(zip(columns, row))
        claims_per_bene = float(d["claims_per_bene"] or 0)
        total_paid = float(d["total_paid"] or 0)
        total_claims = int(d["total_claims"] or 0)
        total_benes = int(d["total_beneficiaries"] or 0)
        p10_ratio = float(d["p10_bene_ratio"] or 0) if d["p10_bene_ratio"] else 0

        severity = "high" if claims_per_bene > 200 else "medium"

        if total_benes > 0 and p10_ratio > 0:
            expected_claims = total_benes * (1.0 / p10_ratio)
            excess_claims = max(0, total_claims - expected_claims)
            cost_per_claim = total_paid / max(total_claims, 1)
            overpayment = excess_claims * cost_per_claim * 0.65
        else:
            overpayment = total_paid * 0.7

        # Get billing NPI list
        billing_list = con.execute("""
            SELECT LIST(DISTINCT billing_npi)
            FROM servicing_hub_totals
            WHERE servicing_npi = ?
        """, [d["servicing_npi"]]).fetchone()[0] or []

        signals.append({
            "signal_type": "phantom_servicing_spread",
            "severity": severity,
            "npi": d["servicing_npi"],
            "evidence": {
                "servicing_provider_name": d["provider_name"],
                "distinct_billing_npis": int(d["distinct_billing_npis"] or 0),
                "billing_npi_list": billing_list[:20],
                "total_paid": round(total_paid, 2),
                "total_claims": total_claims,
                "total_beneficiaries": total_benes,
                "bene_claim_ratio": round(float(d["bene_claim_ratio"] or 0), 4),
                "claims_per_beneficiary": round(claims_per_bene, 1),
                "p10_bene_ratio_baseline": round(p10_ratio, 4),
                "taxonomy_code": d["taxonomy_code"],
                "state": d["state"],
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
    """Run all 19 signals and return results grouped by type."""
    print("\n[1/19] Signal: Excluded Provider Still Billing...")
    excluded = signal_excluded_provider(con)
    print(f"  Found {len(excluded)} flags")

    print("[2/19] Signal: Billing Volume Outlier...")
    outlier = signal_billing_outlier(con)
    print(f"  Found {len(outlier)} flags")

    print("[3/19] Signal: Rapid Billing Escalation...")
    escalation = signal_rapid_escalation(con)
    print(f"  Found {len(escalation)} flags")

    print("[4/19] Signal: Workforce Impossibility...")
    workforce = signal_workforce_impossibility(con)
    print(f"  Found {len(workforce)} flags")

    print("[5/19] Signal: Shared Authorized Official...")
    official = signal_shared_official(con)
    print(f"  Found {len(official)} flags")

    print("[6/19] Signal: Geographic Implausibility...")
    geo = signal_geographic_implausibility(con)
    print(f"  Found {len(geo)} flags")

    print("[7/19] Signal: Address Clustering...")
    clustering = signal_address_clustering(con)
    print(f"  Found {len(clustering)} flags")

    print("[8/19] Signal: Upcoding Detection...")
    upcoding = signal_upcoding(con)
    print(f"  Found {len(upcoding)} flags")

    print("[9/19] Signal: Concurrent Billing Across States...")
    concurrent = signal_concurrent_billing(con)
    print(f"  Found {len(concurrent)} flags")

    print("[10/19] Signal: Burst Enrollment Network...")
    burst = signal_burst_enrollment_network(con)
    print(f"  Found {len(burst)} flags")

    print("[11/19] Signal: Coordinated Billing Ramp...")
    ramp = signal_coordinated_billing_ramp(con)
    print(f"  Found {len(ramp)} flags")

    print("[12/19] Signal: Phantom Servicing Hub...")
    phantom = signal_phantom_servicing_hub(con)
    print(f"  Found {len(phantom)} flags")

    print("[13/19] Signal: Network Beneficiary Dilution...")
    dilution = signal_network_beneficiary_dilution(con)
    print(f"  Found {len(dilution)} flags")

    print("[14/19] Signal: Family Caregiver Density Anomaly...")
    caregiver = signal_caregiver_density_anomaly(con)
    print(f"  Found {len(caregiver)} flags")

    print("[15/19] Signal: Repetitive Service Abuse...")
    repetitive = signal_repetitive_service_abuse(con)
    print(f"  Found {len(repetitive)} flags")

    print("[16/19] Signal: Billing Monoculture...")
    monoculture = signal_billing_monoculture(con)
    print(f"  Found {len(monoculture)} flags")

    print("[17/19] Signal: Billing Bust-Out...")
    bust_out = signal_billing_bust_out(con)
    print(f"  Found {len(bust_out)} flags")

    print("[18/19] Signal: Reimbursement Rate Anomaly...")
    rate_anomaly = signal_reimbursement_rate_anomaly(con)
    print(f"  Found {len(rate_anomaly)} flags")

    print("[19/19] Signal: Phantom Servicing Spread...")
    spread = signal_phantom_servicing_spread(con)
    print(f"  Found {len(spread)} flags")

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
        "burst_enrollment_network": burst,
        "coordinated_billing_ramp": ramp,
        "phantom_servicing_hub": phantom,
        "network_beneficiary_dilution": dilution,
        "caregiver_density_anomaly": caregiver,
        "repetitive_service_abuse": repetitive,
        "billing_monoculture": monoculture,
        "billing_bust_out": bust_out,
        "reimbursement_rate_anomaly": rate_anomaly,
        "phantom_servicing_spread": spread,
    }
