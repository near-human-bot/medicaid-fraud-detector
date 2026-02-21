"""Report generation module — JSON and HTML output with risk scoring."""

import json
import html as html_lib
from datetime import datetime, timezone

VERSION = "2.0.0"

# Statute reference mapping per spec
STATUTE_MAP = {
    "excluded_provider": "31 U.S.C. section 3729(a)(1)(A)",
    "billing_outlier": "31 U.S.C. section 3729(a)(1)(A)",
    "rapid_escalation": "31 U.S.C. section 3729(a)(1)(A)",
    "workforce_impossibility": "31 U.S.C. section 3729(a)(1)(B)",
    "shared_official": "31 U.S.C. section 3729(a)(1)(C)",
    "geographic_implausibility": "31 U.S.C. section 3729(a)(1)(G)",
    "address_clustering": "31 U.S.C. section 3729(a)(1)(C)",
    "upcoding": "31 U.S.C. section 3729(a)(1)(A)",
    "concurrent_billing": "31 U.S.C. section 3729(a)(1)(B)",
}

# Claim type descriptions
CLAIM_TYPE_MAP = {
    "excluded_provider": "Presenting false claims — excluded provider cannot legally bill federal healthcare programs",
    "billing_outlier": "Potential overbilling — provider billing significantly exceeds peer group norms",
    "rapid_escalation": "Potential bust-out scheme — newly enumerated provider with rapid billing escalation",
    "workforce_impossibility": "False records — billing volume implies physically impossible claim fabrication",
    "shared_official": "Conspiracy — coordinated billing through multiple entities controlled by same individual",
    "geographic_implausibility": "Reverse false claims — repeated billing on same patients suggests fabricated home health services",
    "address_clustering": "Potential ghost office — unusually high concentration of billing providers at single address",
    "upcoding": "Systematic upcoding — provider consistently bills highest-complexity codes far exceeding peer norms",
    "concurrent_billing": "Phantom billing — individual provider billing across multiple distant states simultaneously",
}

# Next steps templates
NEXT_STEPS_MAP = {
    "excluded_provider": [
        "Verify provider exclusion status on OIG LEIE database and confirm dates",
        "Request itemized claims data from state Medicaid agency for post-exclusion period",
        "Determine which managed care organizations processed claims for this excluded provider",
    ],
    "billing_outlier": [
        "Request detailed claims data and compare procedure code distribution to peer group",
        "Verify provider is actively practicing at registered address through site visit or public records",
        "Cross-reference with patient records to verify services were actually rendered",
    ],
    "rapid_escalation": [
        "Investigate provider ownership changes around enumeration date",
        "Request detailed claims data for first 12 months of billing activity",
        "Check if provider entity was previously associated with excluded individuals",
    ],
    "workforce_impossibility": [
        "Request employment records showing number of licensed practitioners at this entity",
        "Compare staffing levels to claims volume to determine if services could have been physically rendered",
        "Review claims for patterns of identical procedures billed on same dates",
    ],
    "shared_official": [
        "Investigate corporate structure and beneficial ownership of all entities controlled by this individual",
        "Check for cross-referrals between the controlled entities suggesting kickback arrangements",
        "Review claims for overlapping patients across entities that would indicate coordinated billing",
    ],
    "geographic_implausibility": [
        "Verify patient addresses to confirm home health services were geographically feasible",
        "Request patient visit logs and compare to billed service dates",
        "Cross-reference with other payers to check for duplicate billing of same home health services",
    ],
    "address_clustering": [
        "Conduct site visit to verify each provider at the registered address has a physical office",
        "Check for shared phone numbers, fax numbers, or billing contacts across the clustered NPIs",
        "Review corporate filings to identify common ownership among the clustered entities",
    ],
    "upcoding": [
        "Request medical records for a sample of high-complexity claims and verify documentation supports the billed level",
        "Compare procedure code distribution month-over-month for sudden shifts to higher codes",
        "Interview billing staff to determine if coding education or software changes drove the pattern",
    ],
    "concurrent_billing": [
        "Verify provider travel records or telehealth documentation for multi-state claims",
        "Check if the NPI has been compromised or used without authorization in other states",
        "Request claims detail to determine if services were in-person or could legitimately be remote",
    ],
}

# Severity weight for composite risk scoring
SEVERITY_WEIGHTS = {
    "critical": 10.0,
    "high": 7.0,
    "medium": 4.0,
    "low": 1.0,
}

# Signal-type inherent risk weight
SIGNAL_RISK_WEIGHTS = {
    "excluded_provider": 10.0,
    "workforce_impossibility": 8.0,
    "upcoding": 7.0,
    "rapid_escalation": 7.0,
    "concurrent_billing": 6.0,
    "billing_outlier": 5.0,
    "shared_official": 5.0,
    "address_clustering": 4.0,
    "geographic_implausibility": 4.0,
}


def compute_risk_score(signals: list[dict], total_paid: float) -> dict:
    """Compute a composite risk score (0-100) from a provider's signals.

    Factors:
    - Number of distinct signal types (breadth)
    - Severity of each signal (weight)
    - Signal-type inherent risk
    - Estimated overpayment relative to total billing
    """
    if not signals:
        return {"score": 0, "tier": "low", "factors": []}

    factors = []

    # Factor 1: Signal breadth (more signal types = higher risk)
    signal_types = set(s["signal_type"] for s in signals)
    breadth_score = min(len(signal_types) * 12, 30)  # max 30 points
    factors.append({"name": "signal_breadth", "value": len(signal_types), "points": breadth_score})

    # Factor 2: Severity-weighted signal score
    severity_sum = sum(
        SEVERITY_WEIGHTS.get(s["severity"], 1) * SIGNAL_RISK_WEIGHTS.get(s["signal_type"], 3)
        for s in signals
    )
    severity_score = min(severity_sum / 2, 40)  # max 40 points
    factors.append({"name": "severity_weight", "value": round(severity_sum, 1), "points": round(severity_score, 1)})

    # Factor 3: Overpayment ratio
    total_overpayment = sum(s.get("estimated_overpayment_usd", 0) for s in signals)
    if total_paid > 0 and total_overpayment > 0:
        ratio = total_overpayment / total_paid
        overpay_score = min(ratio * 100, 30)  # max 30 points
    else:
        overpay_score = 0
    factors.append({"name": "overpayment_ratio", "value": round(total_overpayment, 2), "points": round(overpay_score, 1)})

    score = round(min(breadth_score + severity_score + overpay_score, 100), 1)

    if score >= 75:
        tier = "critical"
    elif score >= 50:
        tier = "high"
    elif score >= 25:
        tier = "medium"
    else:
        tier = "low"

    return {"score": score, "tier": tier, "factors": factors}


def generate_case_narrative(provider: dict) -> str:
    """Generate a plain-English case narrative for a flagged provider."""
    name = provider["provider_name"]
    npi = provider["npi"]
    entity = provider["entity_type"]
    state = provider["state"]
    total_paid = provider["total_paid_all_time"]
    signals = provider["signals"]
    overpayment = provider["estimated_overpayment_usd"]

    parts = []
    parts.append(
        f"{name} (NPI: {npi}) is a Medicaid-enrolled {entity} provider "
        f"based in {state} with ${total_paid:,.2f} in total billing."
    )

    signal_descriptions = []
    for sig in signals:
        stype = sig["signal_type"]
        sev = sig["severity"]
        ev = sig["evidence"]

        if stype == "excluded_provider":
            signal_descriptions.append(
                f"This provider appears on the OIG exclusion list (excluded {ev.get('exclusion_date', 'unknown date')}) "
                f"yet continued billing Medicaid for {ev.get('total_claims_after_exclusion', 0):,} claims "
                f"totaling ${ev.get('total_paid_after_exclusion', 0):,.2f}."
            )
        elif stype == "billing_outlier":
            signal_descriptions.append(
                f"Their billing of ${ev.get('total_paid', 0):,.2f} is {ev.get('ratio_to_median', 0):.1f}x "
                f"the median for their specialty ({ev.get('taxonomy_code', 'unknown')}) in {ev.get('state', 'their state')}, "
                f"exceeding the 99th percentile of ${ev.get('peer_99th_percentile', 0):,.2f}."
            )
        elif stype == "rapid_escalation":
            signal_descriptions.append(
                f"As a newly enumerated provider (since {ev.get('enumeration_date', 'unknown')}), "
                f"their billing escalated at a peak 3-month growth rate of {ev.get('peak_3_month_growth_rate', 0):.0f}%, "
                f"far exceeding the 200% threshold for bust-out schemes."
            )
        elif stype == "workforce_impossibility":
            signal_descriptions.append(
                f"In their peak month ({ev.get('peak_month', 'unknown')}), this organization billed "
                f"{ev.get('peak_claims_count', 0):,} claims, implying {ev.get('implied_claims_per_hour', 0):.1f} claims "
                f"per hour — a physically impossible volume for any healthcare practice."
            )
        elif stype == "shared_official":
            signal_descriptions.append(
                f"The authorized official ({ev.get('authorized_official_name', 'unknown')}) controls "
                f"{ev.get('npi_count', 0)} NPIs with combined billing of ${ev.get('combined_total_paid', 0):,.2f}, "
                f"suggesting a coordinated billing network."
            )
        elif stype == "geographic_implausibility":
            signal_descriptions.append(
                f"Home health billing shows a beneficiary-to-claims ratio of {ev.get('beneficiary_claims_ratio', 0):.4f} "
                f"({ev.get('unique_beneficiaries', 0)} beneficiaries for {ev.get('claims_count', 0):,} claims), "
                f"suggesting fabricated services."
            )
        elif stype == "address_clustering":
            signal_descriptions.append(
                f"This provider is part of a cluster of {ev.get('npi_count', 0)} NPIs registered at "
                f"zip code {ev.get('zip_code', 'unknown')} with combined billing of "
                f"${ev.get('combined_total_paid', 0):,.2f}, indicating a potential ghost office operation."
            )
        elif stype == "upcoding":
            signal_descriptions.append(
                f"This provider bills high-complexity E&M codes {ev.get('high_level_percentage', 0):.1f}% of the time, "
                f"compared to a peer average of {ev.get('peer_avg_high_level_percentage', 0):.1f}% — "
                f"a pattern consistent with systematic upcoding."
            )
        elif stype == "concurrent_billing":
            signal_descriptions.append(
                f"This individual provider billed in {ev.get('max_states_in_single_month', 0)} different states "
                f"within a single month, which is physically impossible without telehealth or identity theft."
            )

    if signal_descriptions:
        parts.append(" ".join(signal_descriptions))

    if overpayment > 0:
        parts.append(f"Estimated total overpayment: ${overpayment:,.2f}.")

    risk = provider.get("risk_score", {})
    if risk:
        parts.append(
            f"Composite risk score: {risk.get('score', 0)}/100 ({risk.get('tier', 'unknown')} risk)."
        )

    return " ".join(parts)


def build_provider_record(npi: str, signals: list[dict], con) -> dict:
    """Build a complete provider record from their signals."""
    # Get provider info from NPPES
    try:
        provider_info = con.execute("""
            SELECT
                npi,
                COALESCE(org_name, first_name || ' ' || last_name) AS provider_name,
                CASE WHEN entity_type_code = '1' THEN 'individual' ELSE 'organization' END AS entity_type,
                taxonomy_code,
                state,
                enumeration_date
            FROM nppes
            WHERE npi = ?
            LIMIT 1
        """, [npi]).fetchone()
    except Exception:
        provider_info = None

    if provider_info:
        provider_name = provider_info[1] or "Unknown"
        entity_type = provider_info[2] or "unknown"
        taxonomy_code = provider_info[3] or "Unknown"
        state = provider_info[4] or "Unknown"
        enum_date = str(provider_info[5]) if provider_info[5] else "Unknown"
    else:
        provider_name = "Unknown"
        entity_type = "unknown"
        taxonomy_code = "Unknown"
        state = "Unknown"
        enum_date = "Unknown"

    # Override with signal-specific info if available
    for sig in signals:
        ev = sig.get("evidence", {})
        if "state" in ev and ev["state"]:
            state = ev["state"]
        if "taxonomy_code" in ev and ev["taxonomy_code"]:
            taxonomy_code = ev["taxonomy_code"]

    # Compute totals from spending
    try:
        totals = con.execute("""
            SELECT
                SUM(total_paid) AS total_paid,
                SUM(total_claims) AS total_claims,
                SUM(unique_beneficiaries) AS total_beneficiaries
            FROM spending
            WHERE billing_npi = ?
        """, [npi]).fetchone()
        total_paid = float(totals[0] or 0)
        total_claims = int(totals[1] or 0)
        total_beneficiaries = int(totals[2] or 0)
    except Exception:
        total_paid = 0.0
        total_claims = 0
        total_beneficiaries = 0

    # Build signal list
    signal_records = []
    total_overpayment = 0.0
    for sig in signals:
        sig_type = sig["signal_type"]
        overpayment = sig.get("estimated_overpayment_usd", 0.0)
        total_overpayment += overpayment
        signal_records.append({
            "signal_type": sig_type,
            "severity": sig["severity"],
            "evidence": sig["evidence"],
        })

    # Compute composite risk score
    risk = compute_risk_score(signals, total_paid)

    record = {
        "npi": npi,
        "provider_name": provider_name,
        "entity_type": entity_type,
        "taxonomy_code": taxonomy_code,
        "state": state,
        "enumeration_date": enum_date,
        "total_paid_all_time": round(total_paid, 2),
        "total_claims_all_time": total_claims,
        "total_unique_beneficiaries_all_time": total_beneficiaries,
        "signals": signal_records,
        "estimated_overpayment_usd": round(total_overpayment, 2),
        "risk_score": risk,
        "fca_relevance": {
            "claim_type": CLAIM_TYPE_MAP.get(signals[0]["signal_type"], "Unknown violation pattern"),
            "statute_reference": STATUTE_MAP.get(signals[0]["signal_type"], "31 U.S.C. section 3729"),
            "suggested_next_steps": NEXT_STEPS_MAP.get(signals[0]["signal_type"], [
                "Request detailed claims data from state Medicaid agency",
                "Verify provider information through public records",
            ]),
        },
    }

    # Generate plain-English case narrative
    record["case_narrative"] = generate_case_narrative(record)

    return record


def generate_executive_summary(report: dict) -> dict:
    """Generate an executive summary section for the report."""
    providers = report["flagged_providers"]
    signal_counts = report["signal_counts"]

    total_overpayment = sum(p["estimated_overpayment_usd"] for p in providers)

    # Count by risk tier
    tier_counts = {"critical": 0, "high": 0, "medium": 0, "low": 0}
    for p in providers:
        tier = p.get("risk_score", {}).get("tier", "low")
        tier_counts[tier] = tier_counts.get(tier, 0) + 1

    # Top states by flagged providers
    state_counts: dict[str, int] = {}
    for p in providers:
        st = p.get("state", "Unknown")
        state_counts[st] = state_counts.get(st, 0) + 1
    top_states = sorted(state_counts.items(), key=lambda x: x[1], reverse=True)[:5]

    # Top signal types by count
    top_signals = sorted(signal_counts.items(), key=lambda x: x[1], reverse=True)

    # Highest-risk providers
    top_risk = sorted(providers, key=lambda p: p.get("risk_score", {}).get("score", 0), reverse=True)[:5]

    return {
        "total_providers_scanned": report["total_providers_scanned"],
        "total_providers_flagged": report["total_providers_flagged"],
        "total_estimated_overpayment_usd": round(total_overpayment, 2),
        "risk_tier_distribution": tier_counts,
        "top_states_by_flags": [{"state": s, "count": c} for s, c in top_states],
        "signal_type_summary": [{"signal": s, "count": c} for s, c in top_signals],
        "highest_risk_providers": [
            {
                "npi": p["npi"],
                "provider_name": p["provider_name"],
                "risk_score": p.get("risk_score", {}).get("score", 0),
                "risk_tier": p.get("risk_score", {}).get("tier", "unknown"),
                "signal_count": len(p["signals"]),
                "estimated_overpayment_usd": p["estimated_overpayment_usd"],
            }
            for p in top_risk
        ],
    }


def generate_report(signal_results: dict, con, total_providers_scanned: int) -> dict:
    """Generate the final fraud_signals.json report."""
    # Group signals by NPI
    npi_signals: dict[str, list[dict]] = {}
    signal_counts = {}

    for signal_type, signals in signal_results.items():
        signal_counts[signal_type] = len(signals)
        for sig in signals:
            npi = sig["npi"]
            if npi not in npi_signals:
                npi_signals[npi] = []
            npi_signals[npi].append(sig)

    # Build provider records
    flagged_providers = []
    for npi, signals in npi_signals.items():
        provider = build_provider_record(npi, signals, con)
        flagged_providers.append(provider)

    # Sort by risk score descending (then overpayment as tiebreaker)
    flagged_providers.sort(
        key=lambda p: (p.get("risk_score", {}).get("score", 0), p["estimated_overpayment_usd"]),
        reverse=True,
    )

    report = {
        "generated_at": datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ"),
        "tool_version": VERSION,
        "total_providers_scanned": total_providers_scanned,
        "total_providers_flagged": len(flagged_providers),
        "signal_counts": signal_counts,
        "flagged_providers": flagged_providers,
    }

    # Add executive summary
    report["executive_summary"] = generate_executive_summary(report)

    return report


def write_report(report: dict, output_path: str) -> None:
    """Write the report to a JSON file."""
    with open(output_path, "w") as f:
        json.dump(report, f, indent=2, default=str)
    print(f"\nReport written to: {output_path}")
    print(f"  Providers scanned: {report['total_providers_scanned']:,}")
    print(f"  Providers flagged: {report['total_providers_flagged']:,}")
    for sig_type, count in report['signal_counts'].items():
        print(f"  {sig_type}: {count}")

    summary = report.get("executive_summary", {})
    if summary:
        print(f"\n  Total estimated overpayment: ${summary.get('total_estimated_overpayment_usd', 0):,.2f}")
        tiers = summary.get("risk_tier_distribution", {})
        print(f"  Risk tiers: {tiers.get('critical', 0)} critical, {tiers.get('high', 0)} high, "
              f"{tiers.get('medium', 0)} medium, {tiers.get('low', 0)} low")


def _esc(text: str) -> str:
    """HTML-escape a string."""
    return html_lib.escape(str(text))


def write_html_report(report: dict, output_path: str) -> None:
    """Write an HTML version of the report."""
    summary = report.get("executive_summary", {})
    providers = report.get("flagged_providers", [])

    tier_colors = {"critical": "#dc2626", "high": "#ea580c", "medium": "#ca8a04", "low": "#16a34a"}

    lines = []
    lines.append("<!DOCTYPE html>")
    lines.append('<html lang="en"><head><meta charset="UTF-8">')
    lines.append('<meta name="viewport" content="width=device-width,initial-scale=1">')
    lines.append(f"<title>Medicaid Fraud Signal Report — {_esc(report.get('generated_at', ''))}</title>")
    lines.append("<style>")
    lines.append("""
body { font-family: -apple-system, BlinkMacSystemFont, 'Segoe UI', Roboto, sans-serif;
       max-width: 1200px; margin: 0 auto; padding: 20px; background: #f8fafc; color: #1e293b; }
h1 { color: #0f172a; border-bottom: 3px solid #2563eb; padding-bottom: 10px; }
h2 { color: #1e40af; margin-top: 30px; }
h3 { color: #334155; }
.summary-grid { display: grid; grid-template-columns: repeat(auto-fit, minmax(200px, 1fr)); gap: 16px; margin: 20px 0; }
.summary-card { background: white; border-radius: 8px; padding: 20px; box-shadow: 0 1px 3px rgba(0,0,0,0.1); }
.summary-card .label { font-size: 0.85em; color: #64748b; text-transform: uppercase; letter-spacing: 0.05em; }
.summary-card .value { font-size: 1.8em; font-weight: 700; color: #0f172a; margin-top: 4px; }
table { width: 100%; border-collapse: collapse; margin: 16px 0; background: white; border-radius: 8px; overflow: hidden;
        box-shadow: 0 1px 3px rgba(0,0,0,0.1); }
th { background: #1e40af; color: white; padding: 12px; text-align: left; font-size: 0.85em; text-transform: uppercase; }
td { padding: 10px 12px; border-bottom: 1px solid #e2e8f0; font-size: 0.9em; }
tr:hover { background: #f1f5f9; }
.tier-badge { display: inline-block; padding: 2px 10px; border-radius: 12px; color: white;
              font-size: 0.8em; font-weight: 600; text-transform: uppercase; }
.provider-card { background: white; border-radius: 8px; padding: 20px; margin: 16px 0;
                 box-shadow: 0 1px 3px rgba(0,0,0,0.1); border-left: 4px solid #2563eb; }
.provider-card.critical { border-left-color: #dc2626; }
.provider-card.high { border-left-color: #ea580c; }
.provider-card.medium { border-left-color: #ca8a04; }
.narrative { background: #fffbeb; border: 1px solid #fde68a; border-radius: 6px; padding: 14px; margin: 10px 0;
             font-style: italic; line-height: 1.6; }
.signal-tag { display: inline-block; padding: 2px 8px; border-radius: 4px; margin: 2px;
              font-size: 0.8em; background: #dbeafe; color: #1e40af; }
.score-bar { height: 8px; border-radius: 4px; background: #e2e8f0; margin-top: 6px; }
.score-fill { height: 100%; border-radius: 4px; }
footer { margin-top: 40px; padding-top: 20px; border-top: 1px solid #e2e8f0; color: #94a3b8; font-size: 0.85em; }
""")
    lines.append("</style></head><body>")

    # Header
    lines.append(f"<h1>Medicaid Fraud Signal Detection Report</h1>")
    lines.append(f"<p>Generated: {_esc(report.get('generated_at', 'Unknown'))} | "
                 f"Tool Version: {_esc(report.get('tool_version', ''))}</p>")

    # Executive Summary
    lines.append("<h2>Executive Summary</h2>")
    lines.append('<div class="summary-grid">')
    lines.append(f'<div class="summary-card"><div class="label">Providers Scanned</div>'
                 f'<div class="value">{summary.get("total_providers_scanned", 0):,}</div></div>')
    lines.append(f'<div class="summary-card"><div class="label">Providers Flagged</div>'
                 f'<div class="value">{summary.get("total_providers_flagged", 0):,}</div></div>')
    lines.append(f'<div class="summary-card"><div class="label">Est. Overpayment</div>'
                 f'<div class="value">${summary.get("total_estimated_overpayment_usd", 0):,.0f}</div></div>')
    critical_count = summary.get("risk_tier_distribution", {}).get("critical", 0)
    lines.append(f'<div class="summary-card"><div class="label">Critical Risk</div>'
                 f'<div class="value" style="color:#dc2626">{critical_count}</div></div>')
    lines.append('</div>')

    # Risk tier distribution
    tiers = summary.get("risk_tier_distribution", {})
    lines.append("<h3>Risk Tier Distribution</h3>")
    lines.append("<table><tr><th>Tier</th><th>Count</th></tr>")
    for tier in ["critical", "high", "medium", "low"]:
        color = tier_colors.get(tier, "#64748b")
        count = tiers.get(tier, 0)
        lines.append(f'<tr><td><span class="tier-badge" style="background:{color}">{_esc(tier)}</span></td>'
                     f"<td>{count}</td></tr>")
    lines.append("</table>")

    # Signal type summary
    lines.append("<h3>Signal Type Summary</h3>")
    lines.append("<table><tr><th>Signal</th><th>Flags</th></tr>")
    for item in summary.get("signal_type_summary", []):
        lines.append(f"<tr><td>{_esc(item['signal'])}</td><td>{item['count']}</td></tr>")
    lines.append("</table>")

    # Top states
    top_states = summary.get("top_states_by_flags", [])
    if top_states:
        lines.append("<h3>Top States by Flags</h3>")
        lines.append("<table><tr><th>State</th><th>Flagged Providers</th></tr>")
        for item in top_states:
            lines.append(f"<tr><td>{_esc(item['state'])}</td><td>{item['count']}</td></tr>")
        lines.append("</table>")

    # Highest-risk providers table
    lines.append("<h3>Highest-Risk Providers</h3>")
    lines.append("<table><tr><th>NPI</th><th>Name</th><th>Score</th><th>Tier</th>"
                 "<th>Signals</th><th>Est. Overpayment</th></tr>")
    for p in summary.get("highest_risk_providers", []):
        tier = p.get("risk_tier", "low")
        color = tier_colors.get(tier, "#64748b")
        lines.append(
            f'<tr><td>{_esc(p["npi"])}</td><td>{_esc(p["provider_name"])}</td>'
            f'<td>{p["risk_score"]}</td>'
            f'<td><span class="tier-badge" style="background:{color}">{_esc(tier)}</span></td>'
            f'<td>{p["signal_count"]}</td>'
            f'<td>${p["estimated_overpayment_usd"]:,.2f}</td></tr>'
        )
    lines.append("</table>")

    # Provider details
    lines.append("<h2>Flagged Provider Details</h2>")
    for p in providers[:50]:  # Limit to top 50 in HTML
        tier = p.get("risk_score", {}).get("tier", "low")
        score = p.get("risk_score", {}).get("score", 0)
        color = tier_colors.get(tier, "#64748b")

        lines.append(f'<div class="provider-card {_esc(tier)}">')
        lines.append(f'<h3>{_esc(p["provider_name"])} — NPI: {_esc(p["npi"])}</h3>')
        lines.append(f'<p><strong>State:</strong> {_esc(p["state"])} | '
                     f'<strong>Type:</strong> {_esc(p["entity_type"])} | '
                     f'<strong>Taxonomy:</strong> {_esc(p["taxonomy_code"])} | '
                     f'<strong>Total Billing:</strong> ${p["total_paid_all_time"]:,.2f}</p>')

        # Risk score bar
        lines.append(f'<p><strong>Risk Score:</strong> {score}/100 '
                     f'<span class="tier-badge" style="background:{color}">{_esc(tier)}</span></p>')
        lines.append(f'<div class="score-bar"><div class="score-fill" '
                     f'style="width:{score}%;background:{color}"></div></div>')

        # Signals
        lines.append("<p><strong>Signals:</strong> ")
        for sig in p["signals"]:
            lines.append(f'<span class="signal-tag">{_esc(sig["signal_type"])} ({_esc(sig["severity"])})</span>')
        lines.append("</p>")

        # Overpayment
        lines.append(f'<p><strong>Estimated Overpayment:</strong> ${p["estimated_overpayment_usd"]:,.2f}</p>')

        # Case narrative
        narrative = p.get("case_narrative", "")
        if narrative:
            lines.append(f'<div class="narrative">{_esc(narrative)}</div>')

        # FCA relevance
        fca = p.get("fca_relevance", {})
        if fca:
            lines.append(f'<p><strong>FCA Claim Type:</strong> {_esc(fca.get("claim_type", ""))}</p>')
            lines.append(f'<p><strong>Statute:</strong> {_esc(fca.get("statute_reference", ""))}</p>')
            steps = fca.get("suggested_next_steps", [])
            if steps:
                lines.append("<p><strong>Next Steps:</strong></p><ol>")
                for step in steps:
                    lines.append(f"<li>{_esc(step)}</li>")
                lines.append("</ol>")

        lines.append("</div>")

    if len(providers) > 50:
        lines.append(f"<p><em>Showing top 50 of {len(providers)} flagged providers. "
                     f"See JSON report for complete data.</em></p>")

    # Footer
    lines.append("<footer>")
    lines.append(f"<p>Medicaid Fraud Signal Detection Engine v{_esc(report.get('tool_version', ''))} | "
                 f"Report generated {_esc(report.get('generated_at', ''))} | "
                 f"Data sources: HHS STOP Medicaid Spending, OIG LEIE, CMS NPPES</p>")
    lines.append("</footer></body></html>")

    html_content = "\n".join(lines)
    with open(output_path, "w") as f:
        f.write(html_content)
    print(f"HTML report written to: {output_path}")
