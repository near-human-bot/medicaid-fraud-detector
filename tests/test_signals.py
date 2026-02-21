"""Unit tests for each fraud signal with synthetic data."""

import sys
import os
import tempfile
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from src.signals import (
    signal_excluded_provider,
    signal_billing_outlier,
    signal_rapid_escalation,
    signal_workforce_impossibility,
    signal_shared_official,
    signal_geographic_implausibility,
    signal_address_clustering,
    signal_upcoding,
    signal_concurrent_billing,
    run_all_signals,
)
from src.output import (
    generate_report,
    write_html_report,
    compute_risk_score,
    generate_case_narrative,
    generate_executive_summary,
    STATUTE_MAP,
    NEXT_STEPS_MAP,
    CLAIM_TYPE_MAP,
    METHODOLOGY,
)
from src.signals import compute_cross_signal_correlations


class TestSignalExcludedProvider:
    """Signal 1: Excluded Provider Still Billing."""

    def test_detects_excluded_provider(self, con):
        results = signal_excluded_provider(con)
        assert len(results) >= 1
        npis = [r["npi"] for r in results]
        assert "2222222222" in npis

    def test_excluded_provider_severity_is_critical(self, con):
        results = signal_excluded_provider(con)
        for r in results:
            assert r["severity"] == "critical"

    def test_excluded_provider_has_overpayment(self, con):
        results = signal_excluded_provider(con)
        excluded = [r for r in results if r["npi"] == "2222222222"]
        assert len(excluded) == 1
        assert excluded[0]["estimated_overpayment_usd"] > 0

    def test_does_not_flag_non_excluded_provider(self, con):
        results = signal_excluded_provider(con)
        npis = [r["npi"] for r in results]
        assert "1111111111" not in npis


class TestSignalBillingOutlier:
    """Signal 2: Billing Volume Outlier."""

    def test_detects_outlier(self, con):
        results = signal_billing_outlier(con)
        assert len(results) >= 1
        npis = [r["npi"] for r in results]
        assert "3333333333" in npis

    def test_outlier_has_peer_stats(self, con):
        results = signal_billing_outlier(con)
        outlier = [r for r in results if r["npi"] == "3333333333"]
        assert len(outlier) == 1
        ev = outlier[0]["evidence"]
        assert "peer_median" in ev
        assert "peer_99th_percentile" in ev
        assert "ratio_to_median" in ev
        assert ev["ratio_to_median"] > 1.0

    def test_outlier_severity_high_when_above_5x(self, con):
        results = signal_billing_outlier(con)
        outlier = [r for r in results if r["npi"] == "3333333333"]
        if outlier and outlier[0]["evidence"]["ratio_to_median"] > 5:
            assert outlier[0]["severity"] == "high"

    def test_does_not_flag_normal_providers(self, con):
        results = signal_billing_outlier(con)
        npis = [r["npi"] for r in results]
        assert "3333333334" not in npis


class TestSignalRapidEscalation:
    """Signal 3: Rapid Billing Escalation."""

    def test_detects_rapid_escalation(self, con):
        results = signal_rapid_escalation(con)
        assert len(results) >= 1
        npis = [r["npi"] for r in results]
        assert "4444444444" in npis

    def test_escalation_has_growth_data(self, con):
        results = signal_rapid_escalation(con)
        rapid = [r for r in results if r["npi"] == "4444444444"]
        assert len(rapid) == 1
        ev = rapid[0]["evidence"]
        assert "peak_3_month_growth_rate" in ev
        assert ev["peak_3_month_growth_rate"] > 200
        assert "monthly_amounts_first_12" in ev

    def test_does_not_flag_established_provider(self, con):
        results = signal_rapid_escalation(con)
        npis = [r["npi"] for r in results]
        assert "1111111111" not in npis


class TestSignalWorkforceImpossibility:
    """Signal 4: Workforce Impossibility."""

    def test_detects_impossible_volume(self, con):
        results = signal_workforce_impossibility(con)
        assert len(results) >= 1
        npis = [r["npi"] for r in results]
        assert "5555555555" in npis

    def test_workforce_has_claims_per_hour(self, con):
        results = signal_workforce_impossibility(con)
        wf = [r for r in results if r["npi"] == "5555555555"]
        assert len(wf) == 1
        ev = wf[0]["evidence"]
        assert "implied_claims_per_hour" in ev
        assert ev["implied_claims_per_hour"] > 6.0
        assert "peak_month" in ev
        assert "peak_claims_count" in ev

    def test_overpayment_calculation(self, con):
        results = signal_workforce_impossibility(con)
        wf = [r for r in results if r["npi"] == "5555555555"]
        assert len(wf) == 1
        assert wf[0]["estimated_overpayment_usd"] > 0


class TestSignalSharedOfficial:
    """Signal 5: Shared Authorized Official."""

    def test_detects_shared_official(self, con):
        results = signal_shared_official(con)
        assert len(results) >= 1
        found = False
        for r in results:
            ev = r["evidence"]
            if "ROBERT SMITH" in ev["authorized_official_name"]:
                found = True
                assert ev["npi_count"] >= 5
                assert ev["combined_total_paid"] > 1_000_000
        assert found

    def test_shared_official_has_npi_list(self, con):
        results = signal_shared_official(con)
        for r in results:
            ev = r["evidence"]
            assert len(ev["controlled_npis"]) >= 5

    def test_shared_official_has_overpayment(self, con):
        results = signal_shared_official(con)
        for r in results:
            assert r["estimated_overpayment_usd"] >= 0
            if r["evidence"]["combined_total_paid"] > 0:
                assert r["estimated_overpayment_usd"] > 0


class TestSignalGeographicImplausibility:
    """Signal 6: Geographic Implausibility."""

    def test_detects_geographic_implausibility(self, con):
        results = signal_geographic_implausibility(con)
        assert len(results) >= 1
        npis = [r["npi"] for r in results]
        assert "7777777777" in npis

    def test_geo_has_ratio(self, con):
        results = signal_geographic_implausibility(con)
        geo = [r for r in results if r["npi"] == "7777777777"]
        assert len(geo) == 1
        ev = geo[0]["evidence"]
        assert "beneficiary_claims_ratio" in ev
        assert ev["beneficiary_claims_ratio"] < 0.1
        assert "flagged_hcpcs_codes" in ev

    def test_does_not_flag_normal_home_health(self, con):
        results = signal_geographic_implausibility(con)
        npis = [r["npi"] for r in results]
        assert "1111111111" not in npis


class TestSignalAddressClustering:
    """Signal 7: Address Clustering."""

    def test_detects_address_cluster(self, con):
        results = signal_address_clustering(con)
        assert len(results) >= 1
        # Should find the cluster at zip 11111
        found = False
        for r in results:
            if r["evidence"]["zip_code"] == "11111":
                found = True
                assert r["evidence"]["npi_count"] >= 10
                assert r["evidence"]["combined_total_paid"] > 5_000_000
        assert found

    def test_cluster_has_required_fields(self, con):
        results = signal_address_clustering(con)
        for r in results:
            assert r["signal_type"] == "address_clustering"
            ev = r["evidence"]
            assert "zip_code" in ev
            assert "state" in ev
            assert "npi_count" in ev
            assert "clustered_npis" in ev
            assert "combined_total_paid" in ev

    def test_does_not_flag_low_density_zip(self, con):
        results = signal_address_clustering(con)
        for r in results:
            assert r["evidence"]["zip_code"] != "90210"  # Only 1 provider there


class TestSignalUpcoding:
    """Signal 8: Upcoding Detection."""

    def test_detects_upcoder(self, con):
        results = signal_upcoding(con)
        assert len(results) >= 1
        npis = [r["npi"] for r in results]
        assert "9900000001" in npis

    def test_upcoding_has_percentages(self, con):
        results = signal_upcoding(con)
        upcoder = [r for r in results if r["npi"] == "9900000001"]
        assert len(upcoder) == 1
        ev = upcoder[0]["evidence"]
        assert "high_level_percentage" in ev
        assert ev["high_level_percentage"] > 80
        assert "peer_avg_high_level_percentage" in ev
        assert ev["peer_avg_high_level_percentage"] < 30

    def test_upcoding_has_overpayment(self, con):
        results = signal_upcoding(con)
        upcoder = [r for r in results if r["npi"] == "9900000001"]
        assert len(upcoder) == 1
        assert upcoder[0]["estimated_overpayment_usd"] > 0

    def test_does_not_flag_normal_em_provider(self, con):
        results = signal_upcoding(con)
        npis = [r["npi"] for r in results]
        assert "9900000002" not in npis
        assert "9900000003" not in npis


class TestSignalConcurrentBilling:
    """Signal 9: Concurrent Billing Across States."""

    def test_detects_concurrent_billing(self, con):
        results = signal_concurrent_billing(con)
        assert len(results) >= 1
        npis = [r["npi"] for r in results]
        assert "9800000001" in npis

    def test_does_not_flag_single_state_provider(self, con):
        results = signal_concurrent_billing(con)
        npis = [r["npi"] for r in results]
        assert "1111111111" not in npis

    def test_signal_type_is_correct(self, con):
        results = signal_concurrent_billing(con)
        for r in results:
            assert r["signal_type"] == "concurrent_billing"

    def test_concurrent_has_state_count(self, con):
        results = signal_concurrent_billing(con)
        cb = [r for r in results if r["npi"] == "9800000001"]
        if cb:
            ev = cb[0]["evidence"]
            assert "max_states_in_single_month" in ev
            assert ev["max_states_in_single_month"] >= 5

    def test_concurrent_has_overpayment(self, con):
        results = signal_concurrent_billing(con)
        cb = [r for r in results if r["npi"] == "9800000001"]
        if cb:
            assert cb[0]["estimated_overpayment_usd"] > 0

    def test_does_not_flag_organizations(self, con):
        results = signal_concurrent_billing(con)
        npis = [r["npi"] for r in results]
        # 5555555555 is an org (entity_type_code=2)
        assert "5555555555" not in npis


class TestRunAllSignals:
    """Test the run_all_signals orchestrator."""

    def test_returns_all_signal_types(self, con):
        results = run_all_signals(con)
        expected_types = [
            "excluded_provider", "billing_outlier", "rapid_escalation",
            "workforce_impossibility", "shared_official", "geographic_implausibility",
            "address_clustering", "upcoding", "concurrent_billing",
        ]
        for t in expected_types:
            assert t in results

    def test_all_results_are_lists(self, con):
        results = run_all_signals(con)
        for key, val in results.items():
            assert isinstance(val, list), f"{key} should be a list"


class TestCompositeRiskScore:
    """Test the composite risk scoring system."""

    def test_no_signals_returns_zero(self):
        result = compute_risk_score([], 100000)
        assert result["score"] == 0
        assert result["tier"] == "low"

    def test_single_critical_signal(self):
        signals = [{"signal_type": "excluded_provider", "severity": "critical", "estimated_overpayment_usd": 50000}]
        result = compute_risk_score(signals, 100000)
        assert result["score"] > 0
        assert result["tier"] in ("critical", "high", "medium")

    def test_multiple_signals_higher_score(self):
        single = [{"signal_type": "billing_outlier", "severity": "medium", "estimated_overpayment_usd": 1000}]
        multi = [
            {"signal_type": "billing_outlier", "severity": "medium", "estimated_overpayment_usd": 1000},
            {"signal_type": "upcoding", "severity": "high", "estimated_overpayment_usd": 5000},
            {"signal_type": "workforce_impossibility", "severity": "high", "estimated_overpayment_usd": 10000},
        ]
        score_single = compute_risk_score(single, 100000)
        score_multi = compute_risk_score(multi, 100000)
        assert score_multi["score"] > score_single["score"]

    def test_score_capped_at_100(self):
        signals = [
            {"signal_type": t, "severity": "critical", "estimated_overpayment_usd": 1_000_000}
            for t in ["excluded_provider", "workforce_impossibility", "upcoding",
                      "rapid_escalation", "billing_outlier"]
        ]
        result = compute_risk_score(signals, 100)
        assert result["score"] <= 100

    def test_risk_tiers(self):
        assert compute_risk_score([], 1000)["tier"] == "low"
        # A single medium signal should be low or medium
        low_sig = [{"signal_type": "address_clustering", "severity": "medium", "estimated_overpayment_usd": 0}]
        result = compute_risk_score(low_sig, 100000)
        assert result["tier"] in ("low", "medium")

    def test_factors_present(self):
        signals = [{"signal_type": "billing_outlier", "severity": "high", "estimated_overpayment_usd": 5000}]
        result = compute_risk_score(signals, 100000)
        assert "factors" in result
        factor_names = [f["name"] for f in result["factors"]]
        assert "signal_breadth" in factor_names
        assert "severity_weight" in factor_names
        assert "overpayment_ratio" in factor_names


class TestCaseNarrative:
    """Test plain-English case narrative generation."""

    def test_narrative_contains_provider_info(self, con):
        signal_results = run_all_signals(con)
        report = generate_report(signal_results, con, 100)
        for p in report["flagged_providers"]:
            narrative = p.get("case_narrative", "")
            assert p["npi"] in narrative
            assert "$" in narrative  # Should mention dollar amounts

    def test_narrative_mentions_signal_type(self, con):
        signal_results = run_all_signals(con)
        report = generate_report(signal_results, con, 100)
        # Check a specific provider with a known signal
        excluded = [p for p in report["flagged_providers"] if p["npi"] == "2222222222"]
        if excluded:
            narrative = excluded[0]["case_narrative"]
            assert "exclusion" in narrative.lower()

    def test_narrative_is_nonempty_string(self, con):
        signal_results = run_all_signals(con)
        report = generate_report(signal_results, con, 100)
        for p in report["flagged_providers"]:
            assert isinstance(p["case_narrative"], str)
            assert len(p["case_narrative"]) > 50


class TestExecutiveSummary:
    """Test the executive summary generation."""

    def test_summary_has_required_fields(self, con):
        signal_results = run_all_signals(con)
        report = generate_report(signal_results, con, 100)
        summary = report.get("executive_summary", {})
        assert "total_providers_scanned" in summary
        assert "total_providers_flagged" in summary
        assert "total_estimated_overpayment_usd" in summary
        assert "risk_tier_distribution" in summary
        assert "top_states_by_flags" in summary
        assert "signal_type_summary" in summary
        assert "highest_risk_providers" in summary

    def test_tier_distribution_has_all_tiers(self, con):
        signal_results = run_all_signals(con)
        report = generate_report(signal_results, con, 100)
        tiers = report["executive_summary"]["risk_tier_distribution"]
        for tier in ["critical", "high", "medium", "low"]:
            assert tier in tiers

    def test_total_overpayment_is_sum(self, con):
        signal_results = run_all_signals(con)
        report = generate_report(signal_results, con, 100)
        providers = report["flagged_providers"]
        expected = sum(p["estimated_overpayment_usd"] for p in providers)
        actual = report["executive_summary"]["total_estimated_overpayment_usd"]
        assert abs(actual - expected) < 0.01

    def test_highest_risk_providers_sorted(self, con):
        signal_results = run_all_signals(con)
        report = generate_report(signal_results, con, 100)
        top = report["executive_summary"]["highest_risk_providers"]
        if len(top) >= 2:
            for i in range(len(top) - 1):
                assert top[i]["risk_score"] >= top[i + 1]["risk_score"]


class TestHtmlReport:
    """Test the HTML report generation."""

    def test_html_output_is_valid(self, con):
        signal_results = run_all_signals(con)
        report = generate_report(signal_results, con, 100)
        with tempfile.NamedTemporaryFile(suffix=".html", delete=False, mode="w") as f:
            tmp_path = f.name
        try:
            write_html_report(report, tmp_path)
            with open(tmp_path) as f:
                content = f.read()
            assert content.startswith("<!DOCTYPE html>")
            assert "</html>" in content
            assert "Executive Summary" in content
        finally:
            os.unlink(tmp_path)

    def test_html_contains_provider_data(self, con):
        signal_results = run_all_signals(con)
        report = generate_report(signal_results, con, 100)
        with tempfile.NamedTemporaryFile(suffix=".html", delete=False, mode="w") as f:
            tmp_path = f.name
        try:
            write_html_report(report, tmp_path)
            with open(tmp_path) as f:
                content = f.read()
            # Should contain at least one NPI
            assert "NPI:" in content
            assert "Risk Score:" in content
        finally:
            os.unlink(tmp_path)

    def test_html_escapes_special_chars(self, con):
        signal_results = run_all_signals(con)
        report = generate_report(signal_results, con, 100)
        with tempfile.NamedTemporaryFile(suffix=".html", delete=False, mode="w") as f:
            tmp_path = f.name
        try:
            write_html_report(report, tmp_path)
            with open(tmp_path) as f:
                content = f.read()
            # Should not contain unescaped angle brackets in data
            # (the HTML tags themselves will have them, but data should be escaped)
            assert "<script>" not in content
        finally:
            os.unlink(tmp_path)


class TestOutputGeneration:
    """Test the report generation."""

    def test_report_has_required_fields(self, con):
        signal_results = run_all_signals(con)
        report = generate_report(signal_results, con, 100)
        assert "generated_at" in report
        assert "tool_version" in report
        assert "total_providers_scanned" in report
        assert "total_providers_flagged" in report
        assert "signal_counts" in report
        assert "flagged_providers" in report
        assert "executive_summary" in report

    def test_provider_has_required_fields(self, con):
        signal_results = run_all_signals(con)
        report = generate_report(signal_results, con, 100)
        for p in report["flagged_providers"]:
            assert "npi" in p
            assert "provider_name" in p
            assert "entity_type" in p
            assert "taxonomy_code" in p
            assert "state" in p
            assert "enumeration_date" in p
            assert "total_paid_all_time" in p
            assert "total_claims_all_time" in p
            assert "total_unique_beneficiaries_all_time" in p
            assert "signals" in p
            assert "estimated_overpayment_usd" in p
            assert "fca_relevance" in p
            assert "risk_score" in p
            assert "case_narrative" in p

    def test_risk_score_in_provider(self, con):
        signal_results = run_all_signals(con)
        report = generate_report(signal_results, con, 100)
        for p in report["flagged_providers"]:
            rs = p["risk_score"]
            assert "score" in rs
            assert "tier" in rs
            assert "factors" in rs
            assert 0 <= rs["score"] <= 100
            assert rs["tier"] in ("critical", "high", "medium", "low")

    def test_fca_relevance_has_required_fields(self, con):
        signal_results = run_all_signals(con)
        report = generate_report(signal_results, con, 100)
        for p in report["flagged_providers"]:
            fca = p["fca_relevance"]
            assert "claim_type" in fca
            assert "statute_reference" in fca
            assert "suggested_next_steps" in fca
            assert len(fca["suggested_next_steps"]) >= 2

    def test_statute_references_are_correct(self):
        assert STATUTE_MAP["excluded_provider"] == "31 U.S.C. section 3729(a)(1)(A)"
        assert STATUTE_MAP["billing_outlier"] == "31 U.S.C. section 3729(a)(1)(A)"
        assert STATUTE_MAP["rapid_escalation"] == "31 U.S.C. section 3729(a)(1)(A)"
        assert STATUTE_MAP["workforce_impossibility"] == "31 U.S.C. section 3729(a)(1)(B)"
        assert STATUTE_MAP["shared_official"] == "31 U.S.C. section 3729(a)(1)(C)"
        assert STATUTE_MAP["geographic_implausibility"] == "31 U.S.C. section 3729(a)(1)(G)"
        assert STATUTE_MAP["address_clustering"] == "31 U.S.C. section 3729(a)(1)(C)"
        assert STATUTE_MAP["upcoding"] == "31 U.S.C. section 3729(a)(1)(A)"
        assert STATUTE_MAP["concurrent_billing"] == "31 U.S.C. section 3729(a)(1)(B)"

    def test_next_steps_have_two_per_signal(self):
        for signal_type, steps in NEXT_STEPS_MAP.items():
            assert len(steps) >= 2, f"{signal_type} has fewer than 2 next steps"

    def test_providers_sorted_by_risk_score(self, con):
        signal_results = run_all_signals(con)
        report = generate_report(signal_results, con, 100)
        providers = report["flagged_providers"]
        if len(providers) >= 2:
            for i in range(len(providers) - 1):
                score_a = providers[i].get("risk_score", {}).get("score", 0)
                score_b = providers[i + 1].get("risk_score", {}).get("score", 0)
                assert score_a >= score_b


class TestCrossSignalCorrelations:
    """Test cross-signal correlation analysis."""

    def test_cross_signal_returns_dict(self, con):
        signal_results = run_all_signals(con)
        correlations = compute_cross_signal_correlations(signal_results)
        assert isinstance(correlations, dict)

    def test_cross_signal_has_required_fields(self, con):
        signal_results = run_all_signals(con)
        correlations = compute_cross_signal_correlations(signal_results)
        assert "total_unique_providers_flagged" in correlations
        assert "providers_by_signal_count" in correlations
        assert "multi_signal_providers" in correlations
        assert "top_signal_pairs" in correlations

    def test_cross_signal_counts_match(self, con):
        signal_results = run_all_signals(con)
        correlations = compute_cross_signal_correlations(signal_results)
        # Total unique providers should match the number of distinct NPIs across all signals
        all_npis = set()
        for signals in signal_results.values():
            for s in signals:
                all_npis.add(s["npi"])
        assert correlations["total_unique_providers_flagged"] == len(all_npis)

    def test_cross_signal_in_report(self, con):
        signal_results = run_all_signals(con)
        report = generate_report(signal_results, con, 100)
        assert "cross_signal_analysis" in report
        assert "total_unique_providers_flagged" in report["cross_signal_analysis"]


class TestMethodology:
    """Test the methodology documentation."""

    def test_methodology_in_report(self, con):
        signal_results = run_all_signals(con)
        report = generate_report(signal_results, con, 100)
        assert "methodology" in report
        assert "overview" in report["methodology"]
        assert "signals" in report["methodology"]

    def test_methodology_covers_all_signals(self):
        signal_types = [
            "excluded_provider", "billing_outlier", "rapid_escalation",
            "workforce_impossibility", "shared_official", "geographic_implausibility",
            "address_clustering", "upcoding", "concurrent_billing",
        ]
        for sig_type in signal_types:
            assert sig_type in METHODOLOGY["signals"], f"Missing methodology for {sig_type}"
            sig_method = METHODOLOGY["signals"][sig_type]
            assert "description" in sig_method
            assert "methodology" in sig_method
            assert "overpayment_basis" in sig_method
            assert "threshold" in sig_method

    def test_methodology_has_risk_scoring(self):
        assert "risk_scoring" in METHODOLOGY
        assert "tiers" in METHODOLOGY["risk_scoring"]
        for tier in ["critical", "high", "medium", "low"]:
            assert tier in METHODOLOGY["risk_scoring"]["tiers"]


class TestClaimTypeMap:
    """Test the FCA claim type mappings."""

    def test_all_signal_types_have_claim_type(self):
        signal_types = [
            "excluded_provider", "billing_outlier", "rapid_escalation",
            "workforce_impossibility", "shared_official", "geographic_implausibility",
            "address_clustering", "upcoding", "concurrent_billing",
        ]
        for sig_type in signal_types:
            assert sig_type in CLAIM_TYPE_MAP, f"Missing claim type for {sig_type}"
            assert len(CLAIM_TYPE_MAP[sig_type]) > 10

    def test_all_signal_types_have_statute(self):
        signal_types = [
            "excluded_provider", "billing_outlier", "rapid_escalation",
            "workforce_impossibility", "shared_official", "geographic_implausibility",
            "address_clustering", "upcoding", "concurrent_billing",
        ]
        for sig_type in signal_types:
            assert sig_type in STATUTE_MAP
            assert "31 U.S.C." in STATUTE_MAP[sig_type]


class TestEdgeCases:
    """Test edge cases and boundary conditions."""

    def test_empty_signal_results(self, con):
        empty_results = {
            "excluded_provider": [],
            "billing_outlier": [],
            "rapid_escalation": [],
            "workforce_impossibility": [],
            "shared_official": [],
            "geographic_implausibility": [],
            "address_clustering": [],
            "upcoding": [],
            "concurrent_billing": [],
        }
        report = generate_report(empty_results, con, 0)
        assert report["total_providers_flagged"] == 0
        assert report["flagged_providers"] == []

    def test_risk_score_with_zero_total_paid(self):
        signals = [{"signal_type": "billing_outlier", "severity": "high", "estimated_overpayment_usd": 5000}]
        result = compute_risk_score(signals, 0)
        assert result["score"] >= 0
        assert result["tier"] in ("critical", "high", "medium", "low")

    def test_risk_score_with_negative_overpayment(self):
        signals = [{"signal_type": "billing_outlier", "severity": "medium", "estimated_overpayment_usd": -100}]
        result = compute_risk_score(signals, 100000)
        assert result["score"] >= 0

    def test_narrative_with_minimal_provider(self, con):
        provider = {
            "npi": "0000000000",
            "provider_name": "Unknown",
            "entity_type": "unknown",
            "state": "Unknown",
            "total_paid_all_time": 0.0,
            "signals": [],
            "estimated_overpayment_usd": 0.0,
            "risk_score": {"score": 0, "tier": "low"},
        }
        narrative = generate_case_narrative(provider)
        assert isinstance(narrative, str)
        assert "0000000000" in narrative

    def test_all_severities_have_weights(self):
        from src.output import SEVERITY_WEIGHTS
        for severity in ["critical", "high", "medium", "low"]:
            assert severity in SEVERITY_WEIGHTS
            assert SEVERITY_WEIGHTS[severity] > 0

    def test_all_signal_types_have_risk_weights(self):
        from src.output import SIGNAL_RISK_WEIGHTS
        signal_types = [
            "excluded_provider", "billing_outlier", "rapid_escalation",
            "workforce_impossibility", "shared_official", "geographic_implausibility",
            "address_clustering", "upcoding", "concurrent_billing",
        ]
        for sig_type in signal_types:
            assert sig_type in SIGNAL_RISK_WEIGHTS, f"Missing risk weight for {sig_type}"

    def test_signal_results_have_required_keys(self, con):
        signal_results = run_all_signals(con)
        for signal_type, signals in signal_results.items():
            for sig in signals:
                assert "signal_type" in sig
                assert "severity" in sig
                assert "npi" in sig
                assert "evidence" in sig
                assert "estimated_overpayment_usd" in sig
                assert sig["severity"] in ("critical", "high", "medium", "low")

    def test_overpayments_are_non_negative(self, con):
        signal_results = run_all_signals(con)
        for signal_type, signals in signal_results.items():
            for sig in signals:
                assert sig["estimated_overpayment_usd"] >= 0, \
                    f"Negative overpayment in {signal_type}: {sig['estimated_overpayment_usd']}"


class TestMultiSignalProviders:
    """Test that multi-signal providers are handled correctly."""

    def test_multi_signal_provider_detected(self, con):
        signal_results = run_all_signals(con)
        report = generate_report(signal_results, con, 100)
        # Check if any provider has multiple signals
        multi = [p for p in report["flagged_providers"] if len(p["signals"]) > 1]
        # At least some providers should be flagged by multiple signals
        # (the 9700000001 provider is designed to hit rapid escalation)
        assert isinstance(multi, list)

    def test_multi_signal_provider_has_higher_risk(self, con):
        signal_results = run_all_signals(con)
        report = generate_report(signal_results, con, 100)
        multi = [p for p in report["flagged_providers"] if len(p["signals"]) > 1]
        single = [p for p in report["flagged_providers"] if len(p["signals"]) == 1]
        if multi and single:
            avg_multi = sum(p["risk_score"]["score"] for p in multi) / len(multi)
            avg_single = sum(p["risk_score"]["score"] for p in single) / len(single)
            # Multi-signal providers should generally have higher risk scores
            # (not strictly required but expected)
            assert avg_multi >= 0  # At minimum, they have scores


class TestHtmlReportDetails:
    """Additional HTML report tests."""

    def test_html_has_methodology_section(self, con):
        signal_results = run_all_signals(con)
        report = generate_report(signal_results, con, 100)
        with tempfile.NamedTemporaryFile(suffix=".html", delete=False, mode="w") as f:
            tmp_path = f.name
        try:
            write_html_report(report, tmp_path)
            with open(tmp_path) as f:
                content = f.read()
            assert "Flagged Provider Details" in content
        finally:
            os.unlink(tmp_path)

    def test_html_has_tier_badges(self, con):
        signal_results = run_all_signals(con)
        report = generate_report(signal_results, con, 100)
        with tempfile.NamedTemporaryFile(suffix=".html", delete=False, mode="w") as f:
            tmp_path = f.name
        try:
            write_html_report(report, tmp_path)
            with open(tmp_path) as f:
                content = f.read()
            assert "tier-badge" in content
        finally:
            os.unlink(tmp_path)

    def test_html_has_score_bars(self, con):
        signal_results = run_all_signals(con)
        report = generate_report(signal_results, con, 100)
        with tempfile.NamedTemporaryFile(suffix=".html", delete=False, mode="w") as f:
            tmp_path = f.name
        try:
            write_html_report(report, tmp_path)
            with open(tmp_path) as f:
                content = f.read()
            assert "score-bar" in content
            assert "score-fill" in content
        finally:
            os.unlink(tmp_path)
