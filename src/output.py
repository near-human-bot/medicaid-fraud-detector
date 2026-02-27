"""Report generation module — JSON and HTML output with risk scoring."""

import json
import html as html_lib
import re
from datetime import datetime, timezone

VERSION = "3.0.0"

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
    "burst_enrollment_network": "31 U.S.C. section 3729(a)(1)(C)",
    "coordinated_billing_ramp": "31 U.S.C. section 3729(a)(1)(C)",
    "phantom_servicing_hub": "31 U.S.C. section 3729(a)(1)(A)",
    "network_beneficiary_dilution": "31 U.S.C. section 3729(a)(1)(A)",
    "caregiver_density_anomaly": "31 U.S.C. section 3729(a)(1)(A)",
    "repetitive_service_abuse": "31 U.S.C. section 3729(a)(1)(A)",
    "billing_monoculture": "31 U.S.C. section 3729(a)(1)(A)",
    "billing_bust_out": "31 U.S.C. section 3729(a)(1)(A)",
    "reimbursement_rate_anomaly": "31 U.S.C. section 3729(a)(1)(A)",
    "phantom_servicing_spread": "31 U.S.C. section 3729(a)(1)(A)",
}

# Claim type descriptions
CLAIM_TYPE_MAP = {
    "excluded_provider": "Presenting false claims — excluded provider cannot legally bill federal healthcare programs",
    "billing_outlier": "Potential overbilling — provider billing significantly exceeds peer group norms",
    "rapid_escalation": "Potential bust-out scheme — newly enumerated provider with rapid billing escalation",
    "workforce_impossibility": "False records — billing volume implies physically impossible claim fabrication",
    "shared_official": "Conspiracy — coordinated billing through multiple entities controlled by same individual",
    "geographic_implausibility": "Geographic fraud — individual provider registered in one state but services overwhelmingly rendered in other states, suggesting phantom registration or NPI misuse",
    "address_clustering": "Potential ghost office — unusually high concentration of billing providers at single address",
    "upcoding": "Systematic upcoding — provider consistently bills highest-complexity codes far exceeding peer norms",
    "concurrent_billing": "Phantom billing — individual provider billing across multiple distant states simultaneously",
    "burst_enrollment_network": "Conspiracy — coordinated registration of multiple shell entities with identical specialties in rapid succession",
    "coordinated_billing_ramp": "Conspiracy — synchronized billing escalation across multiple entities controlled by same individual",
    "phantom_servicing_hub": "False claims — single servicing provider listed across many billing entities suggesting phantom referrals or kickbacks",
    "network_beneficiary_dilution": "False claims — network of entities recycling a small beneficiary pool across multiple shell organizations",
    "caregiver_density_anomaly": "False claims — zip code with anomalously high home health billing by individual providers with very few beneficiaries each, consistent with family member caregiver fraud rings",
    "repetitive_service_abuse": "Potential service fabrication — provider bills the same procedure code hundreds of times per beneficiary, far exceeding peer norms, consistent with therapy mill or personal care fraud",
    "billing_monoculture": "Single-code billing concentration — provider derives >85% of all claims from one HCPCS code, a pattern associated with organized fraud schemes targeting high-reimbursement procedures",
    "billing_bust_out": "Bust-out fraud lifecycle — provider exhibits rapid billing escalation followed by abrupt cessation, the complete operational signature of organized Medicaid bust-out schemes",
    "reimbursement_rate_anomaly": "Reimbursement rate manipulation — provider receives >3x the national median per-claim reimbursement for the same procedure code, consistent with modifier abuse or place-of-service fraud",
    "phantom_servicing_spread": "Phantom servicing with beneficiary dilution — servicing provider appears across multiple billing entities with impossibly few beneficiaries relative to claim volume, indicating fabricated services",
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
    "burst_enrollment_network": [
        "Investigate corporate filings for all entities to identify common beneficial owners or registered agents",
        "Check if enumeration dates coincide with any state Medicaid enrollment drives or policy changes",
        "Cross-reference with state licensing boards to verify each entity has legitimate facilities and staff",
    ],
    "coordinated_billing_ramp": [
        "Request detailed claims data for all network entities and compare billing patterns for identical timing",
        "Investigate whether the authorized official has ties to previously prosecuted fraud schemes",
        "Examine bank records to determine if payments flow to common accounts or individuals",
    ],
    "phantom_servicing_hub": [
        "Verify that the servicing provider has capacity to serve the volume of billing entities listed",
        "Request referral documentation and orders from each billing entity to the servicing provider",
        "Check for anti-kickback violations in the relationship between billing and servicing entities",
    ],
    "network_beneficiary_dilution": [
        "Request beneficiary-level claims data to determine if the same patients appear across multiple network entities",
        "Compare network beneficiary count to enrollment records to verify patients actually exist",
        "Interview a sample of listed beneficiaries to confirm services were actually received",
    ],
    "caregiver_density_anomaly": [
        "Verify caregiver-beneficiary relationships through state CDPAS or self-directed care program records",
        "Request timesheets and service logs for individual home health providers in the flagged zip code",
        "Interview a sample of beneficiaries to confirm services were actually received and hours are accurate",
    ],
    "repetitive_service_abuse": [
        "Request clinical records for a random sample of claims for the flagged procedure code and verify medical necessity documentation",
        "Interview beneficiaries to confirm the stated frequency of services was actually received",
        "Compare beneficiary care plans to billed service frequency to identify billing above authorized hours",
    ],
    "billing_monoculture": [
        "Request a full procedure code distribution report and compare to taxonomy peers",
        "Verify the provider has equipment, staffing, and licensure to perform the dominant service at the claimed volume",
    ],
    "billing_bust_out": [
        "Obtain corporate registration and dissolution records to determine if the entity was dissolved after billing cessation",
        "Request all claims data for the ramp and peak periods and verify beneficiary identities",
        "Check for common ownership or principals shared with previously prosecuted or excluded providers",
    ],
    "reimbursement_rate_anomaly": [
        "Request itemized claims data to identify which modifiers or place-of-service codes drive the elevated rate",
        "Verify that place-of-service codes match the provider's registered practice location and facility type",
    ],
    "phantom_servicing_spread": [
        "Verify the servicing provider has sufficient capacity and geographic presence to serve all listed billing entities",
        "Request referral documentation and service orders from each billing entity to confirm the relationship is real",
        "Obtain beneficiary-level claims to determine if the same patients appear across multiple billing entities",
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
    "phantom_servicing_hub": 9.0,
    "coordinated_billing_ramp": 8.0,
    "workforce_impossibility": 8.0,
    "network_beneficiary_dilution": 7.0,
    "upcoding": 7.0,
    "rapid_escalation": 7.0,
    "burst_enrollment_network": 6.0,
    "concurrent_billing": 6.0,
    "billing_outlier": 5.0,
    "shared_official": 5.0,
    "address_clustering": 4.0,
    "geographic_implausibility": 4.0,
    "caregiver_density_anomaly": 7.0,
    "reimbursement_rate_anomaly": 8.0,
    "billing_bust_out": 8.0,
    "repetitive_service_abuse": 7.0,
    "phantom_servicing_spread": 7.0,
    "billing_monoculture": 5.0,
}

# ---------------------------------------------------------------------------
# Known legitimate entity filter — suppress false positives from large orgs
# ---------------------------------------------------------------------------

# Name patterns that indicate government agencies, school districts, etc.
# ---------------------------------------------------------------------------
# Entity classification for filtering:
#   - "high_threshold": tribal health & government agencies — kept on report
#     only if they have exceptional evidence (3+ distinct signal types with
#     at least 1 high severity). Fraud does happen here but the bar is higher.
#   - "always_filter": commercial chains, labs, schools, nonprofits — removed.
# ---------------------------------------------------------------------------

_HIGH_THRESHOLD_PATTERNS = [
    # Government entities
    re.compile(r"\bCOUNTY\b", re.IGNORECASE),
    re.compile(r"\bPARISH\b", re.IGNORECASE),
    re.compile(r"\bSTATE\s+OF\b", re.IGNORECASE),
    re.compile(r"\bCITY\s+OF\b", re.IGNORECASE),
    re.compile(r"\bCOMMONWEALTH\s+OF\b", re.IGNORECASE),
    re.compile(r"\bHOSPITAL\s+(DISTRICT|AUTHORITY)\b", re.IGNORECASE),
    re.compile(r"\bBOARD\s+OF\s+(HEALTH|EDUCATION)\b", re.IGNORECASE),
    re.compile(r"\bPUBLIC\s+HEALTH\b", re.IGNORECASE),
    re.compile(r"\bCORRECTIONAL\b", re.IGNORECASE),
    # State/federal agencies
    re.compile(r"\bDEPARTMENT\s+OF\b", re.IGNORECASE),
    re.compile(r"\bDEPT\s+OF\b", re.IGNORECASE),
    re.compile(r"\bDIVISION\s+OF\b", re.IGNORECASE),
    re.compile(r"\bOFFICE\s+OF\b.*\bSTATE\b", re.IGNORECASE),
    re.compile(r"\bSTATE\s+ACADEMIES\b", re.IGNORECASE),
    re.compile(r"\bDHHS\b", re.IGNORECASE),
    re.compile(r"\bCOUNCIL\s+OF\s+GOVERNMENTS\b", re.IGNORECASE),
    re.compile(r"\bREGIONAL\s+COUNCIL\b", re.IGNORECASE),
    re.compile(r"\bDEVELOPMENT\s+DISTRICT\b", re.IGNORECASE),
    re.compile(r"\bAREA\s+AGENCY\s+ON\s+AGING\b", re.IGNORECASE),
    re.compile(r"\bTRANSIT\s+DISTRICT\b", re.IGNORECASE),
    re.compile(r"\bHUMAN\s+SERVICE\s+CENTER\b", re.IGNORECASE),
    re.compile(r"\bBOARD\s+OF\s+REGENTS\b", re.IGNORECASE),
    # Tribal / Native American / Sovereign nations
    re.compile(r"\bTRIB(E|AL|ES)\b", re.IGNORECASE),
    re.compile(r"\bINDIAN\s+(HEALTH|NATION)\b", re.IGNORECASE),
    re.compile(r"\bNATIVE\s+AMERICAN\b", re.IGNORECASE),
    re.compile(r"\bRANCHERIA\b", re.IGNORECASE),
    re.compile(r"\bBAND\s+OF\s+\w+\b", re.IGNORECASE),
    re.compile(r"\bNATION\b", re.IGNORECASE),
    re.compile(r"\bCHIEFS?\s+CONF", re.IGNORECASE),
    re.compile(r"\bCONFEDERATED\b", re.IGNORECASE),
]

_LEGITIMATE_NAME_PATTERNS = [
    # Schools and education
    re.compile(r"\bSCHOOL\s+(DISTRICT|BOARD)\b", re.IGNORECASE),
    re.compile(r"\bPUBLIC\s+SCHOOLS?\b", re.IGNORECASE),
    re.compile(r"\bINDEPENDENT\s+SCHOOL\b", re.IGNORECASE),
    re.compile(r"\bUNIFIED\s+SCHOOL\b", re.IGNORECASE),
    re.compile(r"\bEDUCATION(AL)?\s+SERVICES?\b", re.IGNORECASE),
    re.compile(r"\bBOARD\s+OF\s+EDUCATION\b", re.IGNORECASE),
    re.compile(r"\bCITY\s+SCHOOLS\b", re.IGNORECASE),
    re.compile(r"\b[A-Z]+\s+ISD\b"),
    re.compile(r"\b[A-Z]+\s+USD\b"),
    # Large nonprofits / community organizations
    re.compile(r"\bVISITING\s+NURSE\b", re.IGNORECASE),
    re.compile(r"\bSALVATION\s+ARMY\b", re.IGNORECASE),
    re.compile(r"\bPLANNED\s+PARENTHOOD\b", re.IGNORECASE),
    re.compile(r"\bGOODWILL\b", re.IGNORECASE),
    re.compile(r"\bCOMMUNITY\s+ACTION\b", re.IGNORECASE),
    re.compile(r"\bEASTER\s*SEALS\b", re.IGNORECASE),
    re.compile(r"\bCATHOLIC\s+(COMMUNITY\s+SERVICES|CHARITIES)\b", re.IGNORECASE),
    re.compile(r"\bJEWISH\s+(BOARD|CHILD|FAMILY|HOME)\b", re.IGNORECASE),
    re.compile(r"\bYMCA\b", re.IGNORECASE),
    re.compile(r"\bUNITED\s+CEREBRAL\s+PALSY\b", re.IGNORECASE),
    re.compile(r"\bVOLUNTEERS?\s+OF\s+AMERICA\b", re.IGNORECASE),
    re.compile(r"\bLUTHERAN\s+FAMILY\b", re.IGNORECASE),
    re.compile(r"\bTHE\s+ARC\b", re.IGNORECASE),
    re.compile(r"\bNEW\s+YORK\s+FOUNDLING\b", re.IGNORECASE),
    re.compile(r"\bHEBREW\s+HOME\b", re.IGNORECASE),
    re.compile(r"\bKENNEDY\s+KRIEGER\b", re.IGNORECASE),
    re.compile(r"\bNEMOURS\b", re.IGNORECASE),
    re.compile(r"\bFEDCAP\b", re.IGNORECASE),
    # Health system chains with many subsidiaries (prefix match)
    re.compile(r"^BANNER\s", re.IGNORECASE),
    re.compile(r"^SANFORD\s", re.IGNORECASE),
    re.compile(r"^STEWARD\s", re.IGNORECASE),
]

# Specific known large entities (substring match, case-insensitive)
_LEGITIMATE_ENTITY_SUBSTRINGS = [
    # National reference labs / diagnostics
    "quest diagnostics",
    "labcorp",
    "laboratory corporation",
    "bioreference",
    "ameripath",
    "unilab",
    "exact sciences",
    "veracyte",
    "caredx",
    # Large dialysis chains
    "fresenius",
    "davita",
    "dva renal",
    "total renal care",
    "bio-medical applications",
    "liberty dialysis",
    "satellite healthcare",
    "northwestern kidney",
    "rai care centers",
    # Major health systems — top 50 by size
    "kaiser",
    "permanente",
    "johns hopkins",
    "mayo clinic",
    "cleveland clinic",
    "stanford health",
    "mount sinai",
    "nyu langone",
    "new york university",
    "presbyterian hosp",
    "presbyterian healthcare",
    "boston medical center",
    "tufts medical center",
    "beth israel",
    "montefiore",
    "children's hospital",
    "childrens hospital",
    "children's",
    "cook children",
    "seattle children",
    "phoenix children",
    "lurie children",
    "rady children",
    "henry ford",
    "beaumont",
    "corewell",
    "geisinger",
    "sentara",
    "ascension",
    "prisma health",
    "sutter",
    "dignity health",
    "dignity community",
    "commonspirit",
    "catholic health",
    "providence health",
    "providence st joseph",
    "adventist health",
    "baptist health",
    "baptist healthcare",
    "baptist medical",
    "baptist memorial",
    "hca health",
    "baylor scott",
    "baylor medical",
    "baystate",
    "mercy health",
    "mercy hospital",
    "mercy medical",
    "mercy clinic",
    "mercy home care",
    "mercy rehab",
    "spectrum health",
    "advocate health",
    "advocate aurora",
    "aurora behavioral",
    "centracare",
    "metrohealth",
    "froedtert",
    "methodist hosp",
    "methodist health",
    # More major health systems
    "upmc",
    "northwell",
    "north shore university hospital",
    "atrium health",
    "ochsner",
    "memorial hermann",
    "cedars-sinai",
    "cedars sinai",
    "mass general brigham",
    "brigham and women",
    "yale new haven",
    "vanderbilt",
    "emory",
    "grady memorial",
    "scripps health",
    "intermountain",
    "ihc health",
    "scl health",
    "bon secours",
    "christus",
    "ssm health",
    "novant",
    "multicare",
    "hackensack meridian",
    "hmh hospitals",
    "meridian hospitals",
    "mainehealth",
    "main line hosp",
    "peacehealth",
    "honorhealth",
    "norton hosp",
    "orlando health",
    "carilion",
    "swedish health",
    "franciscan health",
    "tenet",
    "vhs ",
    "community health systems",
    "community health network",
    "steward medical",
    "steward good sam",
    "steward holy family",
    "steward sharon",
    "steward cgh",
    "steward norwood",
    "steward pgh",
    "prime healthcare",
    "mclaren",
    "munson healthcare",
    "trinity health",
    "lovelace health",
    "genesis healthcare",
    "appalachian regional",
    "gundersen",
    "thedacare",
    "pikeville medical",
    "alegent",
    "presence central",
    "mymichigan",
    "midmichigan",
    "tidalhealth",
    "virtua",
    "penn state health",
    "ohio state university",
    "west virginia university",
    "stony brook",
    "suny",
    "ucsf health",
    "ucsf benioff",
    "cooper health",
    "thomas jefferson university",
    "rutgers health",
    "ou medicine",
    "einstein medical",
    "banner health",
    "banner --",
    "dlp ",
    "new york city health",
    "hennepin healthcare",
    "virginia hospital center",
    "poudre valley",
    "monument health",
    "willis knighton",
    "halifax health",
    "covenant medical",
    "st. cloud hospital",
    "ahs hospital",
    "kpc global",
    "altamed",
    "ahmc ",
    # More health systems — round 2 audit
    "allina health",
    "adena health",
    "alameda health system",
    "bjc ",
    "bronxcare",
    "cape cod hospital",
    "centra health",
    "emanate health",
    "firsthealth",
    "floyd healthcare",
    "garnet health",
    "greenwich hospital",
    "holy redeemer",
    "howard university hospital",
    "huntington memorial",
    "hurley medical",
    "inova",
    "jefferson health",
    "kaleida health",
    "kaweah",
    "lahey clinic",
    "long beach memorial",
    "maimonides",
    "mainegeneral",
    "mary hitchcock",
    "medstar",
    "meharry medical",
    "morton plant",
    "mount carmel health",
    "new york eye",
    "north carolina baptist",
    "north shore-lij",
    "northbay healthcare",
    "northeast georgia medical",
    "ohiohealth",
    "owensboro health",
    "palomar health",
    "providence hospital",
    "regional west medical",
    "ridgeview medical",
    "rochester general",
    "saint alphonsus",
    "saint peter's university",
    "salt lake regional",
    "sanford health",
    "sanford bismarck",
    "sanford clinic",
    "sisters of charity",
    "south shore hospital",
    "southern baptist hospital",
    "southern new hampshire medical",
    "sparrow",
    "st dominic",
    "temple university hospital",
    "trinitas regional",
    "truman medical center",
    "university community hospital",
    "university health shreveport",
    "university hospitals",
    "university medical center",
    "uva ",
    "vassar brothers",
    "wesley medical center",
    "winona health",
    "winter haven hospital",
    "swedish covenant",
    "riverside university health",
    "bellin memorial",
    "columbus regional health",
    "holmes regional",
    "fresno community hospital",
    "glendale adventist",
    "halifax hospital",
    "phoebe putney",
    "deborah heart",
    "spartanburg medical",
    "lexington medical center",
    "sarah bush lincoln",
    "firelands regional",
    "milford regional",
    "monongahela valley",
    "dubois regional",
    "chambersburg hospital",
    "wellspan",
    "baycarehealth",
    "baycare",
    "nuvance health",
    "corewell health",
    "promedica",
    "hshs ",
    "hospital sisters",
    "one brooklyn health",
    "brookdale hospital",
    "cape fear valley",
    "ochsner",
    # Catholic / religious hospital names
    "st joseph health",
    "st. joseph health",
    "st joseph regional",
    "saint joseph regional",
    "st. joseph's hosp",
    "st. joseph's health",
    "st francis hosp",
    "saint francis hosp",
    "st jude hosp",
    "st vincent",
    "saint vincent",
    "st. agnes",
    "st. peter's health",
    "st. luke's",
    "st luke's",
    "our lady of",
    "lourdes hosp",
    "nazareth hosp",
    "blessing hosp",
    "christian hosp",
    "good samaritan hosp",
    "samaritan hosp",
    "columbia st. mary",
    # Academic medical centers / universities
    "university of",
    "regents of the university",
    "medical university",
    "michigan state university",
    "east carolina university",
    "boston university",
    "loma linda university",
    "duke university",
    "medical college of wisconsin",
    "recinto de ciencias medicas",
    # Government / public health systems
    "new york city health and hospitals",
    "detroit wayne mental health",
    # Large home health / hospice / DME chains
    "apria",
    "lincare",
    "bayada",
    "maxim healthcare",
    "vitas",
    "heartland hospice",
    "amedisys",
    "encompass health",
    "kindred",
    "brightspring",
    "interim healthcare",
    "consumer direct",
    "public partnerships",
    "national seating",
    "byram healthcare",
    "stateserv",
    "aeroflow",
    "norco inc",
    # Medical device / pharma / diagnostics companies
    "insulet",
    "novocure",
    "kci usa",
    "guardant health",
    "djo, llc",
    "adapthealth",
    # National service chains — round 2
    "ati holdings",
    "ati physical therapy",
    "national mentor",
    "sevita",
    "texas oncology",
    "uhs-",
    "universal health services",
    # Large physician groups / medical groups
    "pediatrix",
    "mednax",
    "springfield clinic",
    "prohealth care",
    "healthcare partners affiliates",
    "american medical response",
    # Large pharmacy / PBM / insurance chains
    "cvs",
    "walgreens",
    "rite aid",
    "walmart",
    "optum",
    "unitedhealth",
    "united healthcare",
    "centene",
    "molina",
    "anthem",
    "humana",
    "cigna",
    "aetna",
    "blue cross",
    "bluecross",
    "wellcare",
    "amerihealth",
    "carefirst",
    "palco",
    # Large nonprofits
    "easterseals",
    "easter seals",
    "elwyn",
    "mosaic",
    "evangelical lutheran good samaritan",
    "lutheran community services",
    "heartland alliance",
    "southcentral foundation",
    "yukon kuskokwim",
    "yukon-kuskokwim",
    "kidspeace",
    "bair foundation",
    "fortune society",
    "educational alliance",
    "great expressions dental",
    "emeritus",
    "cano health",
    "penn state university",
    "oklahoma state university",
    "tufts university",
    "ucsf",
    "unc physicians",
    "unlv medicine",
    "usc care",
    "usc verdugo",
]

# Pre-compile substrings to lowercase for fast matching
_LEGITIMATE_ENTITY_SUBSTRINGS_LOWER = [s.lower() for s in _LEGITIMATE_ENTITY_SUBSTRINGS]


def is_known_legitimate_entity(provider_name: str) -> bool:
    """Return True if the provider name matches a known legitimate entity pattern.

    These are always filtered from reports (commercial chains, labs, schools, nonprofits).
    """
    if not provider_name:
        return False

    # Filter out data quality issues
    if provider_name.strip().lower() in ("unknown", ""):
        return True

    # Check regex patterns (schools, nonprofits, health system chains)
    for pattern in _LEGITIMATE_NAME_PATTERNS:
        if pattern.search(provider_name):
            return True

    # Check known entity substrings
    name_lower = provider_name.lower()
    for substring in _LEGITIMATE_ENTITY_SUBSTRINGS_LOWER:
        if substring in name_lower:
            return True

    return False


def is_high_threshold_entity(provider_name: str) -> bool:
    """Return True if the provider is a tribal health or government entity.

    These entities are NOT auto-filtered. Instead they require exceptional
    evidence to appear on a fraud report: 3+ distinct signal types with at
    least 1 high-severity signal. Fraud does happen in government and tribal
    health, but the evidentiary bar should be higher to avoid embarrassing
    false positives in a government-bound report.
    """
    if not provider_name:
        return False
    for pattern in _HIGH_THRESHOLD_PATTERNS:
        if pattern.search(provider_name):
            return True
    return False


# Minimum evidence thresholds for high-threshold entities
HIGH_THRESHOLD_MIN_SIGNAL_TYPES = 3
HIGH_THRESHOLD_REQUIRES_HIGH_SEVERITY = True


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
                f"{ev.get('peak_claims_count', 0):,} claims with {ev.get('distinct_workers_in_month', 1)} servicing providers, "
                f"implying {ev.get('implied_claims_per_worker_hour', ev.get('implied_claims_per_hour', 0)):.1f} claims "
                f"per worker per hour — a physically impossible volume."
            )
        elif stype == "shared_official":
            signal_descriptions.append(
                f"The authorized official ({ev.get('authorized_official_name', 'unknown')}) controls "
                f"{ev.get('npi_count', 0)} NPIs with combined billing of ${ev.get('combined_total_paid', 0):,.2f}, "
                f"suggesting a coordinated billing network."
            )
        elif stype == "geographic_implausibility":
            signal_descriptions.append(
                f"Provider registered in {ev.get('registered_state', 'unknown')} but only "
                f"{ev.get('home_state_pct', 0):.1f}% of their {ev.get('total_claims', 0):,} claims "
                f"were serviced in their home state — services are being rendered in "
                f"{ev.get('foreign_states_count', 0)} other states, suggesting the NPI registration "
                f"is geographically detached from actual service delivery."
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
        elif stype == "burst_enrollment_network":
            signal_descriptions.append(
                f"This provider is part of a cluster of {ev.get('npi_count', 0)} organizations "
                f"registered in the same quarter ({ev.get('enrollment_quarter', 'unknown')}) with identical "
                f"taxonomy ({ev.get('taxonomy_code', 'unknown')}) in {ev.get('state', 'unknown')}, "
                f"with combined billing of ${ev.get('combined_total_paid', 0):,.2f} — "
                f"a pattern consistent with coordinated shell entity registration."
            )
        elif stype == "coordinated_billing_ramp":
            signal_descriptions.append(
                f"This provider belongs to a network of {ev.get('network_npi_count', 0)} NPIs "
                f"controlled by {ev.get('authorized_official_name', 'the same official')}, "
                f"where {ev.get('peaking_npi_count', 0)} NPIs peaked billing within "
                f"{ev.get('peak_spread_months', 0)} month(s) — a coordination fingerprint "
                f"indicating a synchronized bust-out scheme."
            )
        elif stype == "phantom_servicing_hub":
            signal_descriptions.append(
                f"The servicing provider NPI {ev.get('servicing_npi', 'unknown')} appears across "
                f"{ev.get('billing_npi_count', 0)} distinct billing entities with "
                f"${ev.get('total_paid_through_hub', 0):,.2f} in total payments — "
                f"suggesting a phantom referral hub or kickback arrangement."
            )
        elif stype == "network_beneficiary_dilution":
            signal_descriptions.append(
                f"The network controlled by {ev.get('authorized_official_name', 'the same official')} "
                f"({ev.get('network_npi_count', 0)} NPIs) shows {ev.get('claims_per_beneficiary', 0):.1f} "
                f"claims per beneficiary — indicating the same small group of beneficiaries "
                f"is being recycled across multiple shell entities."
            )
        elif stype == "caregiver_density_anomaly":
            census_detail = ""
            if "census_ratio_to_state_median" in ev:
                census_detail = (
                    f" Census data shows only {ev.get('census_vulnerable_population', 0):,} "
                    f"elderly/disabled residents in this zip, yet billing is "
                    f"{ev.get('census_ratio_to_state_median', 0):.1f}x the state median per vulnerable person."
                )
            signal_descriptions.append(
                f"This provider is in zip code {ev.get('zip_code', 'unknown')} where "
                f"{ev.get('individual_provider_count', 0)} individual home health providers "
                f"serve only {ev.get('beneficiaries_per_individual_provider', 0):.1f} beneficiaries each "
                f"on average, with combined billing of ${ev.get('total_hh_paid', 0):,.2f} — "
                f"{ev.get('ratio_to_state_median', 0):.1f}x the state median. "
                f"This pattern is consistent with family member caregiver fraud rings.{census_detail}"
            )
        elif stype == "repetitive_service_abuse":
            signal_descriptions.append(
                f"This provider billed HCPCS code {ev.get('hcpcs_code', 'unknown')} "
                f"{ev.get('claims_per_beneficiary', 0):.0f} times per beneficiary on average "
                f"({ev.get('total_claims', 0):,} total claims across {ev.get('total_beneficiaries', 0):,} "
                f"beneficiaries), compared to a peer 99th percentile of "
                f"{ev.get('peer_99th_percentile_claims_per_bene', 0):.0f} — "
                f"a pattern consistent with therapy mill or personal care service fabrication."
            )
        elif stype == "billing_monoculture":
            signal_descriptions.append(
                f"This provider derives {ev.get('dominant_code_share_pct', 0):.1f}% of all claims "
                f"from a single procedure code ({ev.get('dominant_hcpcs_code', 'unknown')}), "
                f"billing {ev.get('dominant_code_claims', 0):,} claims of this one code out of "
                f"{ev.get('total_claims_all_codes', 0):,} total — a concentration pattern "
                f"associated with fraud schemes built around a single high-reimbursement service."
            )
        elif stype == "billing_bust_out":
            signal_descriptions.append(
                f"This provider's billing peaked at ${ev.get('peak_paid', 0):,.2f} in "
                f"{ev.get('peak_month', 'unknown')} then collapsed to "
                f"{ev.get('post_peak_pct_of_peak', 0):.1f}% of peak within 3 months — "
                f"the complete ramp-and-abandon lifecycle of organized Medicaid bust-out fraud."
            )
        elif stype == "reimbursement_rate_anomaly":
            signal_descriptions.append(
                f"This provider receives ${ev.get('avg_rate_per_claim', 0):,.2f} per claim for "
                f"HCPCS code {ev.get('hcpcs_code', 'unknown')}, which is "
                f"{ev.get('rate_ratio_to_median', 0):.1f}x the national median of "
                f"${ev.get('national_median_rate', 0):,.2f} — indicating modifier abuse, "
                f"place-of-service fraud, or billing rate manipulation."
            )
        elif stype == "phantom_servicing_spread":
            signal_descriptions.append(
                f"This servicing provider appears across {ev.get('distinct_billing_npis', 0)} "
                f"billing entities with only {ev.get('total_beneficiaries', 0):,} unique "
                f"beneficiaries across {ev.get('total_claims', 0):,} total claims "
                f"({ev.get('claims_per_beneficiary', 0):.0f} claims per beneficiary) — "
                f"consistent with a phantom servicing hub where most billed services were never rendered."
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


def _batch_load_provider_info(npi_list: list[str], con) -> tuple[dict, dict]:
    """Batch-load NPPES info and spending totals for all flagged NPIs.

    Returns (nppes_map, totals_map) keyed by NPI string.
    This replaces the N+1 per-NPI queries that previously scanned
    the full spending table once per flagged provider.
    """
    if not npi_list:
        return {}, {}

    # Batch NPPES lookup
    nppes_map = {}
    try:
        rows = con.execute("""
            SELECT
                npi,
                COALESCE(org_name, first_name || ' ' || last_name) AS provider_name,
                CASE WHEN entity_type_code = '1' THEN 'individual' ELSE 'organization' END AS entity_type,
                taxonomy_code,
                state,
                enumeration_date
            FROM nppes
            WHERE npi IN (SELECT UNNEST(?::VARCHAR[]))
        """, [npi_list]).fetchall()
        for row in rows:
            nppes_map[row[0]] = {
                "provider_name": row[1] or "Unknown",
                "entity_type": row[2] or "unknown",
                "taxonomy_code": row[3] or "Unknown",
                "state": row[4] or "Unknown",
                "enumeration_date": str(row[5]) if row[5] else "Unknown",
            }
    except Exception:
        pass

    # Batch spending totals from pre-materialized provider_totals table
    totals_map = {}
    try:
        rows = con.execute("""
            SELECT
                npi,
                total_paid,
                total_claims,
                total_beneficiaries
            FROM provider_totals
            WHERE npi IN (SELECT UNNEST(?::VARCHAR[]))
        """, [npi_list]).fetchall()
        for row in rows:
            totals_map[row[0]] = {
                "total_paid": float(row[1] or 0),
                "total_claims": int(row[2] or 0),
                "total_beneficiaries": int(row[3] or 0),
            }
    except Exception:
        pass

    return nppes_map, totals_map


def build_provider_record(npi: str, signals: list[dict], nppes_map: dict, totals_map: dict) -> dict:
    """Build a complete provider record from their signals.

    Uses pre-loaded nppes_map and totals_map instead of per-NPI queries.
    """
    info = nppes_map.get(npi, {})
    provider_name = info.get("provider_name", "Unknown")
    entity_type = info.get("entity_type", "unknown")
    taxonomy_code = info.get("taxonomy_code", "Unknown")
    state = info.get("state", "Unknown")
    enum_date = info.get("enumeration_date", "Unknown")

    # Override with signal-specific info if available
    for sig in signals:
        ev = sig.get("evidence", {})
        if "state" in ev and ev["state"]:
            state = ev["state"]
        if "taxonomy_code" in ev and ev["taxonomy_code"]:
            taxonomy_code = ev["taxonomy_code"]

    totals = totals_map.get(npi, {})
    total_paid = totals.get("total_paid", 0.0)
    total_claims = totals.get("total_claims", 0)
    total_beneficiaries = totals.get("total_beneficiaries", 0)

    # Build signal list
    signal_records = []
    total_overpayment = 0.0
    for sig in signals:
        sig_type = sig["signal_type"]
        overpayment = sig.get("estimated_overpayment_usd", 0.0)
        # Cap per-signal overpayment at the provider's own total billing.
        # Network-level signals (shared_official, address_clustering) compute
        # overpayment from combined network billing and can otherwise assign
        # amounts wildly exceeding what this individual provider actually billed.
        total_overpayment += overpayment
        signal_records.append({
            "signal_type": sig_type,
            "severity": sig["severity"],
            "evidence": sig["evidence"],
        })

    # Cap total estimated overpayment at the provider's own total billing.
    # Multiple signals can individually produce reasonable estimates that sum
    # to more than the provider actually billed (e.g. network-level signals
    # attribute combined network billing to each member).
    if total_paid > 0:
        total_overpayment = min(total_overpayment, total_paid)

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


METHODOLOGY = {
    "overview": (
        "This tool cross-references three federal datasets — CMS Medicaid Provider Spending (227M rows), "
        "OIG LEIE Exclusion List, and CMS NPPES NPI Registry — to detect 19 categories of fraud signals "
        "including 4 network-level signals targeting coordinated group fraud (Feeding Our Future-style schemes). "
        "All processing uses DuckDB for memory-efficient out-of-core analytics on machines with limited RAM."
    ),
    "signals": {
        "excluded_provider": {
            "description": "Identifies providers on the OIG LEIE exclusion list who continue billing Medicaid",
            "methodology": "Join billing NPI and servicing NPI against LEIE exclusion records where exclusion date precedes claim date and no reinstatement",
            "overpayment_basis": "100% of post-exclusion payments — all payments to excluded providers are improper per 42 CFR 1001.1901",
            "threshold": "Any match = flag (zero tolerance for excluded provider billing)",
        },
        "billing_outlier": {
            "description": "Identifies providers billing far above their peer group (same taxonomy + state)",
            "methodology": "Compute per-provider total paid, group by taxonomy_code + state, flag providers above the 99th percentile",
            "overpayment_basis": "Amount exceeding the 99th percentile of the peer group",
            "threshold": "Total paid > peer group 99th percentile, with minimum 5 peers in group",
        },
        "rapid_escalation": {
            "description": "Detects newly enrolled entities with suspicious billing growth patterns",
            "methodology": "Filter to providers enumerated within 24 months of first billing, compute rolling 3-month average growth rate",
            "overpayment_basis": "Sum of payments during months with >200% growth rate",
            "threshold": "Rolling 3-month average growth rate > 200%",
        },
        "workforce_impossibility": {
            "description": "Flags organizations billing impossible claim volumes relative to their workforce size",
            "methodology": "For entity type 2 (organizations), compute peak monthly claims divided by distinct servicing NPIs (workforce proxy) / 22 workdays / 8 hours. Large organizations are not penalized for high absolute volumes — only for rates that are impossible per worker.",
            "overpayment_basis": "Excess claims beyond 6/worker/hour threshold x average cost per claim",
            "threshold": "Implied claims per worker per hour > 6.0",
        },
        "shared_official": {
            "description": "Identifies networks where one authorized official controls 5-50 NPIs concentrated in the same state",
            "methodology": "Group NPPES records by authorized official name, join with billing totals. Require geographic concentration (≥3 NPIs in same state) to eliminate name-collision false positives. Cap at 50 NPIs to exclude legitimate large health systems.",
            "overpayment_basis": "10-20% of combined network billing (DOJ settlement data for shell networks), capped at individual provider's own billing",
            "threshold": "5-50 distinct NPIs under same official, combined billing > $1M, ≥3 NPIs in same state",
        },
        "geographic_implausibility": {
            "description": "Detects individual providers whose services are overwhelmingly rendered outside their registered home state",
            "methodology": "For individual providers (entity_type=1), compute the fraction of claims serviced in the billing NPI's registered state using serv_state_monthly. Flag when <10% of claims are in the home state across ≥2 foreign states.",
            "overpayment_basis": "40% of foreign-state billing (conservative estimate of phantom services)",
            "threshold": "<10% home-state claims, ≥500 total claims, ≥2 foreign service states, ≥$50K total paid",
        },
        "address_clustering": {
            "description": "Identifies zip codes with unusually high concentrations of billing providers",
            "methodology": "Group providers by zip code, sum billing, flag high-density clusters",
            "overpayment_basis": "15% of combined billing (OIG ghost office investigation data)",
            "threshold": "10+ NPIs at same zip code with >$5M combined billing",
        },
        "upcoding": {
            "description": "Detects providers systematically billing highest-complexity E&M codes",
            "methodology": "Compute high-level E&M code percentage per provider, compare to taxonomy+state peer average",
            "overpayment_basis": "Conservative 30% uplift on excess high-level billing vs peer average",
            "threshold": "Provider >80% high-complexity E&M codes when peer average <30%, minimum 50 E&M claims",
        },
        "concurrent_billing": {
            "description": "Flags individual providers billing in 5+ states in a single month",
            "methodology": "Join billing NPI with servicing NPI state, count distinct states per month per individual",
            "overpayment_basis": "60% of payments in flagged months (legitimate multi-state practice is rare for individuals)",
            "threshold": "5+ distinct states in any single month, individuals only (orgs excluded)",
        },
        "burst_enrollment_network": {
            "description": "Detects clusters of 4+ organizations registered in the same quarter with identical taxonomy and state",
            "methodology": "Group NPPES organizations by taxonomy_code + state + quarter(enumeration_date), join with provider_totals for combined billing",
            "overpayment_basis": "25% of combined network billing (DOJ shell network settlement data)",
            "threshold": "4+ NPIs in same taxonomy+state+quarter with >$500K combined billing",
        },
        "coordinated_billing_ramp": {
            "description": "Identifies networks (via shared authorized official) where 3+ NPIs peak billing within a 3-month window",
            "methodology": "Join official networks with provider_monthly, find each NPI's peak month, flag networks where peaks cluster within 3 months",
            "overpayment_basis": "30% of network total paid (coordinated bust-out pattern)",
            "threshold": "3+ NPIs peaking within 3 months under same official; critical if spread<=1mo + 5+ NPIs + >$2M",
        },
        "phantom_servicing_hub": {
            "description": "Detects a single servicing NPI appearing across 5+ distinct billing entities",
            "methodology": "Group spending by servicing_npi (excluding self-servicing), count distinct billing NPIs per servicing provider",
            "overpayment_basis": "35% of total paid through hub (phantom referral/kickback pattern)",
            "threshold": "5+ distinct billing NPIs with >$500K total; critical if 15+ billing NPIs or bene_ratio<0.1",
        },
        "network_beneficiary_dilution": {
            "description": "Flags networks where combined beneficiary/claims ratio is impossibly low",
            "methodology": "Group provider_totals by authorized official network, compute network-wide beneficiary/claims ratio, compare to peer percentiles",
            "overpayment_basis": "Excess claims above peer median rate x average cost per claim, capped at 80%",
            "threshold": ">50 claims per beneficiary or ratio below 10th percentile with 5+ NPIs",
        },
        "caregiver_density_anomaly": {
            "description": "Detects zip codes where home health / PCA billing is anomalously concentrated among individual providers with very few beneficiaries — the family member caregiver fraud pattern",
            "methodology": "Aggregate home health HCPCS billing by provider zip code from pre-materialized hh_zip_totals table. Compute per-zip: total billing, individual provider ratio, beneficiaries per provider. Compare to state median. Optionally enrich with Census ACS ZCTA demographics (elderly + disabled population) to compute expected demand.",
            "overpayment_basis": "40% of excess above state median (OIG family caregiver fraud settlement data)",
            "threshold": "Home health billing >3x state median per zip, >50% individual providers, <5 beneficiaries per individual provider, minimum $100K in zip",
        },
        "repetitive_service_abuse": {
            "description": "Flags providers billing the same HCPCS code at impossibly high claims-per-beneficiary ratios compared to national peers — therapy mill and PCA fraud pattern",
            "methodology": "From provider_code_totals, compute claims/beneficiary ratio per (NPI, HCPCS code). Compare to 99th percentile of all providers billing the same code nationally (minimum 10 peers). Flag providers exceeding p99.",
            "overpayment_basis": "80% of excess claims above p99 threshold × average cost per claim",
            "threshold": ">200 total claims, claims-per-beneficiary exceeds 99th percentile of same-code peers nationally",
        },
        "billing_monoculture": {
            "description": "Detects providers where >85% of all claims come from a single HCPCS code — a concentration pattern associated with fraud mills targeting one high-reimbursement service",
            "methodology": "From provider_code_totals, compute each NPI's dominant HCPCS code share as percentage of total claims. Flag providers with >85% concentration and >500 total claims.",
            "overpayment_basis": "25% of total billing × excess concentration above 85% threshold",
            "threshold": "Dominant code share >85%, total claims >500",
        },
        "billing_bust_out": {
            "description": "Detects the complete fraud lifecycle: rapid billing ramp followed by abrupt cessation — the signature of organized Medicaid bust-out schemes",
            "methodology": "From provider_monthly, identify each NPI's peak billing month. Verify pre-peak ramp (3-month avg <50% of peak) and post-peak collapse (3-month avg <10% of peak). Requires 6+ billing months and 2+ pre-peak months.",
            "overpayment_basis": "40% of total billing during ramp + peak period (OIG bust-out prosecution data)",
            "threshold": "Peak >$50K, pre-peak avg <50% of peak, post-peak 3-month avg <10% of peak, 6+ billing months",
        },
        "reimbursement_rate_anomaly": {
            "description": "Flags providers receiving >3x the national median per-claim reimbursement for the same HCPCS code — catches modifier abuse, place-of-service fraud, and billing manipulation",
            "methodology": "From provider_code_totals, compute average paid per claim per (NPI, HCPCS code). Compare to national median for that code (minimum 10 peers). Flag providers with rate >3x median.",
            "overpayment_basis": "70% of excess per-claim rate above median × total claims",
            "threshold": "Per-claim rate >3x national median, >100 total claims, 10+ peers for the code",
        },
        "phantom_servicing_spread": {
            "description": "Flags servicing NPIs appearing across 5+ billing entities with impossibly low beneficiary-to-claims ratio — phantom servicing where most billed services were never rendered",
            "methodology": "From servicing_hub_totals, group by servicing NPI and compute beneficiary/claims ratio. Flag when claims-per-beneficiary >100 or ratio below 10th percentile of hub peers. Minimum 5 billing entities and $200K total paid.",
            "overpayment_basis": "65% of excess claims above expected volume at p10 baseline × average cost per claim",
            "threshold": "5+ billing entities, >$200K total, claims/beneficiary >100 or bene ratio below p10 peers",
        },
    },
    "risk_scoring": {
        "description": "Each provider receives a composite risk score (0-100) combining signal breadth, severity weight, and overpayment ratio",
        "tiers": {
            "critical": "75-100 — immediate investigation priority",
            "high": "50-74 — investigation recommended within 30 days",
            "medium": "25-49 — review recommended within 90 days",
            "low": "0-24 — monitor and reassess in next cycle",
        },
    },
}


def _select_top_providers(
    providers: list[dict], max_providers: int = 5000, min_per_signal: int = 100
) -> list[dict]:
    """Select top providers guaranteeing minimum representation per signal type.

    Strategy:
    1. First, guarantee at least min_per_signal providers for each signal type
       (taking the highest-risk providers for each signal).
    2. Fill remaining slots with the highest-risk providers overall.
    3. Return at most max_providers, sorted by risk score descending.
    """
    if len(providers) <= max_providers:
        return providers

    selected_npis: set[str] = set()

    # Phase 1: Guarantee min_per_signal per signal type
    signal_type_providers: dict[str, list[dict]] = {}
    for p in providers:
        for sig in p["signals"]:
            stype = sig["signal_type"]
            if stype not in signal_type_providers:
                signal_type_providers[stype] = []
            signal_type_providers[stype].append(p)

    for stype, stype_providers in signal_type_providers.items():
        # Deduplicate by NPI (providers already sorted by risk score)
        seen_npis: set[str] = set()
        count = 0
        for p in stype_providers:
            if p["npi"] not in seen_npis and p["npi"] not in selected_npis:
                selected_npis.add(p["npi"])
                seen_npis.add(p["npi"])
                count += 1
                if count >= min_per_signal:
                    break

    # Phase 2: Fill remaining slots with highest-risk providers
    for p in providers:
        if len(selected_npis) >= max_providers:
            break
        selected_npis.add(p["npi"])

    # Return selected providers in original risk-score order
    result = [p for p in providers if p["npi"] in selected_npis]
    return result


def generate_report(signal_results: dict, con, total_providers_scanned: int) -> dict:
    """Generate the final fraud_signals.json report."""
    from src.signals import compute_cross_signal_correlations

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

    # Batch-load all provider info and spending totals (replaces N+1 queries)
    all_npis = list(npi_signals.keys())
    nppes_map, totals_map = _batch_load_provider_info(all_npis, con)

    # Filter out known legitimate entities and unresolvable NPIs before building records.
    # High-threshold entities (tribal/government) are only excluded if they lack
    # exceptional evidence (3+ distinct signal types with at least 1 high severity).
    excluded_npis = set()
    high_threshold_kept = 0
    for npi in list(npi_signals.keys()):
        info = nppes_map.get(npi, {})
        name = info.get("provider_name", "")
        if not name or is_known_legitimate_entity(name):
            excluded_npis.add(npi)
        elif is_high_threshold_entity(name):
            signals_for_npi = npi_signals[npi]
            distinct_types = set(s["signal_type"] for s in signals_for_npi)
            has_high = any(s["severity"] == "high" for s in signals_for_npi)
            if len(distinct_types) >= HIGH_THRESHOLD_MIN_SIGNAL_TYPES and has_high:
                high_threshold_kept += 1  # exceptional evidence — keep on report
            else:
                excluded_npis.add(npi)
    if excluded_npis:
        print(f"  Filtered {len(excluded_npis)} known legitimate entities from flagged results")
    if high_threshold_kept:
        print(f"  Kept {high_threshold_kept} tribal/government entities with exceptional evidence")

    # Build provider records
    flagged_providers = []
    for npi, signals in npi_signals.items():
        if npi in excluded_npis:
            continue
        provider = build_provider_record(npi, signals, nppes_map, totals_map)
        flagged_providers.append(provider)

    # Sort by risk score descending (then overpayment as tiebreaker)
    flagged_providers.sort(
        key=lambda p: (p.get("risk_score", {}).get("score", 0), p["estimated_overpayment_usd"]),
        reverse=True,
    )

    # Select top 5000 providers, guaranteeing at least 100 per signal type
    flagged_providers = _select_top_providers(flagged_providers, max_providers=5000, min_per_signal=100)

    report = {
        "generated_at": datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ"),
        "tool_version": VERSION,
        "methodology": METHODOLOGY,
        "total_providers_scanned": total_providers_scanned,
        "total_providers_flagged": len(flagged_providers),
        "signal_counts": signal_counts,
        "cross_signal_analysis": compute_cross_signal_correlations(signal_results),
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
    for p in providers[:5000]:  # Top 5000 in HTML
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

    if len(providers) > 5000:
        lines.append(f"<p><em>Showing top 5,000 of {len(providers)} flagged providers. "
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


# ---------------------------------------------------------------------------
# Feeding Our Future-style Network Fraud Report
# ---------------------------------------------------------------------------

# Signal types that indicate coordinated group fraud
FOF_SIGNAL_TYPES = {
    "burst_enrollment_network",
    "coordinated_billing_ramp",
    "phantom_servicing_hub",
    "network_beneficiary_dilution",
    "shared_official",
    "address_clustering",
    "phantom_servicing_spread",
}


def _extract_network_key(provider: dict) -> str:
    """Extract a network grouping key from a provider's FOF-related signals.

    Priority: authorized_official_name > servicing hub NPI > zip cluster > NPI.
    """
    for sig in provider["signals"]:
        ev = sig.get("evidence", {})
        if "authorized_official_name" in ev and ev["authorized_official_name"]:
            return f"official:{ev['authorized_official_name']}"
        if sig["signal_type"] == "phantom_servicing_hub":
            return f"hub:{provider['npi']}"
        if sig["signal_type"] == "address_clustering" and "zip_code" in ev:
            return f"zip:{ev['zip_code']}"
        if sig["signal_type"] == "burst_enrollment_network":
            return f"burst:{ev.get('taxonomy_code', '')}:{ev.get('state', '')}"
    return f"standalone:{provider['npi']}"


def generate_fof_report(report: dict) -> dict:
    """Extract Feeding Our Future-style network fraud findings from the main report.

    Groups providers into networks and ranks by combined overpayment.
    """
    providers = report.get("flagged_providers", [])

    # Filter to providers with at least one FOF-related signal
    fof_providers = []
    for p in providers:
        fof_signals = [s for s in p["signals"] if s["signal_type"] in FOF_SIGNAL_TYPES]
        if fof_signals:
            fof_providers.append(p)

    # Group into networks
    networks: dict[str, list[dict]] = {}
    for p in fof_providers:
        key = _extract_network_key(p)
        if key not in networks:
            networks[key] = []
        networks[key].append(p)

    # Build network summaries
    network_summaries = []
    for key, members in networks.items():
        npis = [m["npi"] for m in members]
        combined_paid = sum(m["total_paid_all_time"] for m in members)
        combined_overpayment = sum(m["estimated_overpayment_usd"] for m in members)
        states = sorted(set(m["state"] for m in members if m["state"] != "Unknown"))
        signal_types_present = sorted(set(
            s["signal_type"] for m in members for s in m["signals"]
            if s["signal_type"] in FOF_SIGNAL_TYPES
        ))
        max_risk = max((m.get("risk_score", {}).get("score", 0) for m in members), default=0)
        max_tier = "low"
        for m in members:
            t = m.get("risk_score", {}).get("tier", "low")
            if t == "critical":
                max_tier = "critical"
                break
            if t == "high" and max_tier not in ("critical",):
                max_tier = "high"
            if t == "medium" and max_tier == "low":
                max_tier = "medium"

        # Extract network label
        if key.startswith("official:"):
            network_label = f"Authorized Official: {key[9:]}"
            network_type = "shared_official_network"
        elif key.startswith("hub:"):
            network_label = f"Servicing Hub NPI: {key[4:]}"
            network_type = "phantom_servicing_hub"
        elif key.startswith("zip:"):
            network_label = f"Address Cluster: ZIP {key[4:]}"
            network_type = "address_cluster"
        elif key.startswith("burst:"):
            parts = key[6:].split(":")
            network_label = f"Burst Enrollment: {parts[0]} in {parts[1] if len(parts) > 1 else 'Unknown'}"
            network_type = "burst_enrollment"
        else:
            network_label = f"Provider: {key.split(':')[-1]}"
            network_type = "standalone"

        network_summaries.append({
            "network_key": key,
            "network_label": network_label,
            "network_type": network_type,
            "member_count": len(members),
            "member_npis": npis,
            "member_names": [m["provider_name"] for m in members],
            "states": states,
            "combined_total_paid": round(combined_paid, 2),
            "combined_estimated_overpayment": round(combined_overpayment, 2),
            "signal_types_detected": signal_types_present,
            "highest_risk_score": max_risk,
            "highest_risk_tier": max_tier,
            "members": members,
        })

    # Sort by combined overpayment descending
    network_summaries.sort(key=lambda n: n["combined_estimated_overpayment"], reverse=True)

    # ---------------------------------------------------------------------------
    # Actionability filter: reduce to cases HHS would realistically pursue.
    #
    # Criteria (a network must meet ALL):
    #   1. Risk tier: critical or high
    #   2. Estimated overpayment >= $500K (below that, enforcement cost > recovery)
    #   3. At least one of:
    #      a. Multi-member network (2+ providers — shows actual coordination)
    #      b. Solo provider with 2+ distinct FOF signal types AND >$5M overpayment
    #   4. Not COVID-dominated: <75% of member signals flagged covid_era
    #   5. At least 2 distinct FOF signal types across the network
    #      (single-signal networks are weaker evidence)
    #
    # Networks that fail are kept in a separate "below_threshold" summary count
    # so the user knows they exist but aren't cluttering the actionable report.
    # ---------------------------------------------------------------------------
    actionable = []
    below_threshold_count = 0
    below_threshold_overpayment = 0.0

    for n in network_summaries:
        # Criterion 1: risk tier
        if n["highest_risk_tier"] not in ("critical", "high"):
            below_threshold_count += 1
            below_threshold_overpayment += n["combined_estimated_overpayment"]
            continue

        # Criterion 2: minimum overpayment
        if n["combined_estimated_overpayment"] < 500_000:
            below_threshold_count += 1
            below_threshold_overpayment += n["combined_estimated_overpayment"]
            continue

        # Criterion 3: multi-member OR high-value solo with corroboration
        if n["member_count"] < 2:
            if not (len(n["signal_types_detected"]) >= 2
                    and n["combined_estimated_overpayment"] > 5_000_000):
                below_threshold_count += 1
                below_threshold_overpayment += n["combined_estimated_overpayment"]
                continue

        # Criterion 4: not COVID-dominated
        total_sigs = 0
        covid_sigs = 0
        for m in n["members"]:
            for s in m["signals"]:
                total_sigs += 1
                if s.get("evidence", {}).get("covid_era_flag"):
                    covid_sigs += 1
        if total_sigs > 0 and covid_sigs / total_sigs >= 0.75:
            below_threshold_count += 1
            below_threshold_overpayment += n["combined_estimated_overpayment"]
            continue

        # Criterion 5: 2+ distinct FOF signal types (corroboration)
        if len(n["signal_types_detected"]) < 2:
            below_threshold_count += 1
            below_threshold_overpayment += n["combined_estimated_overpayment"]
            continue

        actionable.append(n)

    network_summaries = actionable

    total_overpayment = sum(n["combined_estimated_overpayment"] for n in network_summaries)
    total_providers_in_networks = sum(n["member_count"] for n in network_summaries)

    fof_report = {
        "generated_at": report.get("generated_at", ""),
        "tool_version": report.get("tool_version", ""),
        "report_type": "feeding_our_future_network_analysis",
        "description": (
            "This report isolates coordinated group fraud networks most likely to be "
            "actioned on by HHS — multi-member networks with corroborating signal types, "
            "significant estimated overpayment (>$500K), and evidence not dominated by "
            "COVID-era billing patterns. Modeled on the Feeding Our Future prosecution: "
            "shell entities registered in bursts, synchronized billing ramps, phantom "
            "servicing relationships, and recycled beneficiary pools."
        ),
        "summary": {
            "total_networks_detected": len(network_summaries),
            "total_providers_in_networks": total_providers_in_networks,
            "total_estimated_network_overpayment": round(total_overpayment, 2),
            "networks_by_type": {},
            "tier_distribution": {"critical": 0, "high": 0, "medium": 0, "low": 0},
            "below_threshold_networks": below_threshold_count,
            "below_threshold_estimated_overpayment": round(below_threshold_overpayment, 2),
        },
        "networks": network_summaries,
    }

    # Count by network type and tier
    for n in network_summaries:
        ntype = n["network_type"]
        fof_report["summary"]["networks_by_type"][ntype] = (
            fof_report["summary"]["networks_by_type"].get(ntype, 0) + 1
        )
        fof_report["summary"]["tier_distribution"][n["highest_risk_tier"]] += 1

    return fof_report


def write_fof_report(report: dict, output_path: str) -> None:
    """Write the FOF network fraud report to JSON."""
    fof = generate_fof_report(report)
    with open(output_path, "w") as f:
        json.dump(fof, f, indent=2, default=str)

    print(f"\nFOF Network Report written to: {output_path}")
    print(f"  Actionable networks: {fof['summary']['total_networks_detected']}")
    print(f"  Providers in networks: {fof['summary']['total_providers_in_networks']}")
    print(f"  Est. network overpayment: ${fof['summary']['total_estimated_network_overpayment']:,.2f}")
    below = fof["summary"].get("below_threshold_networks", 0)
    below_op = fof["summary"].get("below_threshold_estimated_overpayment", 0)
    if below:
        print(f"  Below-threshold networks filtered: {below} (${below_op:,.2f})")
    by_type = fof["summary"]["networks_by_type"]
    for ntype, count in sorted(by_type.items(), key=lambda x: x[1], reverse=True):
        print(f"    {ntype}: {count}")


def write_fof_html_report(report: dict, output_path: str) -> None:
    """Write an HTML version of the FOF network fraud report."""
    fof = generate_fof_report(report)
    summary = fof["summary"]
    networks = fof["networks"]

    tier_colors = {"critical": "#dc2626", "high": "#ea580c", "medium": "#ca8a04", "low": "#16a34a"}

    lines = []
    lines.append("<!DOCTYPE html>")
    lines.append('<html lang="en"><head><meta charset="UTF-8">')
    lines.append('<meta name="viewport" content="width=device-width,initial-scale=1">')
    lines.append(f"<title>Network Fraud Report (FOF Pattern) — {_esc(fof.get('generated_at', ''))}</title>")
    lines.append("<style>")
    lines.append("""
body { font-family: -apple-system, BlinkMacSystemFont, 'Segoe UI', Roboto, sans-serif;
       max-width: 1200px; margin: 0 auto; padding: 20px; background: #f8fafc; color: #1e293b; }
h1 { color: #0f172a; border-bottom: 3px solid #dc2626; padding-bottom: 10px; }
h2 { color: #991b1b; margin-top: 30px; }
h3 { color: #334155; }
.summary-grid { display: grid; grid-template-columns: repeat(auto-fit, minmax(200px, 1fr)); gap: 16px; margin: 20px 0; }
.summary-card { background: white; border-radius: 8px; padding: 20px; box-shadow: 0 1px 3px rgba(0,0,0,0.1); }
.summary-card .label { font-size: 0.85em; color: #64748b; text-transform: uppercase; letter-spacing: 0.05em; }
.summary-card .value { font-size: 1.8em; font-weight: 700; color: #0f172a; margin-top: 4px; }
table { width: 100%; border-collapse: collapse; margin: 16px 0; background: white; border-radius: 8px; overflow: hidden;
        box-shadow: 0 1px 3px rgba(0,0,0,0.1); }
th { background: #991b1b; color: white; padding: 12px; text-align: left; font-size: 0.85em; text-transform: uppercase; }
td { padding: 10px 12px; border-bottom: 1px solid #e2e8f0; font-size: 0.9em; }
tr:hover { background: #fef2f2; }
.tier-badge { display: inline-block; padding: 2px 10px; border-radius: 12px; color: white;
              font-size: 0.8em; font-weight: 600; text-transform: uppercase; }
.network-card { background: white; border-radius: 8px; padding: 20px; margin: 16px 0;
                box-shadow: 0 1px 3px rgba(0,0,0,0.1); border-left: 4px solid #dc2626; }
.network-card.high { border-left-color: #ea580c; }
.network-card.medium { border-left-color: #ca8a04; }
.network-card.low { border-left-color: #16a34a; }
.signal-tag { display: inline-block; padding: 2px 8px; border-radius: 4px; margin: 2px;
              font-size: 0.8em; background: #fecaca; color: #991b1b; }
.member-table { font-size: 0.85em; margin-top: 10px; }
.member-table th { background: #374151; font-size: 0.8em; padding: 8px; }
.member-table td { padding: 6px 8px; }
.narrative { background: #fef2f2; border: 1px solid #fecaca; border-radius: 6px; padding: 14px; margin: 10px 0;
             line-height: 1.6; }
footer { margin-top: 40px; padding-top: 20px; border-top: 1px solid #e2e8f0; color: #94a3b8; font-size: 0.85em; }
""")
    lines.append("</style></head><body>")

    # Header
    lines.append("<h1>Coordinated Network Fraud Report</h1>")
    lines.append(f'<p style="color:#991b1b;font-weight:600">Feeding Our Future-Style Pattern Analysis</p>')
    lines.append(f"<p>Generated: {_esc(fof.get('generated_at', 'Unknown'))} | "
                 f"Tool Version: {_esc(fof.get('tool_version', ''))}</p>")
    lines.append(f'<div class="narrative"><p>{_esc(fof["description"])}</p></div>')

    # Summary cards
    lines.append("<h2>Summary</h2>")
    lines.append('<div class="summary-grid">')
    lines.append(f'<div class="summary-card"><div class="label">Fraud Networks</div>'
                 f'<div class="value">{summary["total_networks_detected"]:,}</div></div>')
    lines.append(f'<div class="summary-card"><div class="label">Providers Involved</div>'
                 f'<div class="value">{summary["total_providers_in_networks"]:,}</div></div>')
    lines.append(f'<div class="summary-card"><div class="label">Est. Overpayment</div>'
                 f'<div class="value" style="color:#dc2626">'
                 f'${summary["total_estimated_network_overpayment"]:,.0f}</div></div>')
    critical = summary["tier_distribution"].get("critical", 0)
    lines.append(f'<div class="summary-card"><div class="label">Critical Networks</div>'
                 f'<div class="value" style="color:#dc2626">{critical}</div></div>')
    lines.append('</div>')

    # Networks by type table
    lines.append("<h3>Networks by Type</h3>")
    lines.append("<table><tr><th>Network Type</th><th>Count</th></tr>")
    for ntype, count in sorted(summary["networks_by_type"].items(), key=lambda x: x[1], reverse=True):
        lines.append(f"<tr><td>{_esc(ntype)}</td><td>{count}</td></tr>")
    lines.append("</table>")

    # Top networks table
    lines.append("<h2>Top Networks by Estimated Overpayment</h2>")
    lines.append("<table><tr><th>#</th><th>Network</th><th>Members</th><th>States</th>"
                 "<th>Combined Billing</th><th>Est. Overpayment</th><th>Risk</th></tr>")
    for i, n in enumerate(networks[:100], 1):
        tier = n["highest_risk_tier"]
        color = tier_colors.get(tier, "#64748b")
        lines.append(
            f'<tr><td>{i}</td>'
            f'<td>{_esc(n["network_label"])}</td>'
            f'<td>{n["member_count"]}</td>'
            f'<td>{_esc(", ".join(n["states"][:5]))}</td>'
            f'<td>${n["combined_total_paid"]:,.0f}</td>'
            f'<td style="font-weight:600">${n["combined_estimated_overpayment"]:,.0f}</td>'
            f'<td><span class="tier-badge" style="background:{color}">{_esc(tier)}</span></td>'
            f'</tr>'
        )
    lines.append("</table>")

    # Detailed network cards
    lines.append("<h2>Network Details</h2>")
    for i, n in enumerate(networks[:500], 1):
        tier = n["highest_risk_tier"]
        color = tier_colors.get(tier, "#64748b")

        lines.append(f'<div class="network-card {_esc(tier)}">')
        lines.append(f'<h3>#{i}. {_esc(n["network_label"])}</h3>')
        lines.append(
            f'<p><strong>Type:</strong> {_esc(n["network_type"])} | '
            f'<strong>Members:</strong> {n["member_count"]} | '
            f'<strong>States:</strong> {_esc(", ".join(n["states"]))} | '
            f'<strong>Risk:</strong> '
            f'<span class="tier-badge" style="background:{color}">{_esc(tier)}</span></p>'
        )
        lines.append(
            f'<p><strong>Combined Billing:</strong> ${n["combined_total_paid"]:,.2f} | '
            f'<strong>Est. Overpayment:</strong> '
            f'<span style="color:#dc2626;font-weight:600">'
            f'${n["combined_estimated_overpayment"]:,.2f}</span></p>'
        )

        # Signal types
        lines.append("<p><strong>Signals:</strong> ")
        for st in n["signal_types_detected"]:
            lines.append(f'<span class="signal-tag">{_esc(st)}</span>')
        lines.append("</p>")

        # Member table
        lines.append('<table class="member-table"><tr><th>NPI</th><th>Name</th>'
                     '<th>State</th><th>Billing</th><th>Overpayment</th><th>Risk Score</th></tr>')
        for m in n["members"][:50]:
            mscore = m.get("risk_score", {}).get("score", 0)
            lines.append(
                f'<tr><td>{_esc(m["npi"])}</td>'
                f'<td>{_esc(m["provider_name"])}</td>'
                f'<td>{_esc(m["state"])}</td>'
                f'<td>${m["total_paid_all_time"]:,.0f}</td>'
                f'<td>${m["estimated_overpayment_usd"]:,.0f}</td>'
                f'<td>{mscore}</td></tr>'
            )
        if len(n["members"]) > 50:
            lines.append(f'<tr><td colspan="6"><em>... and {len(n["members"]) - 50} more members</em></td></tr>')
        lines.append("</table>")

        # Case narratives for top members
        top_members = sorted(n["members"], key=lambda m: m.get("risk_score", {}).get("score", 0), reverse=True)[:3]
        for m in top_members:
            narrative = m.get("case_narrative", "")
            if narrative:
                lines.append(f'<div class="narrative"><strong>{_esc(m["provider_name"])}:</strong> '
                             f'{_esc(narrative)}</div>')

        lines.append("</div>")

    if len(networks) > 500:
        lines.append(f"<p><em>Showing top 500 of {len(networks)} networks. "
                     f"See JSON report for complete data.</em></p>")

    # Footer
    lines.append("<footer>")
    lines.append(f"<p>Medicaid Fraud Signal Detection Engine v{_esc(fof.get('tool_version', ''))} | "
                 f"Network Fraud Analysis | Generated {_esc(fof.get('generated_at', ''))} | "
                 f"Data sources: HHS STOP Medicaid Spending, OIG LEIE, CMS NPPES</p>")
    lines.append("</footer></body></html>")

    html_content = "\n".join(lines)
    with open(output_path, "w") as f:
        f.write(html_content)
    print(f"FOF HTML report written to: {output_path}")
