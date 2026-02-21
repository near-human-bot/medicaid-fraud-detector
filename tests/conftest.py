"""Shared test fixtures for fraud signal tests."""

import os
import pytest
import duckdb


@pytest.fixture
def con():
    """Create a DuckDB connection with synthetic test data."""
    c = duckdb.connect(":memory:")

    # Create spending table with synthetic data
    c.execute("""
        CREATE TABLE spending AS
        SELECT * FROM (VALUES
            -- Normal provider
            ('1111111111', '1111111111', 'G0151', '2023-01-01'::DATE, 10, 50, 5000.0),
            ('1111111111', '1111111111', 'G0151', '2023-02-01'::DATE, 12, 55, 5500.0),
            ('1111111111', '1111111111', '99213', '2023-03-01'::DATE, 15, 60, 6000.0),

            -- Excluded provider (NPI matches LEIE)
            ('2222222222', '2222222222', '99213', '2023-01-01'::DATE, 20, 100, 10000.0),
            ('2222222222', '2222222222', '99213', '2023-06-01'::DATE, 25, 120, 12000.0),
            ('2222222222', '2222222222', '99213', '2023-12-01'::DATE, 30, 150, 15000.0),

            -- High-volume outlier (taxonomy 207Q00000X, state CA)
            ('3333333333', '3333333333', '99213', '2023-01-01'::DATE, 100, 500, 500000.0),
            ('3333333333', '3333333333', '99213', '2023-06-01'::DATE, 120, 600, 600000.0),

            -- Normal peer provider (same taxonomy+state)
            ('3333333334', '3333333334', '99213', '2023-01-01'::DATE, 10, 50, 5000.0),
            ('3333333335', '3333333335', '99213', '2023-01-01'::DATE, 12, 55, 6000.0),
            ('3333333336', '3333333336', '99213', '2023-01-01'::DATE, 11, 52, 5500.0),
            ('3333333337', '3333333337', '99213', '2023-01-01'::DATE, 10, 48, 4800.0),
            ('3333333338', '3333333338', '99213', '2023-01-01'::DATE, 9, 45, 4500.0),

            -- Rapid escalation provider (new entity, billing grows fast)
            ('4444444444', '4444444444', '99213', '2023-01-01'::DATE, 5, 20, 1000.0),
            ('4444444444', '4444444444', '99213', '2023-02-01'::DATE, 10, 40, 2000.0),
            ('4444444444', '4444444444', '99213', '2023-03-01'::DATE, 20, 100, 10000.0),
            ('4444444444', '4444444444', '99213', '2023-04-01'::DATE, 50, 300, 50000.0),
            ('4444444444', '4444444444', '99213', '2023-05-01'::DATE, 100, 700, 150000.0),
            ('4444444444', '4444444444', '99213', '2023-06-01'::DATE, 200, 1500, 400000.0),

            -- Workforce impossibility org (huge claims in one month)
            ('5555555555', '5555555555', '99213', '2023-01-01'::DATE, 50, 200, 20000.0),
            ('5555555555', '5555555555', '99213', '2023-06-01'::DATE, 100, 5000, 500000.0),

            -- Shared official NPIs (5 NPIs controlled by same person)
            ('6666666661', '6666666661', '99213', '2023-01-01'::DATE, 50, 200, 300000.0),
            ('6666666662', '6666666662', '99213', '2023-01-01'::DATE, 40, 180, 250000.0),
            ('6666666663', '6666666663', '99213', '2023-01-01'::DATE, 30, 150, 200000.0),
            ('6666666664', '6666666664', '99213', '2023-01-01'::DATE, 20, 100, 150000.0),
            ('6666666665', '6666666665', '99213', '2023-01-01'::DATE, 10, 80, 150000.0),

            -- Geographic implausibility: home health, many claims, few beneficiaries
            ('7777777777', '7777777777', 'G0151', '2023-06-01'::DATE, 2, 500, 50000.0),
            ('7777777777', '7777777777', 'T1019', '2023-06-01'::DATE, 3, 200, 20000.0),

            -- Address clustering: 10+ NPIs in same zip with high billing
            ('8800000001', '8800000001', '99213', '2023-01-01'::DATE, 20, 100, 600000.0),
            ('8800000002', '8800000002', '99213', '2023-01-01'::DATE, 18, 90, 550000.0),
            ('8800000003', '8800000003', '99213', '2023-01-01'::DATE, 15, 80, 520000.0),
            ('8800000004', '8800000004', '99213', '2023-01-01'::DATE, 12, 70, 510000.0),
            ('8800000005', '8800000005', '99213', '2023-01-01'::DATE, 10, 60, 500000.0),
            ('8800000006', '8800000006', '99213', '2023-01-01'::DATE, 10, 55, 490000.0),
            ('8800000007', '8800000007', '99213', '2023-01-01'::DATE, 10, 50, 480000.0),
            ('8800000008', '8800000008', '99213', '2023-01-01'::DATE, 10, 45, 470000.0),
            ('8800000009', '8800000009', '99213', '2023-01-01'::DATE, 10, 40, 460000.0),
            ('8800000010', '8800000010', '99213', '2023-01-01'::DATE, 10, 35, 450000.0),

            -- Upcoding provider: bills almost all high-level E&M codes
            ('9900000001', '9900000001', '99215', '2023-01-01'::DATE, 30, 90, 90000.0),
            ('9900000001', '9900000001', '99205', '2023-02-01'::DATE, 20, 60, 60000.0),
            ('9900000001', '9900000001', '99213', '2023-03-01'::DATE, 10, 10, 5000.0),
            -- Normal E&M peers (same taxonomy+state TX, 208D00000X)
            ('9900000002', '9900000002', '99213', '2023-01-01'::DATE, 30, 80, 40000.0),
            ('9900000002', '9900000002', '99215', '2023-02-01'::DATE, 5, 10, 10000.0),
            ('9900000003', '9900000003', '99213', '2023-01-01'::DATE, 25, 70, 35000.0),
            ('9900000003', '9900000003', '99215', '2023-02-01'::DATE, 3, 5, 5000.0),
            ('9900000004', '9900000004', '99213', '2023-01-01'::DATE, 20, 60, 30000.0),

            -- Concurrent billing provider: individual billing across 6 different servicing NPIs in different states
            ('9800000001', '9810000001', '99213', '2023-06-01'::DATE, 10, 50, 5000.0),
            ('9800000001', '9810000002', '99213', '2023-06-01'::DATE, 10, 50, 5000.0),
            ('9800000001', '9810000003', '99213', '2023-06-01'::DATE, 10, 50, 5000.0),
            ('9800000001', '9810000004', '99213', '2023-06-01'::DATE, 10, 50, 5000.0),
            ('9800000001', '9810000005', '99213', '2023-06-01'::DATE, 10, 50, 5000.0),
            ('9800000001', '9810000006', '99213', '2023-06-01'::DATE, 10, 50, 5000.0),

            -- Multi-signal provider: hits both outlier and rapid escalation
            ('9700000001', '9700000001', '99213', '2023-01-01'::DATE, 5, 20, 800.0),
            ('9700000001', '9700000001', '99213', '2023-02-01'::DATE, 10, 50, 2000.0),
            ('9700000001', '9700000001', '99213', '2023-03-01'::DATE, 30, 200, 20000.0),
            ('9700000001', '9700000001', '99213', '2023-04-01'::DATE, 60, 500, 80000.0),
            ('9700000001', '9700000001', '99213', '2023-05-01'::DATE, 100, 900, 200000.0),
            ('9700000001', '9700000001', '99213', '2023-06-01'::DATE, 200, 2000, 500000.0)

        ) AS t(billing_npi, servicing_npi, hcpcs_code, claim_month, unique_beneficiaries, total_claims, total_paid)
    """)

    # Create a view alias for spending
    c.execute("CREATE VIEW spending_view AS SELECT * FROM spending")

    # Create LEIE table
    c.execute("""
        CREATE TABLE leie AS
        SELECT * FROM (VALUES
            ('DOE', 'JOHN', '', '', '', '', '', '2222222222', '', '', '', 'NY', '', '1422a4', '20220101', '', '', '')
        ) AS t(lastname, firstname, midname, busname, general, specialty, upin, npi, dob, address, city, state, zip, excl_type, excl_date_raw, rein_date_raw, waiverdate, wvrstate)
    """)
    # Add parsed dates
    c.execute("""
        ALTER TABLE leie ADD COLUMN excl_date DATE;
        UPDATE leie SET excl_date = TRY_STRPTIME(excl_date_raw, '%Y%m%d');
        ALTER TABLE leie ADD COLUMN rein_date DATE;
        UPDATE leie SET rein_date = CASE WHEN LENGTH(TRIM(rein_date_raw)) = 8 THEN TRY_STRPTIME(rein_date_raw, '%Y%m%d') ELSE NULL END;
    """)

    # Create NPPES table
    c.execute("""
        CREATE TABLE nppes AS
        SELECT * FROM (VALUES
            ('1111111111', '1', NULL, 'Smith', 'Jane', 'CA', '90210', '207Q00000X', '2015-01-15'::DATE, NULL, NULL),
            ('2222222222', '1', NULL, 'Doe', 'John', 'NY', '10001', '207Q00000X', '2018-01-01'::DATE, NULL, NULL),
            ('3333333333', '1', NULL, 'Mega', 'Provider', 'CA', '90210', '207Q00000X', '2010-01-01'::DATE, NULL, NULL),
            ('3333333334', '1', NULL, 'Normal1', 'Doc', 'CA', '90211', '207Q00000X', '2010-01-01'::DATE, NULL, NULL),
            ('3333333335', '1', NULL, 'Normal2', 'Doc', 'CA', '90212', '207Q00000X', '2010-01-01'::DATE, NULL, NULL),
            ('3333333336', '1', NULL, 'Normal3', 'Doc', 'CA', '90213', '207Q00000X', '2010-01-01'::DATE, NULL, NULL),
            ('3333333337', '1', NULL, 'Normal4', 'Doc', 'CA', '90214', '207Q00000X', '2010-01-01'::DATE, NULL, NULL),
            ('3333333338', '1', NULL, 'Normal5', 'Doc', 'CA', '90215', '207Q00000X', '2010-01-01'::DATE, NULL, NULL),
            ('4444444444', '1', NULL, 'Fast', 'Grower', 'TX', '75001', '208000000X', '2022-11-01'::DATE, NULL, NULL),
            ('5555555555', '2', 'MegaCorp Health', NULL, NULL, 'FL', '33101', '251S00000X', '2015-01-01'::DATE, NULL, NULL),
            ('6666666661', '2', 'Shell Corp 1', NULL, NULL, 'NJ', '07001', '261QM1200X', '2018-01-01'::DATE, 'SMITH', 'ROBERT'),
            ('6666666662', '2', 'Shell Corp 2', NULL, NULL, 'NJ', '07002', '261QM1200X', '2018-06-01'::DATE, 'SMITH', 'ROBERT'),
            ('6666666663', '2', 'Shell Corp 3', NULL, NULL, 'NJ', '07003', '261QM1200X', '2019-01-01'::DATE, 'SMITH', 'ROBERT'),
            ('6666666664', '2', 'Shell Corp 4', NULL, NULL, 'NJ', '07004', '261QM1200X', '2019-06-01'::DATE, 'SMITH', 'ROBERT'),
            ('6666666665', '2', 'Shell Corp 5', NULL, NULL, 'NJ', '07005', '261QM1200X', '2020-01-01'::DATE, 'SMITH', 'ROBERT'),
            ('7777777777', '2', 'Home Health LLC', NULL, NULL, 'PA', '19101', '251E00000X', '2016-01-01'::DATE, NULL, NULL),
            -- Address clustering: 10 providers at same zip 11111
            ('8800000001', '2', 'Cluster Corp 1', NULL, NULL, 'NY', '11111', '207Q00000X', '2017-01-01'::DATE, NULL, NULL),
            ('8800000002', '2', 'Cluster Corp 2', NULL, NULL, 'NY', '11111', '207Q00000X', '2017-02-01'::DATE, NULL, NULL),
            ('8800000003', '2', 'Cluster Corp 3', NULL, NULL, 'NY', '11111', '207Q00000X', '2017-03-01'::DATE, NULL, NULL),
            ('8800000004', '2', 'Cluster Corp 4', NULL, NULL, 'NY', '11111', '207Q00000X', '2017-04-01'::DATE, NULL, NULL),
            ('8800000005', '2', 'Cluster Corp 5', NULL, NULL, 'NY', '11111', '207Q00000X', '2017-05-01'::DATE, NULL, NULL),
            ('8800000006', '2', 'Cluster Corp 6', NULL, NULL, 'NY', '11111', '207Q00000X', '2017-06-01'::DATE, NULL, NULL),
            ('8800000007', '2', 'Cluster Corp 7', NULL, NULL, 'NY', '11111', '207Q00000X', '2017-07-01'::DATE, NULL, NULL),
            ('8800000008', '2', 'Cluster Corp 8', NULL, NULL, 'NY', '11111', '207Q00000X', '2017-08-01'::DATE, NULL, NULL),
            ('8800000009', '2', 'Cluster Corp 9', NULL, NULL, 'NY', '11111', '207Q00000X', '2017-09-01'::DATE, NULL, NULL),
            ('8800000010', '2', 'Cluster Corp 10', NULL, NULL, 'NY', '11111', '207Q00000X', '2017-10-01'::DATE, NULL, NULL),
            -- Upcoding provider + peers (taxonomy 208D00000X, state TX)
            ('9900000001', '1', NULL, 'Upcoder', 'Max', 'TX', '77001', '208D00000X', '2015-01-01'::DATE, NULL, NULL),
            ('9900000002', '1', NULL, 'Normal6', 'Doc', 'TX', '77002', '208D00000X', '2015-01-01'::DATE, NULL, NULL),
            ('9900000003', '1', NULL, 'Normal7', 'Doc', 'TX', '77003', '208D00000X', '2015-01-01'::DATE, NULL, NULL),
            ('9900000004', '1', NULL, 'Normal8', 'Doc', 'TX', '77004', '208D00000X', '2015-01-01'::DATE, NULL, NULL),
            -- Concurrent billing: individual provider + 6 servicing NPIs in different states
            ('9800000001', '1', NULL, 'Multi', 'State', 'NY', '10001', '207Q00000X', '2015-01-01'::DATE, NULL, NULL),
            ('9810000001', '1', NULL, 'Serv1', 'Doc', 'NY', '10001', '207Q00000X', '2015-01-01'::DATE, NULL, NULL),
            ('9810000002', '1', NULL, 'Serv2', 'Doc', 'CA', '90001', '207Q00000X', '2015-01-01'::DATE, NULL, NULL),
            ('9810000003', '1', NULL, 'Serv3', 'Doc', 'TX', '75001', '207Q00000X', '2015-01-01'::DATE, NULL, NULL),
            ('9810000004', '1', NULL, 'Serv4', 'Doc', 'FL', '33001', '207Q00000X', '2015-01-01'::DATE, NULL, NULL),
            ('9810000005', '1', NULL, 'Serv5', 'Doc', 'IL', '60001', '207Q00000X', '2015-01-01'::DATE, NULL, NULL),
            ('9810000006', '1', NULL, 'Serv6', 'Doc', 'PA', '19001', '207Q00000X', '2015-01-01'::DATE, NULL, NULL),
            -- Multi-signal provider (rapidly escalating + potentially outlier)
            ('9700000001', '1', NULL, 'Multi', 'Signal', 'CA', '90210', '207Q00000X', '2022-11-01'::DATE, NULL, NULL)
        ) AS t(npi, entity_type_code, org_name, last_name, first_name, state, zip_code, taxonomy_code, enumeration_date, auth_official_last, auth_official_first)
    """)

    return c
