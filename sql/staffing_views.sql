-- =========================================================
-- Facility-Level Staffing Summary
-- =========================================================

CREATE OR REPLACE VIEW healthcare_metrics.vw_facility_summary AS
SELECT
    provnum,
    MAX(provider_name) AS provider_name,
    MAX(state) AS state,
    MAX(provider_city) AS city,
    MAX(provider_county) AS county,
    MAX(provider_ownership_type) AS ownership_type,
    MAX(provider_certified_beds) AS certified_beds,
    MAX(provider_overall_rating) AS overall_rating,
    MAX(provider_health_inspection_rating) AS health_inspection_rating,
    MAX(provider_qm_rating) AS qm_rating,
    MAX(provider_staffing_rating) AS staffing_rating,

    SUM(mdscensus) AS total_resident_days,

    SUM(hrs_rn)
        / NULLIF(SUM(mdscensus), 0)
        AS rn_hprd,

    SUM(hrs_lpn)
        / NULLIF(SUM(mdscensus), 0)
        AS lpn_hprd,

    SUM(hrs_cna)
        / NULLIF(SUM(mdscensus), 0)
        AS cna_hprd,

    SUM(total_direct_hours)
        / NULLIF(SUM(mdscensus), 0)
        AS total_direct_hprd,

    100.0 * SUM(contract_direct_hours)
        / NULLIF(SUM(total_direct_hours), 0)
        AS contract_dependency_pct,

    MIN(workdate) AS first_date,
    MAX(workdate) AS last_date,
    COUNT(DISTINCT workdate) AS reporting_days

FROM healthcare_metrics.facility_metrics

GROUP BY provnum;


-- =========================================================
-- State-Level Staffing Summary
-- =========================================================

CREATE OR REPLACE VIEW healthcare_metrics.vw_state_summary AS
SELECT
    state,

    COUNT(DISTINCT provnum)
        AS total_facilities,

    SUM(mdscensus)
        AS total_resident_days,

    SUM(hrs_rn)
        / NULLIF(SUM(mdscensus), 0)
        AS rn_hprd,

    SUM(hrs_lpn)
        / NULLIF(SUM(mdscensus), 0)
        AS lpn_hprd,

    SUM(hrs_cna)
        / NULLIF(SUM(mdscensus), 0)
        AS cna_hprd,

    SUM(total_direct_hours)
        / NULLIF(SUM(mdscensus), 0)
        AS total_direct_hprd,

    100.0 * SUM(contract_direct_hours)
        / NULLIF(SUM(total_direct_hours), 0)
        AS contract_dependency_pct,

    AVG(provider_overall_rating)
        AS avg_overall_rating,

    AVG(provider_health_inspection_rating)
        AS avg_health_inspection_rating,

    AVG(provider_qm_rating)
        AS avg_qm_rating,

    AVG(provider_staffing_rating)
        AS avg_staffing_rating

FROM healthcare_metrics.facility_metrics

GROUP BY state;


-- =========================================================
-- State-Level Facility-Weighted CMS Ratings
-- =========================================================

CREATE OR REPLACE VIEW healthcare_metrics.vw_state_facility_ratings AS
SELECT
    state,
    COUNT(*) AS total_facilities,
    AVG(overall_rating)
        AS avg_overall_rating,
    AVG(health_inspection_rating)
        AS avg_health_inspection_rating,
    AVG(qm_rating)
        AS avg_qm_rating,
    AVG(staffing_rating)
        AS avg_staffing_rating

FROM healthcare_metrics.vw_facility_summary

GROUP BY state;