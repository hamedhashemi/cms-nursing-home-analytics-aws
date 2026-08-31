-- =========================================================
-- Facility Quality Measures
-- =========================================================

CREATE OR REPLACE VIEW healthcare_metrics.vw_facility_quality AS
SELECT
    provider_id,

    MAX(
        CASE
            WHEN measure_code = 521
            THEN adjusted_score
        END
    ) AS rehospitalization_rate,

    MAX(
        CASE
            WHEN measure_code = 522
            THEN adjusted_score
        END
    ) AS short_stay_ed_rate,

    MAX(
        CASE
            WHEN measure_code = 551
            THEN adjusted_score
        END
    ) AS hospitalizations_per_1000,

    MAX(
        CASE
            WHEN measure_code = 552
            THEN adjusted_score
        END
    ) AS long_stay_ed_visits_per_1000

FROM healthcare_metrics.quality_claims

GROUP BY provider_id;


-- =========================================================
-- Unified Facility Analytics View
-- =========================================================

CREATE OR REPLACE VIEW healthcare_metrics.vw_facility_analytics AS
SELECT
    f.provnum,
    f.provider_name,
    f.state,
    f.city,
    f.county,
    f.ownership_type,
    f.certified_beds,
    f.overall_rating,
    f.health_inspection_rating,
    f.qm_rating,
    f.staffing_rating,
    f.total_resident_days,
    f.rn_hprd,
    f.lpn_hprd,
    f.cna_hprd,
    f.total_direct_hprd,
    f.contract_dependency_pct,
    f.reporting_days,

    q.rehospitalization_rate,
    q.short_stay_ed_rate,
    q.hospitalizations_per_1000,
    q.long_stay_ed_visits_per_1000

FROM healthcare_metrics.vw_facility_summary f

LEFT JOIN healthcare_metrics.vw_facility_quality q
    ON f.provnum = q.provider_id;