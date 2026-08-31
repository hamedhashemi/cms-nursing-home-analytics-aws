CREATE OR REPLACE VIEW healthcare_metrics.vw_staffing_quality_correlations AS
SELECT
    corr(
        total_direct_hprd,
        rehospitalization_rate
    ) AS hprd_vs_rehospitalization,

    corr(
        total_direct_hprd,
        short_stay_ed_rate
    ) AS hprd_vs_short_stay_ed,

    corr(
        total_direct_hprd,
        hospitalizations_per_1000
    ) AS hprd_vs_hospitalizations,

    corr(
        total_direct_hprd,
        long_stay_ed_visits_per_1000
    ) AS hprd_vs_long_stay_ed,

    corr(
        total_direct_hprd,
        overall_rating
    ) AS hprd_vs_overall_rating,

    corr(
        total_direct_hprd,
        staffing_rating
    ) AS hprd_vs_staffing_rating,

    corr(
        rn_hprd,
        rehospitalization_rate
    ) AS rn_vs_rehospitalization,

    corr(
        rn_hprd,
        short_stay_ed_rate
    ) AS rn_vs_short_stay_ed,

    corr(
        rn_hprd,
        hospitalizations_per_1000
    ) AS rn_vs_hospitalizations,

    corr(
        rn_hprd,
        long_stay_ed_visits_per_1000
    ) AS rn_vs_long_stay_ed

FROM healthcare_metrics.vw_facility_analytics;