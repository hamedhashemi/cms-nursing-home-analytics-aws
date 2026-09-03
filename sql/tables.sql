-- =========================================================
-- Athena External Tables
-- =========================================================

CREATE EXTERNAL TABLE IF NOT EXISTS healthcare_metrics.facility_metrics (
    provnum string,
    provname string,
    city string,
    county_name string,
    county_fips string,
    cy_qtr string,
    workdate date,
    mdscensus double,

    hrs_rn double,
    hrs_lpn double,
    hrs_cna double,

    total_direct_hours double,
    contract_direct_hours double,

    rn_hprd double,
    lpn_hprd double,
    cna_hprd double,
    total_direct_hprd double,
    contract_dependency_pct double,

    provider_name string,
    provider_address string,
    provider_city string,
    provider_state string,
    provider_zip string,
    provider_county string,
    provider_ownership_type string,
    provider_certified_beds double,
    provider_avg_residents_per_day double,
    provider_type string,
    provider_overall_rating double,
    provider_health_inspection_rating double,
    provider_qm_rating double,
    provider_staffing_rating double,

    etl_processed_at timestamp
)
PARTITIONED BY (
    state string
)
STORED AS PARQUET
LOCATION 's3://healthcare-metrics-data/curated/facility_metrics/';


CREATE EXTERNAL TABLE IF NOT EXISTS healthcare_metrics.quality_claims (
    provider_id string,
    measure_code integer,
    measure_description string,
    resident_type string,
    adjusted_score double,
    observed_score double,
    expected_score double,
    used_in_five_star string,
    measure_period string,
    processing_date date,
    etl_processed_at timestamp
)
STORED AS PARQUET
LOCATION 's3://healthcare-metrics-data/curated/quality_claims/';