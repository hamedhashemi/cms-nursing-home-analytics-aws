# Pipeline Execution

## Workflow

The pipeline is orchestrated with AWS Glue Workflow.

Execution flow:

```text
Scheduled Trigger
      |
      v
Google Drive Ingestion
      |
      +-----------------------+
      |                       |
      v                       v
PBJ ETL              Provider Info ETL
      |                       |
      +-----------+-----------+
                  |
                  v
         Facility Metrics ETL

Quality Claims ETL runs as a parallel transformation branch.



Ingestion

The Glue Python Shell job:

Connects to Google Drive
Detects new or modified files
Downloads source files
Writes them to S3 raw storage
Updates control/ingestion_state.json
Transformation Jobs
PBJ ETL

Reads PBJ CSV data and creates staffing metrics.

Output:

s3://healthcare-metrics-data/curated/pbj_metrics/
Provider Information ETL

Creates a facility-level provider dataset.

Output:

s3://healthcare-metrics-data/curated/provider_info/
Quality Claims ETL

Standardizes claims-based quality measures.

Output:

s3://healthcare-metrics-data/curated/quality_claims/
Facility Metrics ETL

Joins PBJ staffing data with provider information.

Output:

s3://healthcare-metrics-data/curated/facility_metrics/

The output is stored as Parquet and partitioned by state.

Athena

Athena tables and views expose curated datasets for analytics.

Main views:

vw_facility_summary
vw_state_summary
vw_state_facility_ratings
vw_facility_quality
vw_facility_analytics
vw_staffing_quality_correlations
Dashboard

Run locally with:

python -m streamlit run dashboard/app.py

Streamlit queries Athena using boto3 and caches query results to reduce repeated scans.

Validation

A successful pipeline run should confirm:

Ingestion job succeeds
PBJ ETL succeeds
Provider ETL succeeds
Quality Claims ETL succeeds
Facility Metrics ETL succeeds
Athena tables remain queryable
Dashboard loads successfully

