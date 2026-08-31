# Architecture Design

## 1. Overview

This project implements an AWS-based batch data engineering platform for analyzing U.S. nursing-home staffing, facility characteristics, CMS ratings, and claims-based quality outcomes.

The architecture was designed around four principles:

1. Serverless or managed AWS services where practical
2. Incremental source ingestion
3. Separation of raw and curated data layers
4. Minimal infrastructure complexity for a batch analytical workload

The final architecture is:

```text
Google Drive
      |
      v
AWS Glue Workflow
      |
      v
Glue Python Shell Ingestion
      |
      v
Amazon S3 Raw / Bronze
      |
      v
AWS Glue Spark ETL
      |
      v
Amazon S3 Curated / Silver
      |
      v
Amazon Athena
      |
      v
Streamlit Dashboard



Supporting AWS services include AWS Secrets Manager, AWS Glue Data Catalog, and Amazon CloudWatch.

2. Source Layer

The source data consists of CMS nursing-home datasets distributed through Google Drive.

The primary dataset is the CMS Payroll-Based Journal (PBJ) Daily Nurse Staffing dataset for Q2 2024.

Supporting datasets include:

CMS Nursing Home Provider Information
CMS Claims-Based Quality Measures

The source datasets have different grains:

Dataset	Grain
PBJ Daily Nurse Staffing	Facility + Work Date
Provider Information	Facility
Claims Quality Measures	Facility + Measure Code

This difference in grain is handled during the transformation and analytical modeling stages.

3. Orchestration Layer

AWS Glue Workflow coordinates ingestion and transformation jobs.

The workflow begins with the Google Drive ingestion job.

After successful ingestion, transformation jobs process the required source datasets.

Conceptually:

Google Drive Ingestion
        |
        +---------------------+
        |                     |
        v                     v
     PBJ ETL          Provider Info ETL
        |                     |
        +----------+----------+
                   |
                   v
          Facility Metrics ETL

Quality Claims ETL runs as an independent transformation branch
after ingestion.

PBJ ETL and Provider Information ETL can execute in parallel.

Facility Metrics ETL executes after both required upstream transformations have completed successfully.

Quality Claims ETL does not need to block Facility Metrics ETL because quality measures are combined with facility staffing metrics in the Athena analytical layer.

AWS Glue scheduling is used directly rather than introducing a separate EventBridge/Lambda orchestration layer.

4. Incremental Ingestion

The ingestion process is implemented as an AWS Glue Python Shell job.

Google OAuth credentials are retrieved at runtime from AWS Secrets Manager.

The ingestion process:

Connects to the Google Drive API.
Lists source files in configured Drive folders.
Retrieves file IDs and modification timestamps.
Compares source metadata against previously processed state.
Downloads new or modified files.
Writes files to the S3 raw layer.
Updates ingestion state after successful processing.

Incremental state is stored in:

s3://healthcare-metrics-data/control/ingestion_state.json

Using both Google Drive file ID and modification timestamp allows the pipeline to skip unchanged source files.

5. Raw / Bronze Storage

Amazon S3 is the system of record for ingested source data.

The major raw prefixes are:

s3://healthcare-metrics-data/raw/pbj/
s3://healthcare-metrics-data/raw/supporting/

Files in the raw layer remain close to their original source representation.

This allows transformation jobs to be rerun without downloading the source data again.

6. Transformation Layer

AWS Glue Spark jobs perform transformation and validation.

PBJ ETL

The PBJ transformation:

Removes invalid or empty records
Standardizes provider identifiers
Converts work dates
Converts staffing fields to numeric types
Validates resident census
Detects negative staffing hours
Calculates direct-care staffing hours
Calculates contract staffing hours
Calculates HPRD metrics
Calculates contract dependency

Primary derived metrics include:

RN HPRD
LPN HPRD
CNA HPRD
Total Direct HPRD
Contract Dependency %
Provider Information ETL

Provider information is transformed into a facility-level dataset containing:

CMS Certification Number
Provider name
Location
Ownership type
Certified beds
Overall CMS rating
Health inspection rating
Quality measure rating
Staffing rating
Quality Claims ETL

The claims transformation creates a standardized facility-measure dataset containing:

Provider ID
Measure code
Measure description
Resident type
Adjusted score
Observed score
Expected score
Measure period
Facility Metrics ETL

Facility Metrics ETL enriches PBJ daily staffing records with provider attributes.

The join uses:

PBJ PROVNUM
      =
Provider Information CCN

A left join is used so that valid PBJ staffing records are retained even when provider enrichment is unavailable.

Observed join coverage was approximately 99.88%.

7. Curated / Silver Storage

Transformed datasets are written to Amazon S3 in Apache Parquet format.

Major curated datasets include:

curated/pbj_metrics/
curated/provider_info/
curated/quality_claims/
curated/facility_metrics/

Parquet was selected because it provides:

Columnar storage
Reduced storage footprint
Efficient Athena column projection
Reduced query scan volume
Improved analytical query performance

The facility metrics dataset is partitioned by state.

Example:

curated/facility_metrics/state=FL/
curated/facility_metrics/state=VA/

This enables Athena partition pruning for state-specific dashboard queries.

8. Query and Semantic Layer

Amazon Athena provides serverless SQL access to curated Parquet datasets.

AWS Glue Data Catalog stores table metadata.

Athena views provide a semantic layer above the physical datasets.

Important views include:

vw_facility_summary

Aggregates daily PBJ records to one row per facility.

vw_state_summary

Provides state-level staffing metrics.

vw_state_facility_ratings

Calculates facility-weighted state CMS ratings.

vw_facility_quality

Pivots CMS quality measure codes into facility-level outcome columns.

vw_facility_analytics

Combines staffing, provider, rating, and quality information into a unified facility analytical view.

vw_staffing_quality_correlations

Calculates selected Pearson correlations between staffing metrics, CMS ratings, and utilization outcomes.

9. Presentation Layer

Streamlit provides the analytical dashboard.

The application queries Athena through boto3.

Dashboard capabilities include:

National staffing overview
National quality outcomes
State staffing analysis
State comparison
Facility rankings
Facility explorer
CMS ratings
Quality outcomes
Daily staffing trends
Staffing-group comparisons
Staffing-quality correlation summaries

Athena query results are cached in Streamlit to reduce repeated query execution.

10. Security

Security controls include:

Secrets Management

Google OAuth credentials are stored in AWS Secrets Manager rather than source code.

IAM

AWS Glue jobs use a dedicated IAM role with access limited to required:

S3 locations
Secrets Manager secret
Glue execution capabilities

The local Streamlit application uses an IAM identity with access limited to:

Athena query execution
Glue Data Catalog read access
Curated S3 data
Athena result storage
S3

S3 Block Public Access is enabled.

Source Control

Credential files, OAuth tokens, virtual environments, raw datasets, and local development files are excluded from Git.

11. Monitoring

AWS Glue execution logs are available through Amazon CloudWatch.

Glue Workflow provides job-level execution status and dependency visibility.

Failures in ingestion or transformation jobs prevent dependent workflow stages from proceeding.

12. Architecture Decisions
Why Athena Instead of Redshift?

The workload is:

Batch-oriented
Analytical
Relatively low-frequency
Based on curated S3 datasets

Athena provides serverless querying without requiring a continuously provisioned data warehouse.

For this workload, adding Redshift would increase infrastructure complexity and cost without providing a necessary capability.

Why Glue Workflow Instead of Lambda + EventBridge?

Glue Workflow already supports the required scheduling and job dependency orchestration.

Adding Lambda and EventBridge would introduce additional services without a corresponding functional requirement.

Why S3 Instead of DynamoDB for Ingestion State?

The ingestion state is small and updated only during batch ingestion.

A JSON state object in S3 is sufficient for the required incremental-processing state.

DynamoDB would be justified for higher-frequency or concurrent state access, which this workload does not require.

Why No Glue Crawler?

The curated analytical schemas are known and controlled by the pipeline.

Explicit table definitions provide deterministic schema management and avoid unnecessary crawler execution.

13. Scalability

The architecture can scale by:

Adding additional CMS datasets
Processing additional PBJ quarters
Adding historical S3 partitions
Increasing Glue worker capacity
Adding additional Athena analytical views
Deploying the Streamlit layer to a managed environment

Because storage and query layers are decoupled, historical data can be added without redesigning the overall architecture.

14. Final Architecture

The final design intentionally minimizes unnecessary infrastructure.

Google Drive
      |
      v
AWS Glue Workflow
      |
      v
Glue Python Shell
      |
      v
Amazon S3 Raw
      |
      v
AWS Glue Spark ETL
      |
      v
Amazon S3 Curated / Parquet
      |
      v
AWS Glue Data Catalog
      |
      v
Amazon Athena
      |
      v
Streamlit

Supporting services:

AWS Secrets Manager
Amazon CloudWatch
IAM

The result is a serverless-oriented batch analytics architecture with incremental ingestion, scalable transformation, columnar storage, serverless querying, and an interactive analytical presentation layer


The CMS provider identifier is represented by PROVNUM.

PROVNUM is treated as a string rather than a numeric field because it represents an identifier rather than a measurable value.

4. Key Fields

Important PBJ fields include:

Field	Description
PROVNUM	CMS provider identifier
PROVNAME	Provider name
STATE	Facility state
COUNTY_NAME	County
WorkDate	Daily staffing reporting date
MDScensus	Daily resident census
Hrs_RN	Registered Nurse hours
Hrs_LPN	Licensed Practical Nurse hours
Hrs_CNA	Certified Nursing Assistant hours

The source also contains employee and contract components for staffing categories.

5. Date Validation

The expected quarter was:

2024-04-01 through 2024-06-30

Validation confirmed:

Minimum date: 2024-04-01
Maximum date: 2024-06-30
Distinct reporting dates: 91

No records outside the expected quarter were identified in the validated full PBJ dataset.

6. Negative Staffing Validation

Staffing-hour fields were checked for negative values.

Validation result:

Negative staffing-hour records: 0

Negative hours would represent invalid values for the staffing metrics used by the project and would require investigation before metric calculation.

7. Census Validation

MDScensus is used as the denominator for Hours Per Resident Day calculations.

Records are validated before HPRD calculation to avoid invalid denominator values.

HPRD calculations use null-safe division logic when census is zero or unavailable.

8. Staffing Reconciliation

Where employee and contractor components are available, staffing totals were checked against their component values.

This validation helps ensure that contract dependency and direct-care hour calculations are based on internally consistent staffing fields.

9. Duplicate Validation

The expected PBJ business key is:

PROVNUM + WorkDate

Duplicate-key checks were performed to identify repeated facility-day records.

The profiling process did not identify duplicate facility-date keys in the validated data used by the pipeline.

10. Provider Information Dataset

The CMS Provider Information source contained:

Metric	Result
Rows	14,814
Columns	103

The primary join key is:

CMS Certification Number (CCN)

This is normalized to:

provider_id

for the curated provider dataset.

Selected provider attributes include:

Provider Name
City
State
County
Ownership Type
Certified Beds
Overall Rating
Health Inspection Rating
Quality Measure Rating
Staffing Rating
11. Provider Join Validation

The PBJ dataset was enriched using:

PBJ.PROVNUM = ProviderInfo.provider_id

Validation results:

Metric	Result
PBJ Rows	1,325,324
PBJ Facilities	14,564
Provider Records	14,814
Matched PBJ Facilities	14,547
Unmatched PBJ Facilities	17
Match Coverage	99.88%
Output Rows	1,325,324

A left join was intentionally used.

Therefore, unmatched PBJ facilities remain in the analytical dataset while provider enrichment columns may be null.

The output row count matching the PBJ input row count confirms that the enrichment join did not unintentionally multiply facility-day records.

12. Claims-Based Quality Dataset

The CMS claims-based quality source contained:

Metric	Result
Rows	59,256
Facilities	14,814
Measures	4

The dataset grain is:

Provider ID + Measure Code

Each of the four measure codes contains one record per provider before considering missing score values.

13. Quality Measures

The four claims-based measures are:

Code	Measure
521	Short-stay residents rehospitalized after nursing-home admission
522	Short-stay residents with an outpatient emergency-department visit
551	Hospitalizations per 1,000 long-stay resident days
552	Outpatient ED visits per 1,000 long-stay resident days

The project uses Adjusted Score for cross-facility analytical comparison.

14. Quality Score Completeness

Adjusted-score availability differs by measure.

Measure Code	Non-null Adjusted Scores
521	11,928
522	11,928
551	11,638
552	11,638

Missing adjusted scores are retained as null rather than imputed.

This prevents the pipeline from manufacturing quality outcomes where CMS does not provide an adjusted measure.

15. Quality Dataset Uniqueness

Duplicate validation confirmed:

Duplicate Provider + Measure combinations: 0

This supports the expected facility-measure grain.

16. Curated PBJ Validation

After PBJ ETL, validation confirmed:

Rows:             1,325,324
Facilities:       14,564
Reporting dates:  91
Minimum date:     2024-04-01
Maximum date:     2024-06-30
Negative hours:   0

This demonstrates that transformation preserved the expected source coverage.

17. Facility Metrics Validation

The enriched facility metrics output contains:

1,325,324 facility-day records

matching the PBJ input row count.

This is an important pipeline control because enrichment should add provider attributes without changing the daily PBJ grain.

18. Athena Validation

Athena validation of the curated facility dataset confirmed:

Rows:       1,325,324
Facilities: 14,564
Dates:      91

with the expected Q2 2024 minimum and maximum dates.

19. Data Quality Strategy

The project uses several forms of validation:

Structural Validation
Expected columns
Type conversion
Date parsing
Provider identifier normalization
Business Validation
Valid census
Non-negative staffing hours
Expected quarter
Unique business grain
Reconciliation
Input versus output row counts
Employee/contract staffing components
Facility join coverage
Analytical Validation
Facility counts
Reporting-day counts
Quality-measure uniqueness
Null-score analysis
20. Data Quality Conclusion

The datasets were suitable for the intended staffing and quality analysis after normalization and validation.

The most important data-quality considerations are:

Some facilities do not have matching provider enrichment records.
CMS adjusted quality scores are unavailable for a subset of facilities.
Staffing and claims-based quality measures cover different measurement periods.

These conditions are preserved transparently in the analytical layer rather than being hidden through aggressive filtering or imputation.


