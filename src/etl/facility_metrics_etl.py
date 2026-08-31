import sys

from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job

from pyspark.sql import functions as F


# ============================================================
# SETUP
# ============================================================

args = getResolvedOptions(
    sys.argv,
    ["JOB_NAME", "BUCKET_NAME"]
)

bucket = args["BUCKET_NAME"]

pbj_path = f"s3://{bucket}/curated/pbj_metrics/"
provider_path = f"s3://{bucket}/curated/provider_info/"
output_path = f"s3://{bucket}/curated/facility_metrics/"


sc = SparkContext()
glue_context = GlueContext(sc)
spark = glue_context.spark_session

job = Job(glue_context)
job.init(args["JOB_NAME"], args)


print("=" * 70)
print("Healthcare Facility Metrics ETL")
print("=" * 70)


# ============================================================
# READ CURATED DATA
# ============================================================

pbj = spark.read.parquet(pbj_path)
provider = spark.read.parquet(provider_path)

print(f"PBJ rows: {pbj.count()}")
print(f"Provider rows: {provider.count()}")


# ============================================================
# NORMALIZE JOIN KEYS
# ============================================================

pbj = pbj.withColumn(
    "join_provider_id",
    F.trim(F.col("PROVNUM").cast("string"))
)

provider = provider.withColumn(
    "join_provider_id",
    F.trim(F.col("provider_id").cast("string"))
)


# ============================================================
# JOIN COVERAGE TEST
# ============================================================

pbj_facilities = (
    pbj
    .select("join_provider_id")
    .where(F.col("join_provider_id").isNotNull())
    .distinct()
)

provider_facilities = (
    provider
    .select("join_provider_id")
    .where(F.col("join_provider_id").isNotNull())
    .distinct()
)


total_pbj_facilities = pbj_facilities.count()

matched_facilities = (
    pbj_facilities
    .join(
        provider_facilities,
        "join_provider_id",
        "inner"
    )
    .count()
)

unmatched = (
    pbj_facilities
    .join(
        provider_facilities,
        "join_provider_id",
        "left_anti"
    )
)


unmatched_count = unmatched.count()

coverage_pct = (
    matched_facilities
    / total_pbj_facilities
    * 100
    if total_pbj_facilities > 0
    else 0
)


print("=" * 70)
print("JOIN COVERAGE")
print("=" * 70)

print(
    f"PBJ facilities       : {total_pbj_facilities}"
)

print(
    f"Matched facilities   : {matched_facilities}"
)

print(
    f"Unmatched facilities : {unmatched_count}"
)

print(
    f"Coverage             : {coverage_pct:.2f}%"
)


if unmatched_count > 0:
    print("UNMATCHED PROVIDER IDs:")

    unmatched.show(
        50,
        truncate=False
    )


# ============================================================
# SAFETY CHECK
# ============================================================

if coverage_pct < 95:
    raise RuntimeError(
        f"Join coverage is only {coverage_pct:.2f}%. "
        "Investigate provider IDs before publishing dataset."
    )


# ============================================================
# JOIN
# ============================================================

provider_selected = provider.select(
    "join_provider_id",

    F.col("provider_name")
        .alias("provider_name"),

    F.col("provider_address")
        .alias("provider_address"),

    F.col("city")
        .alias("provider_city"),

    F.col("state")
        .alias("provider_state"),

    F.col("zip_code")
        .alias("provider_zip_code"),

    F.col("county")
        .alias("provider_county"),

    F.col("ownership_type")
        .alias("provider_ownership_type"),

    F.col("certified_beds")
        .alias("provider_certified_beds"),

    F.col("avg_residents_per_day")
        .alias("provider_avg_residents_per_day"),

    F.col("provider_type")
        .alias("provider_type"),

    F.col("overall_rating")
        .alias("provider_overall_rating"),

    F.col("health_inspection_rating")
        .alias("provider_health_inspection_rating"),

    F.col("qm_rating")
        .alias("provider_qm_rating"),

    F.col("staffing_rating")
        .alias("provider_staffing_rating")
)


facility_metrics = (
    pbj
    .join(
        provider_selected,
        "join_provider_id",
        "left"
    )
    .drop("join_provider_id")
)


# ============================================================
# METADATA
# ============================================================

facility_metrics = (
    facility_metrics
    .withColumn(
        "facility_metrics_processed_at",
        F.current_timestamp()
    )
)


# ============================================================
# VALIDATION
# ============================================================

pbj_row_count = pbj.count()
output_row_count = facility_metrics.count()

print("=" * 70)
print("ROW VALIDATION")
print("=" * 70)

print(f"PBJ rows    : {pbj_row_count}")
print(f"Output rows : {output_row_count}")


if pbj_row_count != output_row_count:
    raise RuntimeError(
        "Row count changed after provider join."
    )


# ============================================================
# STANDARDIZE PARTITION COLUMN FOR ATHENA
# ============================================================

facility_metrics = facility_metrics.withColumnRenamed(
    "STATE",
    "state"
)

print("=" * 70)
print("FINAL FACILITY METRICS SCHEMA")
print("=" * 70)

facility_metrics.printSchema()


# ============================================================
# WRITE CURATED FACILITY METRICS
# ============================================================

(
    facility_metrics
    .write
    .mode("overwrite")
    .partitionBy("state")
    .parquet(output_path)
)

print("=" * 70)
print("Facility Metrics ETL completed successfully")
print(f"Output: {output_path}")
print("=" * 70)

job.commit()