import sys

from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from pyspark.sql import functions as F


# ============================================================
# JOB SETUP
# ============================================================

args = getResolvedOptions(
    sys.argv,
    ["JOB_NAME", "BUCKET_NAME"]
)

bucket = args["BUCKET_NAME"]

source_path = (
    f"s3://{bucket}/raw/supporting/"
    "NH_ProviderInfo_Oct2024.csv"
)

target_path = (
    f"s3://{bucket}/curated/provider_info/"
)


sc = SparkContext()
glue_context = GlueContext(sc)
spark = glue_context.spark_session

job = Job(glue_context)
job.init(args["JOB_NAME"], args)


print("=" * 70)
print("Healthcare Provider Info ETL")
print(f"Source: {source_path}")
print(f"Target: {target_path}")
print("=" * 70)


# ============================================================
# READ RAW DATA
# ============================================================

df = (
    spark.read
    .option("header", "true")
    .option("inferSchema", "true")
    .option("multiLine", "true")
    .option("quote", '"')
    .option("escape", '"')
    .csv(source_path)
)

raw_count = df.count()

print(f"Raw rows: {raw_count}")


# ============================================================
# SELECT USEFUL COLUMNS
# ============================================================

provider = df.select(

    F.col("CMS Certification Number (CCN)")
        .cast("string")
        .alias("provider_id"),

    F.col("Provider Name")
        .alias("provider_name"),

    F.col("Provider Address")
        .alias("provider_address"),

    F.col("City/Town")
        .alias("city"),

    F.col("State")
        .alias("state"),

    F.col("ZIP Code")
        .cast("string")
        .alias("zip_code"),

    F.col("County/Parish")
        .alias("county"),

    F.col("Ownership Type")
        .alias("ownership_type"),

    F.col("Number of Certified Beds")
        .cast("double")
        .alias("certified_beds"),

    F.col("Average Number of Residents per Day")
        .cast("double")
        .alias("avg_residents_per_day"),

    F.col("Provider Type")
        .alias("provider_type"),

    F.col("Overall Rating")
        .cast("double")
        .alias("overall_rating"),

    F.col("Health Inspection Rating")
        .cast("double")
        .alias("health_inspection_rating"),

    F.col("QM Rating")
        .cast("double")
        .alias("qm_rating"),

    F.col("Staffing Rating")
        .cast("double")
        .alias("staffing_rating")
)


# ============================================================
# CLEANING
# ============================================================

provider = provider.filter(
    F.col("provider_id").isNotNull()
)


# Trim provider ID
provider = provider.withColumn(
    "provider_id",
    F.trim(F.col("provider_id"))
)


# Remove accidental duplicates by CCN
provider = provider.dropDuplicates(
    ["provider_id"]
)


# ETL metadata
provider = provider.withColumn(
    "etl_processed_at",
    F.current_timestamp()
)


# ============================================================
# VALIDATION
# ============================================================

curated_count = provider.count()

print(f"Curated rows: {curated_count}")

print("Sample:")
provider.show(5, truncate=False)


# ============================================================
# WRITE PARQUET
# ============================================================

(
    provider.write
    .mode("overwrite")
    .parquet(target_path)
)


print("=" * 70)
print("Provider Info ETL completed successfully")
print(f"Output: {target_path}")
print("=" * 70)


job.commit()