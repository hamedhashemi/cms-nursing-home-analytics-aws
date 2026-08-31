import sys

from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job

from pyspark.sql import functions as F


# ============================================================
# PARAMETERS
# ============================================================

args = getResolvedOptions(
    sys.argv,
    [
        "JOB_NAME",
        "BUCKET_NAME"
    ]
)

bucket = args["BUCKET_NAME"]

input_path = (
    f"s3://{bucket}/raw/supporting/"
    "NH_QualityMsr_Claims_Oct2024.csv"
)

output_path = (
    f"s3://{bucket}/curated/"
    "quality_claims/"
)


# ============================================================
# GLUE / SPARK
# ============================================================

sc = SparkContext()

glue_context = GlueContext(sc)

spark = glue_context.spark_session

job = Job(glue_context)

job.init(
    args["JOB_NAME"],
    args
)


# ============================================================
# READ SOURCE
# ============================================================

df = (
    spark.read
    .option("header", "true")
    .option("inferSchema", "true")
    .option("multiLine", "true")
    .option("quote", '"')
    .option("escape", '"')
    .csv(input_path)
)


print(
    f"Raw quality rows: {df.count():,}"
)


# ============================================================
# SELECT / STANDARDIZE
# ============================================================

quality = df.select(

    F.trim(
        F.col(
            "CMS Certification Number (CCN)"
        )
    ).alias(
        "provider_id"
    ),

    F.col(
        "Measure Code"
    ).cast(
        "int"
    ).alias(
        "measure_code"
    ),

    F.col(
        "Measure Description"
    ).alias(
        "measure_description"
    ),

    F.col(
        "Resident type"
    ).alias(
        "resident_type"
    ),

    F.col(
        "Adjusted Score"
    ).cast(
        "double"
    ).alias(
        "adjusted_score"
    ),

    F.col(
        "Observed Score"
    ).cast(
        "double"
    ).alias(
        "observed_score"
    ),

    F.col(
        "Expected Score"
    ).cast(
        "double"
    ).alias(
        "expected_score"
    ),

    F.col(
        "Used in Quality Measure Five Star Rating"
    ).alias(
        "used_in_five_star"
    ),

    F.col(
        "Measure Period"
    ).alias(
        "measure_period"
    ),

    F.col(
        "Processing Date"
    ).alias(
        "processing_date"
    )
)


# ============================================================
# VALIDATION
# ============================================================

quality = quality.filter(
    F.col("provider_id").isNotNull()
    &
    F.col("measure_code").isNotNull()
)


print(
    f"Valid quality rows: {quality.count():,}"
)


print(
    "Distinct facilities:",
    quality.select(
        "provider_id"
    ).distinct().count()
)


print(
    "Distinct measures:",
    quality.select(
        "measure_code"
    ).distinct().count()
)


# ============================================================
# MEASURE DISTRIBUTION
# ============================================================

quality.groupBy(
    "measure_code",
    "measure_description",
    "resident_type"
).agg(

    F.count("*").alias(
        "rows"
    ),

    F.count(
        "adjusted_score"
    ).alias(
        "available_adjusted_scores"
    )

).orderBy(
    "measure_code"
).show(
    100,
    truncate=False
)


# ============================================================
# DUPLICATE CHECK
# ============================================================

duplicates = (
    quality
    .groupBy(
        "provider_id",
        "measure_code"
    )
    .count()
    .filter(
        F.col("count") > 1
    )
)


duplicate_count = (
    duplicates.count()
)


print(
    "Duplicate provider/measure combinations:",
    duplicate_count
)


# ============================================================
# ADD ETL TIMESTAMP
# ============================================================

quality = quality.withColumn(
    "etl_processed_at",
    F.current_timestamp()
)


# ============================================================
# WRITE CURATED PARQUET
# ============================================================

(
    quality.write
    .mode("overwrite")
    .parquet(
        output_path
    )
)


print(
    f"Quality data written to: {output_path}"
)


job.commit()