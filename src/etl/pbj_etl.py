import sys

from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job

from pyspark.sql import functions as F
from pyspark.sql.types import DoubleType


# ============================================================
# JOB PARAMETERS
# ============================================================

args = getResolvedOptions(
    sys.argv,
    [
        "JOB_NAME",
        "BUCKET_NAME"
    ]
)

bucket = args["BUCKET_NAME"]

raw_path = f"s3://{bucket}/raw/pbj/"
curated_path = f"s3://{bucket}/curated/pbj_metrics/"


# ============================================================
# SPARK / GLUE SETUP
# ============================================================

sc = SparkContext()

glue_context = GlueContext(sc)

spark = glue_context.spark_session

job = Job(glue_context)

job.init(
    args["JOB_NAME"],
    args
)


print("=" * 80)
print("Healthcare PBJ ETL")
print(f"Source : {raw_path}")
print(f"Target : {curated_path}")
print("=" * 80)


# ============================================================
# READ RAW PBJ CSV
# ============================================================

df = (
    spark.read
    .option("header", "true")
    .option("inferSchema", "true")
    .option("multiLine", "true")
    .option("quote", '"')
    .option("escape", '"')
    .csv(raw_path)
)


# ============================================================
# SOURCE FILE DIAGNOSTICS
# ============================================================

df = df.withColumn(
    "_source_file",
    F.input_file_name()
)


print()
print("=" * 80)
print("PBJ SOURCE FILES")
print("=" * 80)

(
    df
    .groupBy("_source_file")
    .count()
    .orderBy(F.desc("count"))
    .show(
        100,
        truncate=False
    )
)


raw_count = df.count()

print()
print(f"RAW ROW COUNT: {raw_count}")
print(f"RAW COLUMN COUNT: {len(df.columns)}")


# ============================================================
# REMOVE COMPLETELY EMPTY ROWS
# ============================================================

df = df.dropna(
    how="all",
    subset=[
        c
        for c in df.columns
        if c != "_source_file"
    ]
)


after_empty_drop_count = df.count()

print(
    f"Rows after removing completely empty rows: "
    f"{after_empty_drop_count}"
)


# ============================================================
# NORMALIZE CORE COLUMNS
# ============================================================

df = df.withColumn(
    "PROVNUM",
    F.trim(
        F.col("PROVNUM").cast("string")
    )
)


# ============================================================
# WORKDATE PARSING
# ============================================================

# PBJ WorkDate is normally yyyyMMdd.
# Cast through string first so it works whether inferSchema
# detected integer or string.
df = df.withColumn(
    "WorkDate",
    F.to_date(
        F.col("WorkDate")
        .cast("string"),
        "yyyyMMdd"
    )
)


# ============================================================
# NUMERIC COLUMN CASTS
# ============================================================

numeric_columns = [
    "MDScensus",

    "Hrs_RNDON",
    "Hrs_RNDON_emp",
    "Hrs_RNDON_ctr",

    "Hrs_RNadmin",
    "Hrs_RNadmin_emp",
    "Hrs_RNadmin_ctr",

    "Hrs_RN",
    "Hrs_RN_emp",
    "Hrs_RN_ctr",

    "Hrs_LPNadmin",
    "Hrs_LPNadmin_emp",
    "Hrs_LPNadmin_ctr",

    "Hrs_LPN",
    "Hrs_LPN_emp",
    "Hrs_LPN_ctr",

    "Hrs_CNA",
    "Hrs_CNA_emp",
    "Hrs_CNA_ctr",

    "Hrs_NAtrn",
    "Hrs_NAtrn_emp",
    "Hrs_NAtrn_ctr",

    "Hrs_MedAide",
    "Hrs_MedAide_emp",
    "Hrs_MedAide_ctr"
]


for column_name in numeric_columns:

    if column_name in df.columns:

        df = df.withColumn(
            column_name,
            F.col(column_name)
            .cast(DoubleType())
        )


# ============================================================
# DATA QUALITY FILTERS
# ============================================================

# Valid provider ID
df = df.filter(
    F.col("PROVNUM").isNotNull()
    &
    (F.length(F.col("PROVNUM")) > 0)
)


# Valid work date
df = df.filter(
    F.col("WorkDate").isNotNull()
)


# Valid census
df = df.filter(
    F.col("MDScensus").isNotNull()
)


df = df.filter(
    F.col("MDScensus") >= 0
)


# ============================================================
# OPTIONAL NEGATIVE HOUR VALIDATION
# ============================================================

hour_columns = [
    c
    for c in numeric_columns
    if c.startswith("Hrs_")
]


negative_condition = None

for c in hour_columns:

    condition = F.col(c) < 0

    if negative_condition is None:
        negative_condition = condition
    else:
        negative_condition = (
            negative_condition | condition
        )


if negative_condition is not None:

    negative_hour_rows = (
        df.filter(
            negative_condition
        )
        .count()
    )

    print(
        f"Rows containing negative staffing hours: "
        f"{negative_hour_rows}"
    )


# ============================================================
# DIRECT CARE HOURS
# ============================================================

df = df.withColumn(
    "TOTAL_DIRECT_HOURS",
    (
        F.coalesce(
            F.col("Hrs_RN"),
            F.lit(0.0)
        )
        +
        F.coalesce(
            F.col("Hrs_LPN"),
            F.lit(0.0)
        )
        +
        F.coalesce(
            F.col("Hrs_CNA"),
            F.lit(0.0)
        )
    )
)


# ============================================================
# CONTRACT DIRECT CARE HOURS
# ============================================================

df = df.withColumn(
    "CONTRACT_DIRECT_HOURS",
    (
        F.coalesce(
            F.col("Hrs_RN_ctr"),
            F.lit(0.0)
        )
        +
        F.coalesce(
            F.col("Hrs_LPN_ctr"),
            F.lit(0.0)
        )
        +
        F.coalesce(
            F.col("Hrs_CNA_ctr"),
            F.lit(0.0)
        )
    )
)


# ============================================================
# RN HPRD
# ============================================================

df = df.withColumn(
    "RN_HPRD",
    F.when(
        F.col("MDScensus") > 0,
        F.col("Hrs_RN")
        /
        F.col("MDScensus")
    )
)


# ============================================================
# LPN HPRD
# ============================================================

df = df.withColumn(
    "LPN_HPRD",
    F.when(
        F.col("MDScensus") > 0,
        F.col("Hrs_LPN")
        /
        F.col("MDScensus")
    )
)


# ============================================================
# CNA HPRD
# ============================================================

df = df.withColumn(
    "CNA_HPRD",
    F.when(
        F.col("MDScensus") > 0,
        F.col("Hrs_CNA")
        /
        F.col("MDScensus")
    )
)


# ============================================================
# TOTAL DIRECT HPRD
# ============================================================

df = df.withColumn(
    "TOTAL_DIRECT_HPRD",
    F.when(
        F.col("MDScensus") > 0,
        F.col("TOTAL_DIRECT_HOURS")
        /
        F.col("MDScensus")
    )
)


# ============================================================
# CONTRACT DEPENDENCY
# ============================================================

df = df.withColumn(
    "CONTRACT_DEPENDENCY_PCT",
    F.when(
        F.col("TOTAL_DIRECT_HOURS") > 0,
        (
            F.col("CONTRACT_DIRECT_HOURS")
            /
            F.col("TOTAL_DIRECT_HOURS")
        )
        * 100
    )
)


# ============================================================
# BASIC DATA VALIDATION
# ============================================================

curated_count = df.count()

facility_count = (
    df.select("PROVNUM")
    .distinct()
    .count()
)

date_count = (
    df.select("WorkDate")
    .distinct()
    .count()
)


date_range = (
    df.agg(
        F.min("WorkDate")
        .alias("min_date"),

        F.max("WorkDate")
        .alias("max_date")
    )
    .collect()[0]
)


print()
print("=" * 80)
print("PBJ VALIDATION SUMMARY")
print("=" * 80)

print(
    f"Valid rows       : "
    f"{curated_count}"
)

print(
    f"Facilities       : "
    f"{facility_count}"
)

print(
    f"Distinct dates   : "
    f"{date_count}"
)

print(
    f"Minimum date     : "
    f"{date_range['min_date']}"
)

print(
    f"Maximum date     : "
    f"{date_range['max_date']}"
)


# ============================================================
# STATE DISTRIBUTION
# ============================================================

print()
print("=" * 80)
print("STATE DISTRIBUTION")
print("=" * 80)

(
    df
    .groupBy("STATE")
    .agg(
        F.count("*")
        .alias("rows"),

        F.countDistinct("PROVNUM")
        .alias("facilities")
    )
    .orderBy("STATE")
    .show(
        100,
        truncate=False
    )
)


# ============================================================
# DATE / QUARTER DIAGNOSTIC
# ============================================================

print()
print("=" * 80)
print("YEAR / QUARTER DISTRIBUTION")
print("=" * 80)

(
    df
    .withColumn(
        "_year",
        F.year("WorkDate")
    )
    .withColumn(
        "_quarter",
        F.quarter("WorkDate")
    )
    .groupBy(
        "_year",
        "_quarter"
    )
    .agg(
        F.count("*")
        .alias("rows"),

        F.countDistinct("PROVNUM")
        .alias("facilities")
    )
    .orderBy(
        "_year",
        "_quarter"
    )
    .show(
        100,
        truncate=False
    )
)


# ============================================================
# ETL METADATA
# ============================================================

df = df.withColumn(
    "etl_processed_at",
    F.current_timestamp()
)


# ============================================================
# REMOVE DIAGNOSTIC SOURCE COLUMN
# ============================================================

df = df.drop(
    "_source_file"
)


# ============================================================
# WRITE CURATED PARQUET
# ============================================================

print()
print("=" * 80)
print("WRITING CURATED PBJ DATA")
print("=" * 80)


(
    df.write
    .mode("overwrite")
    .partitionBy("STATE")
    .parquet(curated_path)
)


print()
print("=" * 80)
print("Healthcare PBJ ETL completed successfully")
print(f"Output: {curated_path}")
print("=" * 80)


job.commit()