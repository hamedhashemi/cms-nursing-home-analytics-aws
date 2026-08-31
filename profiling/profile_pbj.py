from pathlib import Path
import pandas as pd
import numpy as np

# --------------------------------------------------
# 1. File location
# --------------------------------------------------

BASE_DIR = Path(__file__).resolve().parent.parent

FILE_PATH = (
    BASE_DIR
    / "data"
    / "raw"
    / "PBJ_Daily_Nurse_Staffing_Q2_2024-sample.csv"
)

OUTPUT_DIR = BASE_DIR / "profiling" / "output"
OUTPUT_DIR.mkdir(parents=True, exist_ok=True)

# --------------------------------------------------
# 2. Read data
# --------------------------------------------------

print("\nReading PBJ dataset...")

df = pd.read_csv(
    FILE_PATH,
    dtype={"PROVNUM": "string"}
)

print("File loaded successfully.")


# --------------------------------------------------
# 3. Remove completely empty rows
# --------------------------------------------------

rows_before = len(df)

df = df.dropna(how="all")

df["WorkDate"] = pd.to_datetime(
    df["WorkDate"].astype("Int64").astype("string"),
    format="%Y%m%d",
    errors="coerce"
)

rows_after = len(df)

blank_rows_removed = rows_before - rows_after


# --------------------------------------------------
# 4. Dataset overview
# --------------------------------------------------

print("\n" + "=" * 60)
print("PBJ DATA PROFILING")
print("=" * 60)

print("\n--- DATASET OVERVIEW ---")

print(f"Rows read from CSV       : {rows_before:,}")
print(f"Blank rows removed       : {blank_rows_removed:,}")
print(f"Valid rows               : {rows_after:,}")
print(f"Number of columns        : {df.shape[1]:,}")


# --------------------------------------------------
# 5. Schema
# --------------------------------------------------

print("\n--- SCHEMA ---")

schema = pd.DataFrame({
    "column_name": df.columns,
    "data_type": df.dtypes.astype(str).values
})

print(schema.to_string(index=False))

schema.to_csv(
    OUTPUT_DIR / "pbj_schema.csv",
    index=False
)

# --------------------------------------------------
# 6. Basic cardinality
# --------------------------------------------------

print("\n--- BASIC CARDINALITY ---")

print(
    f"Unique facilities        : "
    f"{df['PROVNUM'].nunique():,}"
)

print(
    f"Unique states            : "
    f"{df['STATE'].nunique():,}"
)

print(
    f"Unique work dates        : "
    f"{df['WorkDate'].nunique():,}"
)


# --------------------------------------------------
# 7. Null analysis
# --------------------------------------------------

print("\n--- NULL ANALYSIS ---")

null_analysis = pd.DataFrame({
    "column_name": df.columns,
    "null_count": df.isna().sum().values,
    "null_percentage": (df.isna().mean().values * 100).round(2)
})

null_analysis = null_analysis.sort_values(
    by="null_percentage",
    ascending=False
)

print(null_analysis.to_string(index=False))

null_analysis.to_csv(
    OUTPUT_DIR / "pbj_null_analysis.csv",
    index=False
)

# --------------------------------------------------
# 8. Duplicate and grain analysis
# --------------------------------------------------

print("\n--- DUPLICATE ANALYSIS ---")

full_duplicates = df.duplicated().sum()

business_key_duplicates = df.duplicated(
    subset=["PROVNUM", "WorkDate"]
).sum()

print(f"Fully duplicated rows       : {full_duplicates:,}")
print(f"Duplicate PROVNUM + WorkDate: {business_key_duplicates:,}")



# --------------------------------------------------
# 9. Date validation
# --------------------------------------------------

print("\n--- DATE VALIDATION ---")

invalid_dates = df["WorkDate"].isna().sum()

print(f"Minimum WorkDate : {df['WorkDate'].min()}")
print(f"Maximum WorkDate : {df['WorkDate'].max()}")
print(f"Invalid dates    : {invalid_dates:,}")

q2_start = pd.Timestamp("2024-04-01")
q2_end = pd.Timestamp("2024-06-30")

outside_q2 = (
    (df["WorkDate"] < q2_start) |
    (df["WorkDate"] > q2_end)
).sum()

print(f"Dates outside Q2 : {outside_q2:,}")


# --------------------------------------------------
# 10. Cardinality analysis
# --------------------------------------------------

print("\n--- CARDINALITY ANALYSIS ---")

print(f"Unique facilities : {df['PROVNUM'].nunique():,}")
print(f"Unique states     : {df['STATE'].nunique():,}")
print(f"Unique counties   : {df['COUNTY_FIPS'].nunique():,}")
print(f"Unique dates      : {df['WorkDate'].nunique():,}")

# --------------------------------------------------
# 11. Census profiling
# --------------------------------------------------

print("\n--- MDS CENSUS PROFILING ---")

print(
    df["MDScensus"]
    .describe()
    .round(2)
    .to_string()
)

negative_census = (df["MDScensus"] < 0).sum()
zero_census = (df["MDScensus"] == 0).sum()

print(f"\nNegative census rows : {negative_census:,}")
print(f"Zero census rows     : {zero_census:,}")

key_staffing_columns = [
    "Hrs_RN",
    "Hrs_LPN",
    "Hrs_CNA"
]

print("\n--- KEY STAFFING STATISTICS ---")

print(
    df[key_staffing_columns]
    .describe()
    .round(2)
    .to_string()
)

negative_hours = (df[key_staffing_columns] < 0).sum()

print("\nNegative staffing hours:")
print(negative_hours.to_string())

# --------------------------------------------------
# 12. Employee + contractor reconciliation
# --------------------------------------------------

print("\n--- STAFFING RECONCILIATION ---")

staff_types = [
    ("RN", "Hrs_RN", "Hrs_RN_emp", "Hrs_RN_ctr"),
    ("LPN", "Hrs_LPN", "Hrs_LPN_emp", "Hrs_LPN_ctr"),
    ("CNA", "Hrs_CNA", "Hrs_CNA_emp", "Hrs_CNA_ctr"),
    ("NA Training", "Hrs_NAtrn", "Hrs_NAtrn_emp", "Hrs_NAtrn_ctr"),
    ("Med Aide", "Hrs_MedAide", "Hrs_MedAide_emp", "Hrs_MedAide_ctr"),
]

for name, total_col, emp_col, ctr_col in staff_types:

    difference = (
        df[total_col].fillna(0)
        - (
            df[emp_col].fillna(0)
            + df[ctr_col].fillna(0)
        )
    ).abs()

    mismatches = (difference > 0.01).sum()

    print(f"{name:<12}: {mismatches:,} mismatches")

# --------------------------------------------------
# 13. Outlier analysis
# --------------------------------------------------

print("\n--- OUTLIER ANALYSIS (IQR) ---")

for column in ["MDScensus", "Hrs_RN", "Hrs_LPN", "Hrs_CNA"]:

    series = df[column].dropna()

    q1 = series.quantile(0.25)
    q3 = series.quantile(0.75)

    iqr = q3 - q1

    lower_bound = q1 - 1.5 * iqr
    upper_bound = q3 + 1.5 * iqr

    outliers = (
        (series < lower_bound) |
        (series > upper_bound)
    ).sum()

    print(
        f"{column:<12} "
        f"Lower={lower_bound:.2f} "
        f"Upper={upper_bound:.2f} "
        f"Outliers={outliers:,}"
    )    

# --------------------------------------------------
# 14. HPRD profiling
# --------------------------------------------------

print("\n--- HOURS PER RESIDENT DAY (HPRD) ---")

df["RN_HPRD"] = (
    df["Hrs_RN"] / df["MDScensus"]
)

df["LPN_HPRD"] = (
    df["Hrs_LPN"] / df["MDScensus"]
)

df["CNA_HPRD"] = (
    df["Hrs_CNA"] / df["MDScensus"]
)

df["TOTAL_DIRECT_HPRD"] = (
    df["Hrs_RN"]
    + df["Hrs_LPN"]
    + df["Hrs_CNA"]
) / df["MDScensus"]

hprd_columns = [
    "RN_HPRD",
    "LPN_HPRD",
    "CNA_HPRD",
    "TOTAL_DIRECT_HPRD"
]

print(
    df[hprd_columns]
    .describe()
    .round(3)
    .to_string()
)

# --------------------------------------------------
# 15. Contract staffing profiling
# --------------------------------------------------

print("\n--- CONTRACT STAFFING ANALYSIS ---")

df["TOTAL_DIRECT_HOURS"] = (
    df["Hrs_RN"]
    + df["Hrs_LPN"]
    + df["Hrs_CNA"]
)

df["TOTAL_DIRECT_CONTRACT_HOURS"] = (
    df["Hrs_RN_ctr"]
    + df["Hrs_LPN_ctr"]
    + df["Hrs_CNA_ctr"]
)

df["CONTRACT_DEPENDENCY_PCT"] = (
    df["TOTAL_DIRECT_CONTRACT_HOURS"]
    / df["TOTAL_DIRECT_HOURS"]
    * 100
)

print(
    df["CONTRACT_DEPENDENCY_PCT"]
    .describe()
    .round(2)
    .to_string()
)

invalid_contract_pct = (
    (df["CONTRACT_DEPENDENCY_PCT"] < 0)
    |
    (df["CONTRACT_DEPENDENCY_PCT"] > 100)
).sum()

print(
    f"Invalid contract dependency %: "
    f"{invalid_contract_pct:,}"
)



print("\nProfiling completed successfully.")