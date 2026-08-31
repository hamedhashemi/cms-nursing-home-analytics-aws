import time

import boto3
import pandas as pd
import streamlit as st


# ============================================================
# CONFIGURATION
# ============================================================

AWS_REGION = "us-east-1"

ATHENA_DATABASE = "healthcare_metrics"

ATHENA_OUTPUT = (
    "s3://healthcare-metrics-data/"
    "athena-results/"
)


# ============================================================
# PAGE CONFIG
# ============================================================

st.set_page_config(
    page_title="CMS Nursing Home Staffing Analytics",
    page_icon="🏥",
    layout="wide",
    initial_sidebar_state="expanded",
)


# ============================================================
# AWS CLIENT
# ============================================================

@st.cache_resource
def get_athena_client():
    return boto3.client(
        "athena",
        region_name=AWS_REGION,
    )


athena = get_athena_client()


# ============================================================
# ATHENA QUERY FUNCTION
# ============================================================

@st.cache_data(ttl=1800, show_spinner=False)
def run_athena_query(sql):

    response = athena.start_query_execution(
        QueryString=sql,
        QueryExecutionContext={
            "Database": ATHENA_DATABASE
        },
        ResultConfiguration={
            "OutputLocation": ATHENA_OUTPUT
        },
    )

    query_execution_id = response[
        "QueryExecutionId"
    ]

    while True:

        response = athena.get_query_execution(
            QueryExecutionId=query_execution_id
        )

        status = (
            response["QueryExecution"]
            ["Status"]["State"]
        )

        if status == "SUCCEEDED":
            break

        if status in [
            "FAILED",
            "CANCELLED",
        ]:

            reason = (
                response["QueryExecution"]
                ["Status"]
                .get(
                    "StateChangeReason",
                    "Unknown Athena error",
                )
            )

            raise RuntimeError(
                f"Athena query {status}: {reason}"
            )

        time.sleep(0.5)

    paginator = athena.get_paginator(
        "get_query_results"
    )

    rows = []
    column_names = None
    first_page = True

    for page in paginator.paginate(
        QueryExecutionId=query_execution_id
    ):

        if column_names is None:

            column_names = [
                column["Label"]
                for column in
                page["ResultSet"]
                ["ResultSetMetadata"]
                ["ColumnInfo"]
            ]

        page_rows = (
            page["ResultSet"]["Rows"]
        )

        if first_page and page_rows:
            page_rows = page_rows[1:]
            first_page = False

        for row in page_rows:

            data = row.get(
                "Data",
                []
            )

            values = [
                (
                    data[i].get(
                        "VarCharValue"
                    )
                    if i < len(data)
                    else None
                )
                for i in range(
                    len(column_names)
                )
            ]

            rows.append(values)

    return pd.DataFrame(
        rows,
        columns=column_names,
    )


# ============================================================
# HELPERS
# ============================================================

def convert_numeric_columns(
    df,
    columns,
):

    df = df.copy()

    for column in columns:

        if column in df.columns:

            df[column] = pd.to_numeric(
                df[column],
                errors="coerce",
            )

    return df


def safe_float(value):

    try:

        if pd.isna(value):
            return None

        return float(value)

    except Exception:
        return None


def safe_int(value):

    try:

        if pd.isna(value):
            return None

        return int(float(value))

    except Exception:
        return None


def format_number(
    value,
    decimals=2,
):

    value = safe_float(value)

    if value is None:
        return "N/A"

    return f"{value:.{decimals}f}"


def format_integer(value):

    value = safe_int(value)

    if value is None:
        return "N/A"

    return f"{value:,}"


def format_percent(
    value,
    decimals=2,
):

    value = safe_float(value)

    if value is None:
        return "N/A"

    return f"{value:.{decimals}f}%"


def sql_escape(value):

    return str(value).replace(
        "'",
        "''",
    )


# ============================================================
# HEADER
# ============================================================

st.title(
    "🏥 CMS Nursing Home Staffing Analytics"
)

st.caption(
    "CMS Payroll-Based Journal (PBJ) "
    "Daily Nurse Staffing — Q2 2024"
)

st.markdown(
    """
This dashboard analyzes nursing-home staffing,
contract labor dependency, CMS ratings,
and resident quality outcomes across the United States.
"""
)

st.divider()


# ============================================================
# LOAD STATE DATA
# ============================================================

state_query = """
SELECT
    s.state,
    s.total_facilities,
    s.total_resident_days,
    s.rn_hprd,
    s.lpn_hprd,
    s.cna_hprd,
    s.total_direct_hprd,
    s.contract_dependency_pct,
    r.avg_overall_rating,
    r.avg_health_inspection_rating,
    r.avg_qm_rating,
    r.avg_staffing_rating

FROM healthcare_metrics.vw_state_summary s

LEFT JOIN healthcare_metrics.vw_state_facility_ratings r
    ON s.state = r.state

ORDER BY s.state
"""


try:

    with st.spinner(
        "Loading healthcare metrics..."
    ):

        state_df = run_athena_query(
            state_query
        )

except Exception as e:

    st.error(
        f"Unable to query Amazon Athena: {e}"
    )

    st.stop()


state_numeric_columns = [
    "total_facilities",
    "total_resident_days",
    "rn_hprd",
    "lpn_hprd",
    "cna_hprd",
    "total_direct_hprd",
    "contract_dependency_pct",
    "avg_overall_rating",
    "avg_health_inspection_rating",
    "avg_qm_rating",
    "avg_staffing_rating",
]


state_df = convert_numeric_columns(
    state_df,
    state_numeric_columns,
)


# ============================================================
# NATIONAL KPI DATA
# ============================================================

national_query = """
SELECT
    COUNT(DISTINCT provnum)
        AS total_facilities,

    SUM(mdscensus)
        AS total_resident_days,

    SUM(hrs_rn)
        / NULLIF(SUM(mdscensus), 0)
        AS rn_hprd,

    SUM(hrs_lpn)
        / NULLIF(SUM(mdscensus), 0)
        AS lpn_hprd,

    SUM(hrs_cna)
        / NULLIF(SUM(mdscensus), 0)
        AS cna_hprd,

    SUM(total_direct_hours)
        / NULLIF(SUM(mdscensus), 0)
        AS total_direct_hprd,

    100.0
        * SUM(contract_direct_hours)
        / NULLIF(
            SUM(total_direct_hours),
            0
        )
        AS contract_dependency_pct

FROM healthcare_metrics.facility_metrics
"""


national_df = run_athena_query(
    national_query
)


national_df = convert_numeric_columns(
    national_df,
    [
        "total_facilities",
        "total_resident_days",
        "rn_hprd",
        "lpn_hprd",
        "cna_hprd",
        "total_direct_hprd",
        "contract_dependency_pct",
    ],
)


national = national_df.iloc[0]


# ============================================================
# NATIONAL QUALITY DATA
# ============================================================

national_quality_query = """
SELECT
    AVG(rehospitalization_rate)
        AS avg_rehospitalization_rate,

    AVG(short_stay_ed_rate)
        AS avg_short_stay_ed_rate,

    AVG(hospitalizations_per_1000)
        AS avg_hospitalizations_per_1000,

    AVG(long_stay_ed_visits_per_1000)
        AS avg_long_stay_ed_per_1000

FROM healthcare_metrics.vw_facility_analytics
"""


national_quality_df = run_athena_query(
    national_quality_query
)


national_quality_df = (
    convert_numeric_columns(
        national_quality_df,
        [
            "avg_rehospitalization_rate",
            "avg_short_stay_ed_rate",
            "avg_hospitalizations_per_1000",
            "avg_long_stay_ed_per_1000",
        ],
    )
)


national_quality = (
    national_quality_df.iloc[0]
)


# ============================================================
# SIDEBAR
# ============================================================

st.sidebar.title(
    "Dashboard Filters"
)


state_list = (
    state_df["state"]
    .dropna()
    .sort_values()
    .tolist()
)


default_state_index = (
    state_list.index("FL")
    if "FL" in state_list
    else 0
)


selected_state = (
    st.sidebar.selectbox(
        "State",
        state_list,
        index=default_state_index,
    )
)


st.sidebar.markdown("---")


ranking_metric_label = (
    st.sidebar.selectbox(
        "State Ranking Metric",
        [
            "Total Direct HPRD",
            "RN HPRD",
            "LPN HPRD",
            "CNA HPRD",
            "Contract Dependency %",
            "Overall Rating",
            "Staffing Rating",
        ],
    )
)


top_n_states = (
    st.sidebar.slider(
        "Number of States",
        min_value=5,
        max_value=len(state_df),
        value=min(
            15,
            len(state_df),
        ),
    )
)


st.sidebar.markdown("---")


if st.sidebar.button(
    "Refresh Athena Data"
):

    st.cache_data.clear()

    st.rerun()


st.sidebar.caption(
    "Athena query results are cached "
    "for 30 minutes."
)


# ============================================================
# NATIONAL OVERVIEW
# ============================================================

st.header(
    "National Overview"
)


k1, k2, k3, k4, k5 = (
    st.columns(5)
)


k1.metric(
    "Facilities",
    format_integer(
        national[
            "total_facilities"
        ]
    ),
)


k2.metric(
    "RN HPRD",
    format_number(
        national[
            "rn_hprd"
        ]
    ),
)


k3.metric(
    "CNA HPRD",
    format_number(
        national[
            "cna_hprd"
        ]
    ),
)


k4.metric(
    "Total Direct HPRD",
    format_number(
        national[
            "total_direct_hprd"
        ]
    ),
)


k5.metric(
    "Contract Dependency",
    format_percent(
        national[
            "contract_dependency_pct"
        ]
    ),
)


st.caption(
    "HPRD = Hours Per Resident Day"
)


# ============================================================
# NATIONAL QUALITY OVERVIEW
# ============================================================

st.subheader(
    "National Quality Outcomes"
)


q1, q2, q3, q4 = (
    st.columns(4)
)


q1.metric(
    "Rehospitalization",
    format_percent(
        national_quality[
            "avg_rehospitalization_rate"
        ]
    ),
)


q2.metric(
    "Short-Stay ED",
    format_percent(
        national_quality[
            "avg_short_stay_ed_rate"
        ]
    ),
)


q3.metric(
    "Hospitalizations / 1,000",
    format_number(
        national_quality[
            "avg_hospitalizations_per_1000"
        ]
    ),
)


q4.metric(
    "Long-Stay ED / 1,000",
    format_number(
        national_quality[
            "avg_long_stay_ed_per_1000"
        ]
    ),
)


st.caption(
    "Quality outcomes use CMS risk-adjusted scores "
    "where available."
)


st.divider()


# ============================================================
# STATE ANALYSIS
# ============================================================

st.header(
    f"State Overview — {selected_state}"
)


selected_state_df = (
    state_df[
        state_df["state"]
        == selected_state
    ]
)


if selected_state_df.empty:

    st.warning(
        "No state data available."
    )

    st.stop()


state_row = (
    selected_state_df.iloc[0]
)


s1, s2, s3, s4, s5 = (
    st.columns(5)
)


s1.metric(
    "Facilities",
    format_integer(
        state_row[
            "total_facilities"
        ]
    ),
)


s2.metric(
    "RN HPRD",
    format_number(
        state_row[
            "rn_hprd"
        ]
    ),
)


s3.metric(
    "LPN HPRD",
    format_number(
        state_row[
            "lpn_hprd"
        ]
    ),
)


s4.metric(
    "CNA HPRD",
    format_number(
        state_row[
            "cna_hprd"
        ]
    ),
)


s5.metric(
    "Total HPRD",
    format_number(
        state_row[
            "total_direct_hprd"
        ]
    ),
)


left_column, right_column = (
    st.columns(2)
)


with left_column:

    st.subheader(
        "Staffing Mix"
    )

    staffing_mix = pd.DataFrame(
        {
            "Staff Type": [
                "RN",
                "LPN",
                "CNA",
            ],
            "HPRD": [
                safe_float(
                    state_row[
                        "rn_hprd"
                    ]
                ) or 0,
                safe_float(
                    state_row[
                        "lpn_hprd"
                    ]
                ) or 0,
                safe_float(
                    state_row[
                        "cna_hprd"
                    ]
                ) or 0,
            ],
        }
    )

    st.bar_chart(
        staffing_mix,
        x="Staff Type",
        y="HPRD",
    )


with right_column:

    st.subheader(
        "CMS Ratings"
    )

    ratings_df = pd.DataFrame(
        {
            "Rating": [
                "Overall",
                "Health Inspection",
                "Quality Measure",
                "Staffing",
            ],
            "Score": [
                safe_float(
                    state_row[
                        "avg_overall_rating"
                    ]
                ) or 0,
                safe_float(
                    state_row[
                        "avg_health_inspection_rating"
                    ]
                ) or 0,
                safe_float(
                    state_row[
                        "avg_qm_rating"
                    ]
                ) or 0,
                safe_float(
                    state_row[
                        "avg_staffing_rating"
                    ]
                ) or 0,
            ],
        }
    )

    st.bar_chart(
        ratings_df,
        x="Rating",
        y="Score",
    )


st.divider()


# ============================================================
# STATE COMPARISON
# ============================================================

st.header(
    "State Comparison"
)


metric_mapping = {

    "Total Direct HPRD":
        "total_direct_hprd",

    "RN HPRD":
        "rn_hprd",

    "LPN HPRD":
        "lpn_hprd",

    "CNA HPRD":
        "cna_hprd",

    "Contract Dependency %":
        "contract_dependency_pct",

    "Overall Rating":
        "avg_overall_rating",

    "Staffing Rating":
        "avg_staffing_rating",
}


ranking_column = (
    metric_mapping[
        ranking_metric_label
    ]
)


state_ranking_df = (
    state_df[
        [
            "state",
            ranking_column,
        ]
    ]
    .dropna()
    .sort_values(
        ranking_column,
        ascending=False,
    )
    .head(top_n_states)
)


st.bar_chart(
    state_ranking_df,
    x="state",
    y=ranking_column,
)


with st.expander(
    "View Full State Comparison"
):

    state_display = (
        state_df.copy()
    )

    for column in state_numeric_columns:

        if column in state_display.columns:

            state_display[column] = (
                state_display[column]
                .round(2)
            )

    st.dataframe(
        state_display,
        use_container_width=True,
        hide_index=True,
    )


st.divider()


# ============================================================
# LOAD FACILITY ANALYTICS
# ============================================================

safe_state = sql_escape(
    selected_state
)


facility_query = f"""
SELECT
    provnum,
    provider_name,
    state,
    city,
    county,
    ownership_type,
    certified_beds,

    overall_rating,
    health_inspection_rating,
    qm_rating,
    staffing_rating,

    total_resident_days,
    rn_hprd,
    lpn_hprd,
    cna_hprd,
    total_direct_hprd,
    contract_dependency_pct,
    reporting_days,

    rehospitalization_rate,
    short_stay_ed_rate,
    hospitalizations_per_1000,
    long_stay_ed_visits_per_1000

FROM healthcare_metrics.vw_facility_analytics

WHERE state = '{safe_state}'

ORDER BY provider_name
"""


facility_df = (
    run_athena_query(
        facility_query
    )
)


facility_numeric_columns = [
    "certified_beds",
    "overall_rating",
    "health_inspection_rating",
    "qm_rating",
    "staffing_rating",
    "total_resident_days",
    "rn_hprd",
    "lpn_hprd",
    "cna_hprd",
    "total_direct_hprd",
    "contract_dependency_pct",
    "reporting_days",
    "rehospitalization_rate",
    "short_stay_ed_rate",
    "hospitalizations_per_1000",
    "long_stay_ed_visits_per_1000",
]


facility_df = (
    convert_numeric_columns(
        facility_df,
        facility_numeric_columns,
    )
)


# ============================================================
# FACILITY RANKINGS
# ============================================================

st.header(
    f"Facility Performance — {selected_state}"
)


if facility_df.empty:

    st.warning(
        "No facilities found for this state."
    )

    st.stop()


rank_left, rank_right = (
    st.columns(2)
)


with rank_left:

    st.subheader(
        "Highest Staffing Facilities"
    )

    top_facilities = (
        facility_df[
            [
                "provider_name",
                "total_direct_hprd",
                "rehospitalization_rate",
            ]
        ]
        .dropna(
            subset=[
                "total_direct_hprd"
            ]
        )
        .sort_values(
            "total_direct_hprd",
            ascending=False,
        )
        .head(10)
    )

    st.dataframe(
        top_facilities,
        hide_index=True,
        use_container_width=True,
        column_config={
            "provider_name":
                "Facility",

            "total_direct_hprd":
                st.column_config.NumberColumn(
                    "Total HPRD",
                    format="%.2f",
                ),

            "rehospitalization_rate":
                st.column_config.NumberColumn(
                    "Rehospitalization %",
                    format="%.2f",
                ),
        },
    )


with rank_right:

    st.subheader(
        "Lowest Staffing Facilities"
    )

    bottom_facilities = (
        facility_df[
            [
                "provider_name",
                "total_direct_hprd",
                "rehospitalization_rate",
            ]
        ]
        .dropna(
            subset=[
                "total_direct_hprd"
            ]
        )
        .sort_values(
            "total_direct_hprd",
            ascending=True,
        )
        .head(10)
    )

    st.dataframe(
        bottom_facilities,
        hide_index=True,
        use_container_width=True,
        column_config={
            "provider_name":
                "Facility",

            "total_direct_hprd":
                st.column_config.NumberColumn(
                    "Total HPRD",
                    format="%.2f",
                ),

            "rehospitalization_rate":
                st.column_config.NumberColumn(
                    "Rehospitalization %",
                    format="%.2f",
                ),
        },
    )


st.divider()


# ============================================================
# FACILITY EXPLORER FILTERS
# ============================================================

st.header(
    "Facility Explorer"
)


filter1, filter2 = (
    st.columns(2)
)


ownership_options = (
    facility_df[
        "ownership_type"
    ]
    .dropna()
    .sort_values()
    .unique()
    .tolist()
)


with filter1:

    selected_ownership = (
        st.multiselect(
            "Ownership Type",
            ownership_options,
        )
    )


with filter2:

    minimum_rating = (
        st.slider(
            "Minimum Overall Rating",
            min_value=1,
            max_value=5,
            value=1,
        )
    )


filtered_facility_df = (
    facility_df.copy()
)


if selected_ownership:

    filtered_facility_df = (
        filtered_facility_df[
            filtered_facility_df[
                "ownership_type"
            ]
            .isin(
                selected_ownership
            )
        ]
    )


filtered_facility_df = (
    filtered_facility_df[
        (
            filtered_facility_df[
                "overall_rating"
            ].isna()
        )
        |
        (
            filtered_facility_df[
                "overall_rating"
            ]
            >= minimum_rating
        )
    ]
)


st.caption(
    f"{len(filtered_facility_df):,} "
    "facilities match the filters."
)


if filtered_facility_df.empty:

    st.warning(
        "No facilities match the selected filters."
    )

    st.stop()


filtered_facility_df[
    "display_name"
] = (
    filtered_facility_df[
        "provider_name"
    ]
    .fillna(
        "Unknown Facility"
    )
    + " — "
    + filtered_facility_df[
        "provnum"
    ]
)


selected_facility_name = (
    st.selectbox(
        "Select Facility",
        filtered_facility_df[
            "display_name"
        ]
        .tolist(),
    )
)


facility_row = (
    filtered_facility_df[
        filtered_facility_df[
            "display_name"
        ]
        == selected_facility_name
    ]
    .iloc[0]
)


# ============================================================
# FACILITY DETAILS
# ============================================================

facility_title = (
    facility_row[
        "provider_name"
    ]
)


if pd.isna(
    facility_title
):

    facility_title = (
        facility_row[
            "provnum"
        ]
    )


st.subheader(
    facility_title
)


info_left, info_right = (
    st.columns(2)
)


with info_left:

    st.markdown(
        f"""
**CMS Provider ID:** {facility_row['provnum']}

**City:** {facility_row['city']}

**County:** {facility_row['county']}

**Ownership:** {facility_row['ownership_type']}
"""
    )


with info_right:

    st.markdown(
        f"""
**Certified Beds:** {format_integer(facility_row['certified_beds'])}

**Reporting Days:** {format_integer(facility_row['reporting_days'])}

**Resident Days:** {format_integer(facility_row['total_resident_days'])}

**State:** {facility_row['state']}
"""
    )


# ============================================================
# FACILITY STAFFING KPIs
# ============================================================

st.markdown(
    "#### Staffing Metrics"
)


f1, f2, f3, f4, f5 = (
    st.columns(5)
)


f1.metric(
    "RN HPRD",
    format_number(
        facility_row[
            "rn_hprd"
        ]
    ),
)


f2.metric(
    "LPN HPRD",
    format_number(
        facility_row[
            "lpn_hprd"
        ]
    ),
)


f3.metric(
    "CNA HPRD",
    format_number(
        facility_row[
            "cna_hprd"
        ]
    ),
)


f4.metric(
    "Total HPRD",
    format_number(
        facility_row[
            "total_direct_hprd"
        ]
    ),
)


f5.metric(
    "Contract Labor",
    format_percent(
        facility_row[
            "contract_dependency_pct"
        ]
    ),
)


# ============================================================
# FACILITY QUALITY OUTCOMES
# ============================================================

st.markdown(
    "#### Quality Outcomes"
)


o1, o2, o3, o4 = (
    st.columns(4)
)


o1.metric(
    "Rehospitalization",
    format_percent(
        facility_row[
            "rehospitalization_rate"
        ]
    ),
)


o2.metric(
    "Short-Stay ED",
    format_percent(
        facility_row[
            "short_stay_ed_rate"
        ]
    ),
)


o3.metric(
    "Hospitalizations / 1,000",
    format_number(
        facility_row[
            "hospitalizations_per_1000"
        ]
    ),
)


o4.metric(
    "Long-Stay ED / 1,000",
    format_number(
        facility_row[
            "long_stay_ed_visits_per_1000"
        ]
    ),
)


# ============================================================
# FACILITY CMS RATINGS
# ============================================================

st.markdown(
    "#### CMS Ratings"
)


r1, r2, r3, r4 = (
    st.columns(4)
)


r1.metric(
    "Overall",
    format_number(
        facility_row[
            "overall_rating"
        ],
        1,
    ),
)


r2.metric(
    "Health Inspection",
    format_number(
        facility_row[
            "health_inspection_rating"
        ],
        1,
    ),
)


r3.metric(
    "Quality Measure",
    format_number(
        facility_row[
            "qm_rating"
        ],
        1,
    ),
)


r4.metric(
    "Staffing",
    format_number(
        facility_row[
            "staffing_rating"
        ],
        1,
    ),
)


# ============================================================
# DAILY FACILITY TREND
# ============================================================

st.markdown(
    "#### Daily Staffing Trend"
)


selected_provnum = (
    sql_escape(
        facility_row[
            "provnum"
        ]
    )
)


trend_query = f"""
SELECT
    workdate,
    mdscensus,
    rn_hprd,
    lpn_hprd,
    cna_hprd,
    total_direct_hprd,
    contract_dependency_pct

FROM healthcare_metrics.facility_metrics

WHERE state = '{safe_state}'
  AND provnum = '{selected_provnum}'

ORDER BY workdate
"""


trend_df = (
    run_athena_query(
        trend_query
    )
)


trend_df[
    "workdate"
] = pd.to_datetime(
    trend_df[
        "workdate"
    ],
    errors="coerce",
)


trend_df = (
    convert_numeric_columns(
        trend_df,
        [
            "mdscensus",
            "rn_hprd",
            "lpn_hprd",
            "cna_hprd",
            "total_direct_hprd",
            "contract_dependency_pct",
        ],
    )
)


trend_tab1, trend_tab2, trend_tab3 = (
    st.tabs(
        [
            "Staffing HPRD",
            "Total HPRD",
            "Contract Labor",
        ]
    )
)


with trend_tab1:

    chart_df = (
        trend_df[
            [
                "workdate",
                "rn_hprd",
                "lpn_hprd",
                "cna_hprd",
            ]
        ]
        .set_index(
            "workdate"
        )
    )

    st.line_chart(
        chart_df
    )


with trend_tab2:

    st.line_chart(
        trend_df[
            [
                "workdate",
                "total_direct_hprd",
            ]
        ]
        .set_index(
            "workdate"
        )
    )


with trend_tab3:

    st.line_chart(
        trend_df[
            [
                "workdate",
                "contract_dependency_pct",
            ]
        ]
        .set_index(
            "workdate"
        )
    )


with st.expander(
    "View Daily Facility Data"
):

    daily_display = (
        trend_df.copy()
    )

    for column in [
        "mdscensus",
        "rn_hprd",
        "lpn_hprd",
        "cna_hprd",
        "total_direct_hprd",
        "contract_dependency_pct",
    ]:

        if column in (
            daily_display.columns
        ):

            daily_display[
                column
            ] = (
                daily_display[
                    column
                ]
                .round(2)
            )

    st.dataframe(
        daily_display,
        hide_index=True,
        use_container_width=True,
    )


st.divider()


# ============================================================
# STAFFING & QUALITY INSIGHTS
# ============================================================

st.header(
    "Staffing & Quality Insights"
)


staffing_group_query = """
SELECT

    CASE

        WHEN total_direct_hprd < 3.0
            THEN '< 3.0 HPRD'

        WHEN total_direct_hprd < 4.0
            THEN '3.0 - 3.99 HPRD'

        WHEN total_direct_hprd < 5.0
            THEN '4.0 - 4.99 HPRD'

        ELSE '5.0+ HPRD'

    END AS staffing_group,

    COUNT(*) AS facilities,

    AVG(total_direct_hprd)
        AS avg_total_hprd,

    AVG(rehospitalization_rate)
        AS avg_rehospitalization_rate,

    AVG(short_stay_ed_rate)
        AS avg_short_stay_ed_rate,

    AVG(overall_rating)
        AS avg_overall_rating,

    AVG(staffing_rating)
        AS avg_staffing_rating

FROM healthcare_metrics.vw_facility_analytics

WHERE rehospitalization_rate IS NOT NULL

GROUP BY 1

ORDER BY avg_total_hprd
"""


staffing_group_df = (
    run_athena_query(
        staffing_group_query
    )
)


staffing_group_df = (
    convert_numeric_columns(
        staffing_group_df,
        [
            "facilities",
            "avg_total_hprd",
            "avg_rehospitalization_rate",
            "avg_short_stay_ed_rate",
            "avg_overall_rating",
            "avg_staffing_rating",
        ],
    )
)


# ============================================================
# CORRELATION DATA FROM ATHENA
# ============================================================

correlation_query = """
SELECT
    hprd_vs_rehospitalization,
    hprd_vs_short_stay_ed,
    hprd_vs_hospitalizations,
    hprd_vs_long_stay_ed,
    hprd_vs_overall_rating,
    hprd_vs_staffing_rating,
    rn_vs_rehospitalization,
    rn_vs_short_stay_ed,
    rn_vs_hospitalizations,
    rn_vs_long_stay_ed

FROM healthcare_metrics.vw_staffing_quality_correlations
"""


correlation_result = (
    run_athena_query(
        correlation_query
    )
)


correlation_result = (
    convert_numeric_columns(
        correlation_result,
        [
            "hprd_vs_rehospitalization",
            "hprd_vs_short_stay_ed",
            "hprd_vs_hospitalizations",
            "hprd_vs_long_stay_ed",
            "hprd_vs_overall_rating",
            "hprd_vs_staffing_rating",
            "rn_vs_rehospitalization",
            "rn_vs_short_stay_ed",
            "rn_vs_hospitalizations",
            "rn_vs_long_stay_ed",
        ],
    )
)


corr = (
    correlation_result.iloc[0]
)


correlation_df = pd.DataFrame(
    {
        "Relationship": [
            "Total HPRD vs Rehospitalization",
            "Total HPRD vs Short-Stay ED",
            "Total HPRD vs Hospitalizations",
            "Total HPRD vs Long-Stay ED",
            "Total HPRD vs Overall Rating",
            "Total HPRD vs Staffing Rating",
            "RN HPRD vs Rehospitalization",
            "RN HPRD vs Short-Stay ED",
            "RN HPRD vs Hospitalizations",
            "RN HPRD vs Long-Stay ED",
        ],

        "Correlation": [
            corr[
                "hprd_vs_rehospitalization"
            ],

            corr[
                "hprd_vs_short_stay_ed"
            ],

            corr[
                "hprd_vs_hospitalizations"
            ],

            corr[
                "hprd_vs_long_stay_ed"
            ],

            corr[
                "hprd_vs_overall_rating"
            ],

            corr[
                "hprd_vs_staffing_rating"
            ],

            corr[
                "rn_vs_rehospitalization"
            ],

            corr[
                "rn_vs_short_stay_ed"
            ],

            corr[
                "rn_vs_hospitalizations"
            ],

            corr[
                "rn_vs_long_stay_ed"
            ],
        ],
    }
)


# ============================================================
# INSIGHT TABS
# ============================================================

insight_tab1, insight_tab2, insight_tab3 = (
    st.tabs(
        [
            "Quality Outcomes",
            "CMS Ratings",
            "Correlation Summary",
        ]
    )
)


# ============================================================
# QUALITY OUTCOME GROUP CHART
# ============================================================

with insight_tab1:

    st.subheader(
        "Quality Outcomes by Staffing Level"
    )

    quality_group_chart = (
        staffing_group_df[
            [
                "staffing_group",
                "avg_rehospitalization_rate",
                "avg_short_stay_ed_rate",
            ]
        ]
        .set_index(
            "staffing_group"
        )
    )

    st.bar_chart(
        quality_group_chart
    )

    st.caption(
        "Lower values represent fewer "
        "rehospitalizations or emergency "
        "department visits."
    )


# ============================================================
# CMS RATING GROUP CHART
# ============================================================

with insight_tab2:

    st.subheader(
        "CMS Ratings by Staffing Level"
    )

    rating_group_chart = (
        staffing_group_df[
            [
                "staffing_group",
                "avg_overall_rating",
                "avg_staffing_rating",
            ]
        ]
        .set_index(
            "staffing_group"
        )
    )

    st.bar_chart(
        rating_group_chart
    )


# ============================================================
# CORRELATION SUMMARY
# ============================================================

with insight_tab3:

    st.subheader(
        "Correlation Summary"
    )

    st.dataframe(
        correlation_df,
        hide_index=True,
        use_container_width=True,
        column_config={
            "Correlation":
                st.column_config.NumberColumn(
                    "Correlation",
                    format="%.3f",
                )
        },
    )

    st.info(
        """
Higher staffing levels show a clearer positive
association with CMS ratings than with adverse
utilization outcomes.

Relationships with rehospitalization,
hospitalization, and emergency-department use are
generally negative but weak.

RN staffing shows a more consistently negative
association with adverse utilization outcomes than
the other staffing categories examined.

Correlation indicates association only and does not
establish causation.
"""
    )


# ============================================================
# GROUP DATA TABLE
# ============================================================

with st.expander(
    "View Staffing Group Analysis"
):

    staffing_group_display = (
        staffing_group_df.copy()
    )

    for column in [
        "avg_total_hprd",
        "avg_rehospitalization_rate",
        "avg_short_stay_ed_rate",
        "avg_overall_rating",
        "avg_staffing_rating",
    ]:

        staffing_group_display[
            column
        ] = (
            staffing_group_display[
                column
            ]
            .round(2)
        )

    st.dataframe(
        staffing_group_display,
        hide_index=True,
        use_container_width=True,
    )


st.divider()


# ============================================================
# ALL FACILITIES TABLE
# ============================================================

st.header(
    f"All Facilities — {selected_state}"
)


facility_table = (
    filtered_facility_df[
        [
            "provnum",
            "provider_name",
            "city",
            "ownership_type",
            "certified_beds",
            "overall_rating",
            "staffing_rating",
            "rn_hprd",
            "lpn_hprd",
            "cna_hprd",
            "total_direct_hprd",
            "contract_dependency_pct",
            "rehospitalization_rate",
            "short_stay_ed_rate",
            "hospitalizations_per_1000",
            "long_stay_ed_visits_per_1000",
        ]
    ]
    .copy()
)


for column in [
    "rn_hprd",
    "lpn_hprd",
    "cna_hprd",
    "total_direct_hprd",
    "contract_dependency_pct",
    "rehospitalization_rate",
    "short_stay_ed_rate",
    "hospitalizations_per_1000",
    "long_stay_ed_visits_per_1000",
]:

    facility_table[
        column
    ] = (
        facility_table[
            column
        ]
        .round(2)
    )


st.dataframe(
    facility_table,
    hide_index=True,
    use_container_width=True,
)


# ============================================================
# ABOUT / ARCHITECTURE
# ============================================================

st.divider()


with st.expander(
    "About This Data Platform"
):

    st.markdown(
        """
### Data Architecture

**Google Drive**  
↓  
**AWS Glue Workflow**  
↓  
**AWS Glue Python Shell Ingestion**  
↓  
**Amazon S3 Raw / Bronze**  
↓  
**AWS Glue Spark ETL**  
↓  
**Amazon S3 Curated / Parquet**  
↓  
**Amazon Athena**  
↓  
**Streamlit Dashboard**

### Primary Dataset

CMS Payroll-Based Journal (PBJ)
Daily Nurse Staffing — Q2 2024.

### Supporting Data

CMS Provider Information and CMS Claims-Based
Quality Measures are used to enrich staffing data
with facility characteristics, CMS ratings,
and resident quality outcomes.

### Core Project Metrics

1. Total Direct HPRD
2. RN HPRD
3. CNA HPRD
4. Contract Dependency %
5. Risk-Adjusted Rehospitalization Rate

### Supporting Indicators

- LPN HPRD
- Overall CMS Rating
- Staffing Rating
- Health Inspection Rating
- Quality Measure Rating
- Short-Stay ED Rate
- Hospitalizations per 1,000 Long-Stay Resident Days
- Long-Stay ED Visits per 1,000 Resident Days

### Analytical Interpretation

Higher staffing levels are associated more clearly
with better CMS ratings than with reduced adverse
utilization outcomes.

Observed correlations with hospitalization and
emergency-department measures are generally negative
but weak.

RN staffing shows the most consistently negative
association with adverse utilization outcomes among
the staffing categories examined.

These analyses are descriptive and associative.
They should not be interpreted as causal evidence.
"""
    )


st.caption(
    "Healthcare Metrics Data Engineering Project | "
    "CMS PBJ Q2 2024 | "
    "AWS Glue • Amazon S3 • Athena • Streamlit"
)