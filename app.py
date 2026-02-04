import streamlit as st
from pyspark.sql import SparkSession
import pandas as pd

from streamlit_autorefresh import st_autorefresh

st.set_page_config(page_title="Weather Analytics", layout="wide")

st_autorefresh(interval=300000, key="weather_refresh")
# -----------------------------
# Spark session (read-only)
# -----------------------------
@st.cache_resource
def get_spark():
    return (
        SparkSession.builder
        .appName("WeatherStreamlit")
        .master("local[*]")
        .getOrCreate()
    )

spark = get_spark()

import subprocess

def pipeline_done():
    cmd = ["hdfs", "dfs", "-test", "-e", "/weather/_SUCCESS"]
    return subprocess.call(cmd) == 0

if pipeline_done():
    st.success("✅ Data pipeline completed")
else:
    st.warning("⏳ Pipeline still running")
    st.stop()



# -----------------------------
# Helper: read parquet from HDFS
# -----------------------------
@st.cache_data
def load_data(hdfs_path):
    df = spark.read.parquet(hdfs_path)
    return df.toPandas()

# -----------------------------
# UI
# -----------------------------

st.title("🌦 Weather Analytics Dashboard")

menu = st.sidebar.selectbox(
    "Select Analysis",
    [
        "Yearly Summary",
        "Monthly Summary",
        "Weekly Summary",
        "Seasonal Temperature",
        "Seasonal Rainfall",
        "Seasonal Wind"
    ]
)

# -----------------------------
# Yearly
# -----------------------------
if menu == "Yearly Summary":
    df = load_data("hdfs://localhost:9000/weather/analytics/city_summary_yearly")
    city = st.selectbox("City", sorted(df["city"].unique()))
    st.dataframe(df[df["city"] == city], use_container_width=True)

    plot_df = df[df["city"] == city].sort_values("year")
    plot_df = plot_df.set_index("year")

    st.subheader(f"📈 Yearly Avg Temperature — {city}")
    st.line_chart(plot_df[["avg_max_temp", "avg_min_temp"]])

# -----------------------------
# Monthly
# -----------------------------
elif menu == "Monthly Summary":
    df = load_data("hdfs://localhost:9000/weather/analytics/city_summary_monthly")
    city = st.selectbox("City", sorted(df["city"].unique()))
    year = st.selectbox("Year", sorted(df["year"].unique()))


    st.dataframe(df[(df["city"] == city) & (df["year"] == year)]
, use_container_width=True)
    plot_df = df[(df["city"] == city) & (df["year"] == year)].sort_values("month")
    plot_df = plot_df.set_index("month")

    st.subheader(f"📈 Monthly Avg Temperature — {city} ({year})")
    st.line_chart(plot_df[["avg_max_temp", "avg_min_temp"]])

# -----------------------------
# Weeklys
# -----------------------------
elif menu == "Weekly Summary":
    df = load_data("hdfs://localhost:9000/weather/analytics/city_summary_weekly")
    city = st.selectbox("City", sorted(df["city"].unique()))
    year = st.selectbox("Year", sorted(df["year"].unique()))
    st.dataframe(df[df["city"] == city], use_container_width=True)

    plot_df = df[(df["city"] == city) & (df["year"] == year)].sort_values("weekofyear(date)")
    plot_df = plot_df.set_index("weekofyear(date)")

    st.subheader(f"📈 Weekly Avg Temperature — {city} ({year})")
    st.line_chart(plot_df[["avg_max_temp", "avg_min_temp"]])
# -----------------------------
# Seasonal Temperature
# -----------------------------
elif menu == "Seasonal Temperature":
    df = load_data("hdfs://localhost:9000/weather/analytics/city_seasonal_temp")
    city = st.selectbox("City", sorted(df["city"].unique()))
    year = st.selectbox("Year", sorted(df["year"].unique()))
    st.dataframe(df[df["city"] == city], use_container_width=True)

    plot_df = df[(df["city"] == city) & (df["year"] == year)]

    season_order = ["Winter", "Summer", "Monsoon", "Post-Monsoon"]
    plot_df["season"] = pd.Categorical(
        plot_df["season"],
        categories=season_order,
        ordered=True
    )

    plot_df = plot_df.sort_values("season").set_index("season")

    st.subheader(f"🌡 Seasonal Temperature — {city} ({year})")
    st.line_chart(plot_df[["avg_max_temp", "avg_min_temp"]])


# -----------------------------
# Seasonal Rainfall
# -----------------------------
elif menu == "Seasonal Rainfall":
    df = load_data("hdfs://localhost:9000/weather/analytics/city_seasonal_rainfall")
    city = st.selectbox("City", sorted(df["city"].unique()))
    year = st.selectbox("Year", sorted(df["year"].unique()))

    st.dataframe(df[df["city"] == city], use_container_width=True)

    plot_df = df[
        (df["city"] == city) & (df["year"] == year)
    ]

    season_order = ["Winter", "Summer", "Monsoon", "Post-Monsoon"]
    plot_df["season"] = pd.Categorical(
        plot_df["season"],
        categories=season_order,
        ordered=True
    )

    plot_df = plot_df.sort_values("season").set_index("season")

    st.subheader(f"🌧 Seasonal Rainfall — {city} ({year})")
    st.bar_chart(plot_df["seasonal_rainfall"])



# -----------------------------
# Seasonal Wind
# -----------------------------
elif menu == "Seasonal Wind":
    df = load_data("hdfs://localhost:9000/weather/analytics/city_seasonal_wind")
    city = st.selectbox("City", sorted(df["city"].unique()))
    year = st.selectbox("Year", sorted(df["year"].unique()))
    st.dataframe(df[df["city"] == city], use_container_width=True)

    plot_df = df[
        (df["city"] == city) & (df["year"] == year)
    ]

    season_order = ["Winter", "Summer", "Monsoon", "Post-Monsoon"]
    plot_df["season"] = pd.Categorical(
        plot_df["season"],
        categories=season_order,
        ordered=True
    )

    plot_df = plot_df.sort_values("season").set_index("season")

    st.subheader(f"💨 Seasonal Wind — {city} ({year})")
    st.line_chart(plot_df[["avg_wind_speed", "max_wind_gust"]])

