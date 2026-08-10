"""Builds the two Apache Spark notebooks via nbformat."""

from nbformat.v4 import new_notebook, new_markdown_cell, new_code_cell
import nbformat as nbf
import os

BASE = os.path.dirname(os.path.abspath(__file__))
NB_DIR = os.path.join(BASE, "notebooks")
os.makedirs(NB_DIR, exist_ok=True)


def md(src):
    return new_markdown_cell(src)


def code(src):
    return new_code_cell(src)


# ---------------------------------------------------------------------------
# NOTEBOOK 1 : Data Preprocessing
# ---------------------------------------------------------------------------
preprocess_cells = [
    md("""# 01 - Apache Spark : Data Preprocessing

This notebook uses **Apache Spark (PySpark)** to load and clean the large traffic datasets:

| Dataset             | Rows        | Size  |
|---------------------|-------------|-------|
| `traffic_data.csv`  | 1,500,000   | ~105 MB |
| `area_monitor.csv`  | 1,000,000   | ~55 MB  |

Raw CSV files are too large for Pandas on a single machine - Spark distributes the
work across multiple CPU cores. The cleaned result is written to **Parquet**
(columnar, compressed) so the main analysis notebook can read it instantly.

> Run cells from top to bottom. The Spark session is created once and stopped at the end.
"""),

    md("""## 1. Spark Session Setup

`master("local[*]")` uses every available CPU core. A memory fraction is reserved so
the JVM does not get killed during shuffles."""),
    code('''# 1. Spark session setup
from pyspark.sql import SparkSession
from pyspark import SparkConf

conf = SparkConf().setAppName("traffic_preprocessing").setMaster("local[*]")
conf.set("spark.sql.adaptive.enabled", "true")
conf.set("spark.sql.shuffle.partitions", "8")   # cores * 2 is a good default

spark = SparkSession.builder.config(conf=conf).getOrCreate()
spark.sparkContext.setLogLevel("WARN")          # reduce log noise

print("Spark version   :", spark.version)
print("Default parallelism:", spark.sparkContext.defaultParallelism)'''),

    md("""## 2. Load Raw CSVs

Spark reads the file lazily - no data is loaded until an **action** runs
(`show`, `count`, `write`, ...). We also inspect the inferred schema."""),
    code('''# 2. Resolve paths so the notebook works from any launch folder
import os
ROOT = os.path.abspath(".") if os.path.isdir("data") else os.path.abspath("..")
DATA_DIR = os.path.join(ROOT, "data")
print("Data directory:", DATA_DIR)

traffic_raw = spark.read.csv(
    os.path.join(DATA_DIR, "traffic_data.csv"), header=True, inferSchema=True,
)
monitor_raw = spark.read.csv(
    os.path.join(DATA_DIR, "area_monitor.csv"), header=True, inferSchema=True,
)

print("traffic_data rows  :", traffic_raw.count())
print("area_monitor rows  :", monitor_raw.count())

print("\\n--- traffic_data schema ---")
traffic_raw.printSchema()
print("\\n--- area_monitor schema ---")
monitor_raw.printSchema()'''),

    md("""### 2.1 Peek at the Data"""),
    code('''# Sample rows
traffic_raw.show(5, truncate=False)
print()
monitor_raw.show(5, truncate=False)'''),

    md("""## 3. Data Quality Checks

### 3.1 Missing Values (per column)"""),
    code('''# 3.1 Null counts across all columns (runs one job)
def null_summary(df, name):
    rows = df.select(
        [fn.sum(fn.col(c).isNull().cast("int")).alias(c) for c in df.columns]
    )
    nulls = rows.collect()[0].asDict()
    print(f"--- {name} nulls ---")
    for col, cnt in nulls.items():
        print(f"  {col:20s}: {cnt:,}")
    return nulls

from pyspark.sql import functions as fn

t_nulls = null_summary(traffic_raw, "traffic_data")
m_nulls = null_summary(monitor_raw, "area_monitor")'''),

    md("""### 3.2 Duplicate Records"""),
    code('''# Count duplicates by the primary key
dup_t = traffic_raw.groupBy("event_id").count().filter("count > 1").count()
dup_m = monitor_raw.groupBy("monitor_id").count().filter("count > 1").count()
print("Duplicate event_id in traffic_data  :", dup_t)
print("Duplicate monitor_id in area_monitor:", dup_m)

# Drop any exact duplicate rows
traffic_raw = traffic_raw.dropDuplicates()
monitor_raw = monitor_raw.dropDuplicates()'''),

    md("""### 3.3 Range Check & Outliers (e.g. speed must be 0-150 km/h)"""),
    code('''# Speed outliers outside a sane range
bad_speed = traffic_raw.filter(~fn.col("speed_kmh").between(0, 150)).count()
print("Speed values outside 0-150 km/h:", bad_speed)

# traffic_volume must be >= 0
bad_vol = traffic_raw.filter(fn.col("traffic_volume") < 0).count()
print("Negative traffic volumes       :", bad_vol)'''),

    md("""## 4. Cleaning & Feature Engineering

We will:
1. Fill/remove nulls,
2. Cast types,
3. Add a `time_of_day` bucket and a `speed_category` label,
4. Drop columns we do not need."""),
    code('''# 4.1 Handle nulls & outliers
traffic = traffic_raw.filter(
    fn.col("speed_kmh").between(0, 150)
    & fn.col("traffic_volume").isNotNull()
    & fn.col("timestamp").isNotNull()
)
# Monitor: keep only active sensors
monitor = monitor_raw.filter(fn.col("status") == "active").drop("status")

print("After cleaning traffic rows:", traffic.count())
print("After cleaning monitor rows:", monitor.count())'''),
    code('''# 4.2 Feature engineering
traffic = (
    traffic
    .withColumn("date", fn.to_date("timestamp"))
    .withColumn("day_of_week", fn.date_format("timestamp", "EEEE"))
    .withColumn("time_of_day",
        fn.when(fn.hour("timestamp").between(0, 5), "night")
          .when(fn.hour("timestamp").between(6, 11), "morning")
          .when(fn.hour("timestamp").between(12, 16), "afternoon")
          .otherwise("evening"))
    .withColumn("speed_category",
        fn.when(fn.col("speed_kmh") < 20, "slow")
          .when(fn.col("speed_kmh") < 40, "moderate")
          .when(fn.col("speed_kmh") < 65, "normal")
          .otherwise("fast"))
)

traffic.select(
    "event_id", "timestamp", "date", "day_of_week",
    "time_of_day", "speed_kmh", "speed_category",
).show(8, truncate=False)'''),

    md("""## 5. Descriptive Statistics

`summary()` gives min / max / mean / std on numeric columns - all distributed by Spark."""),
    code('''# 5. Summary statistics
traffic.select("speed_kmh", "traffic_volume").summary(
    "count", "mean", "stddev", "min", "max"
).show()

print()
traffic.groupBy("congestion_level").count().show()
traffic.groupBy("vehicle_type").count().show()'''),

    md("""## 6. Cache & Persist

The clean frame is **cached** in memory so repeated actions (multiple queries in the
next notebook) do not re-scan the 105 MB CSV every time."""),
    code('''# 6. Cache the cleaned traffic frame
traffic = traffic.cache()
print("Cached partitions:", traffic.rdd.getNumPartitions())'''),

    md("""## 7. Write Cleaned Data to Parquet

Parquet is columnar + compressed -> much faster to re-read than CSV."""),
    code('''# 7. Persist cleaned data as parquet
PROCESSED_DIR = os.path.join(DATA_DIR, "processed")
traffic.write.mode("overwrite").parquet(os.path.join(PROCESSED_DIR, "traffic_clean.parquet"))
monitor.write.mode("overwrite").parquet(os.path.join(PROCESSED_DIR, "area_monitor_clean.parquet"))
print("Wrote parquet files under", PROCESSED_DIR)'''),

    md("""## 8. Summary

- Loaded **2.5M** rows from CSV with Spark (no Pandas, no memory issues).
- Found & handled nulls, duplicates and speed outliers.
- Engineered `time_of_day`, `day_of_week`, `speed_category`.
- Cached in memory and persisted to Parquet.

The cleaned datasets are now ready for the main analysis notebook."""),

    code('''# 9. Stop the Spark session (releases JVM memory)
spark.stop()
print("Spark session stopped.")'''),
]

nb1 = new_notebook(cells=preprocess_cells, metadata={"kernelspec": {"name": "python3", "display_name": "Python 3", "language": "python"}, "language_info": {"name": "python", "version": "3.12.3"}})

# ---------------------------------------------------------------------------
# NOTEBOOK 2 : Main Analysis
# ---------------------------------------------------------------------------
main_cells = [
    md("""# 02 - Apache Spark : Main Analysis

The heavy lifting already happened in `01_data_preprocessing.ipynb`. Here we read the
**Parquet** files and run distributed analytics with PySpark **DataFrame API** and
**Spark SQL**, then push only the small aggregated results to Pandas for plotting.

Goals:
- Aggregations (`groupBy`, `agg`) over 1.5M traffic records
- SQL queries via temporary views
- **Join** traffic_data <-> area_monitor on `area_id`
- **Window functions** (moving averages, ranking)
- Pivot tables
- Matplotlib visualizations (on aggregated samples)
"""),

    md("""## 1. Session & Load Parquet"""),
    code('''# 1. Spark session + read preprocessed parquet
from pyspark.sql import SparkSession, functions as fn, Window
import os

spark = SparkSession.builder.appName("traffic_analysis").master("local[*]").getOrCreate()
spark.sparkContext.setLogLevel("WARN")

# Resolve paths so the notebook works from any launch folder
ROOT = os.path.abspath(".") if os.path.isdir("data") else os.path.abspath("..")
DATA_DIR = os.path.join(ROOT, "data")
PROCESSED_DIR = os.path.join(DATA_DIR, "processed")
OUTPUT_DIR = os.path.join(ROOT, "outputs")
os.makedirs(OUTPUT_DIR, exist_ok=True)

traffic = spark.read.parquet(os.path.join(PROCESSED_DIR, "traffic_clean.parquet"))
monitor = spark.read.parquet(os.path.join(PROCESSED_DIR, "area_monitor_clean.parquet"))

print("traffic rows :", traffic.count())
print("monitor rows :", monitor.count())
print()
traffic.printSchema()'''),

    md("""## 2. SQL Views"""),
    code('''# 2. Register temp views for Spark SQL
traffic.createOrReplaceTempView("traffic")
monitor.createOrReplaceTempView("monitor")

spark.sql("SELECT * FROM traffic LIMIT 5").show(truncate=False)'''),

    md("""## 3. Aggregations

### 3.1 Average speed & volume by area (DataFrame API)"""),
    code('''# 3.1 Aggregations
area_stats = (
    traffic.groupBy("area_id")
    .agg(
        fn.round(fn.avg("speed_kmh"), 1).alias("avg_speed"),
        fn.sum("traffic_volume").alias("total_volume"),
        fn.count("event_id").alias("event_count"),
    )
    .orderBy(fn.col("total_volume").desc())
)
area_stats.show(10)'''),

    md("""### 3.2 Top congested areas by SQL"""),
    code('''# 3.2 Spark SQL
spark.sql("""
    SELECT area_id,
           ROUND(AVG(speed_kmh), 1) AS avg_speed,
           SUM(traffic_volume)      AS total_volume
    FROM traffic
    GROUP BY area_id
    ORDER BY avg_speed ASC
    LIMIT 10
""").show()'''),

    md("""### 3.3 Hourly traffic profile"""),
    code('''# 3.3 Traffic volume by hour
hourly = (
    traffic.groupBy("hour")
    .agg(fn.round(fn.avg("traffic_volume"), 1).alias("avg_volume"),
         fn.avg("speed_kmh").alias("avg_speed"))
    .orderBy("hour")
)
hourly.show(10)'''),

    md("""## 4. Joins

### 4.1 Enrich traffic with area metadata"""),
    code('''# 4.1 Inner join on area_id
enriched = traffic.join(monitor, "area_id")

print("Enriched rows:", enriched.count())
enriched.select("event_id", "area_id", "city", "speed_kmh", "traffic_volume") \\
        .show(5, truncate=False)

# What fraction of traffic rows have no matching area?
no_match = traffic.join(monitor, "area_id", "left_anti").count()
print("Traffic rows without an area match:", no_match)'''),

    md("""### 4.2 City-level summary"""),
    code('''# 4.2 Group after join
city_stats = (
    enriched.groupBy("city")
    .agg(fn.round(fn.avg("speed_kmh"), 1).alias("avg_speed"),
         fn.sum("traffic_volume").alias("total_volume"),
         fn.countDistinct("area_id").alias("areas"))
    .orderBy(fn.col("total_volume").desc())
)
city_stats.show()'''),

    md("""## 5. Window Functions

### 5.1 Ranking - most congested area per day"""),
    code('''# 5.1 Rank areas by congestion within each day
w = Window.partitionBy("date").orderBy(fn.col("traffic_volume").desc())

ranked = traffic.select(
    "date", "area_id", "traffic_volume",
    fn.row_number().over(w).alias("rank_in_day")
)
ranked.filter("rank_in_day <= 3").show(15, truncate=False)'''),

    md("""### 5.2 Moving average of speed (7-day window per area)"""),
    code('''# 5.2 Rolling 7-row moving average of daily avg speed per area
daily = (
    traffic.groupBy("area_id", "date")
    .agg(fn.avg("speed_kmh").alias("day_speed"))
)

w2 = Window.partitionBy("area_id").orderBy("date").rowsBetween(-6, 0)
moving = daily.withColumn(
    "ma7_speed", fn.round(fn.avg("day_speed").over(w2), 2)
)
moving.filter("area_id = 1").orderBy("date").show(10)'''),

    md("""## 6. Pivot - traffic volume by hour x day_of_week"""),
    code('''# 6. Pivot hourly volume across weekdays
pivot = (
    traffic.groupBy("hour")
    .pivot("day_of_week", ["Monday", "Tuesday", "Wednesday", "Thursday", "Friday", "Saturday", "Sunday"])
    .agg(fn.round(fn.avg("traffic_volume"), 1))
    .orderBy("hour")
)
pivot.show(24, truncate=False)'''),

    md("""## 7. Push to Pandas & Visualize

Only the small aggregated results (hundreds of rows) are moved to Pandas for plotting."""),
    code('''# 7.1 Convert small results to Pandas
hourly_pd = hourly.toPandas()
city_pd   = city_stats.toPandas()
area_pd   = area_stats.limit(15).toPandas()
print("Pandas frames ready:", hourly_pd.shape, city_pd.shape, area_pd.shape)'''),
    code('''# 7.2 Imports & style
import matplotlib.pyplot as plt
import pandas as pd

plt.style.use("seaborn-v0_8-whitegrid")
fig, axes = plt.subplots(2, 2, figsize=(14, 10))
fig.suptitle("Traffic Analytics powered by Apache Spark", fontsize=15)

# (a) Hourly volume profile
axes[0, 0].plot(hourly_pd["hour"], hourly_pd["avg_volume"], marker="o", color="#1f77b4")
axes[0, 0].set_title("Average traffic volume by hour")
axes[0, 0].set_xlabel("Hour"); axes[0, 0].set_ylabel("Avg volume")

# (b) City-level totals
axes[0, 1].bar(city_pd["city"], city_pd["total_volume"], color="#ff7f0e")
axes[0, 1].set_title("Total traffic volume by city")
axes[0, 1].tick_params(axis="x", rotation=30)

# (c) Top areas by volume
axes[1, 0].barh(area_pd["area_id"][::-1], area_pd["total_volume"][::-1], color="#2ca02c")
axes[1, 0].set_title("Top 15 areas by total volume")

# (d) Speed vs volume scatter
sample = hourly_pd
axes[1, 1].scatter(sample["avg_volume"], sample["avg_speed"], s=60, color="#d62728")
axes[1, 1].set_title("Avg speed vs avg volume")
axes[1, 1].set_xlabel("Avg volume"); axes[1, 1].set_ylabel("Avg speed")

plt.tight_layout()
plt.savefig(os.path.join(OUTPUT_DIR, "analysis_plots.png"), dpi=120, bbox_inches="tight")
plt.show()
print("Saved figure to", os.path.join(OUTPUT_DIR, "analysis_plots.png"))'''),

    md("""## 8. Save Final Results

Write the enriched, aggregated tables back out so the whole pipeline is reproducible."""),
    code('''# 8. Persist results (coalesce to a single file per result)
city_stats.coalesce(1).write.mode("overwrite").csv(os.path.join(OUTPUT_DIR, "city_stats.csv"), header=True)
area_stats.coalesce(1).write.mode("overwrite").csv(os.path.join(OUTPUT_DIR, "area_stats.csv"), header=True)
pivot.coalesce(1).write.mode("overwrite").csv(os.path.join(OUTPUT_DIR, "hour_weekday_pivot.csv"), header=True)
print("Results written under", OUTPUT_DIR)'''),

    md("""## Summary

All heavy computation (aggregations, joins, window functions, pivots over **1.5M**
rows) ran in-memory on Spark's distributed engine. Only the tiny results were pulled
into Pandas for plotting - this is the correct pattern for large data.

> **Next steps:** increase parallelism with a cluster (`spark://...`), try `spark-sql`,
> or run MLlib (`spark.ml`) models on the cleaned features."""),
    code('''spark.stop()
print("Spark session stopped.")'''),
]

nb2 = new_notebook(cells=main_cells, metadata={"kernelspec": {"name": "python3", "display_name": "Python 3", "language": "python"}, "language_info": {"name": "python", "version": "3.12.3"}})

with open(os.path.join(NB_DIR, "01_data_preprocessing.ipynb"), "w") as f:
    nbf.write(nb1, f)
with open(os.path.join(NB_DIR, "02_main_analysis.ipynb"), "w") as f:
    nbf.write(nb2, f)

print("Wrote 2 notebooks to", NB_DIR)
