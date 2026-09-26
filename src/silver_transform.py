import os
import sys
import json
from datetime import datetime, timezone

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, when, lit, trim, upper


# =========================================================
# Python environment
# =========================================================

os.environ["PYSPARK_PYTHON"] = sys.executable
os.environ["PYSPARK_DRIVER_PYTHON"] = sys.executable


# =========================================================
# Configuration
# =========================================================

BRONZE_FILE = "bronze/university_chapters_raw.json"

SILVER_PATH = "silver/university_chapters"

GOLD_PATH = "gold/university_chapters"

QUARANTINE_PATH = "quarantine/university_chapters"


# =========================================================
# Unique pipeline run ID
# =========================================================

INGEST_RUN_ID = datetime.now(timezone.utc).strftime(
    "%Y%m%dT%H%M%SZ"
)


# =========================================================
# Create Spark Session
# =========================================================

spark = (
    SparkSession.builder
    .master("local[*]")
    .appName("UniversityChaptersSilver")
    .getOrCreate()
)


# =========================================================
# Read Bronze JSON
# =========================================================

with open(BRONZE_FILE, "r", encoding="utf-8") as f:
    bronze_data = json.load(f)


# =========================================================
# Extract API records
# =========================================================

records = bronze_data["data"]["features"]


# =========================================================
# Flatten attributes + geometry
# =========================================================

rows = []

for record in records:

    attributes = record.get("attributes", {})
    geometry = record.get("geometry") or {}

    rows.append({
        "chapter_id": attributes.get("ChapterID"),
        "chapter_name": attributes.get("University_Chapter"),
        "city": attributes.get("City"),
        "state": attributes.get("State"),
        "longitude": geometry.get("x"),
        "latitude": geometry.get("y"),
        "raw_payload": json.dumps(record),
        "ingest_run_id": INGEST_RUN_ID
    })


# =========================================================
# Create Spark DataFrame
# =========================================================

df = spark.createDataFrame(rows)


# =========================================================
# Explicit data types
# =========================================================

df = (
    df
    .withColumn(
        "chapter_id",
        col("chapter_id").cast("string")
    )
    .withColumn(
        "chapter_name",
        col("chapter_name").cast("string")
    )
    .withColumn(
        "city",
        col("city").cast("string")
    )
    .withColumn(
        "state",
        upper(trim(col("state")))
    )
    .withColumn(
        "longitude",
        col("longitude").cast("double")
    )
    .withColumn(
        "latitude",
        col("latitude").cast("double")
    )
)


# =========================================================
# Scope: CA / OR / WA only
# =========================================================

df = df.filter(
    col("state").isin("CA", "OR", "WA")
)


# =========================================================
# Input count
# =========================================================

rows_in = df.count()


# =========================================================
# DQ-Q1
# Invalid coordinates -> QUARANTINE
# =========================================================

valid_coordinates = (
    col("longitude").isNotNull()
    & col("latitude").isNotNull()
    & col("longitude").between(-180, 180)
    & col("latitude").between(-90, 90)
)


quarantine_df = (
    df
    .filter(~valid_coordinates)
    .withColumn(
        "quarantine_reason",
        lit("INVALID_COORDINATES")
    )
)


# =========================================================
# Valid records
# =========================================================

clean_df = df.filter(valid_coordinates)


# =========================================================
# SILVER
# Cleaned + typed + deduplicated
# =========================================================

silver_df = (
    clean_df
    .dropDuplicates(["chapter_id"])
    .select(
        "chapter_id",
        "chapter_name",
        "city",
        "state",
        "longitude",
        "latitude",
        "raw_payload",
        "ingest_run_id"
    )
)


# =========================================================
# DQ-W1
# Missing / blank / UNKNOWN city
# =========================================================

missing_city = (
    col("city").isNull()
    | (trim(col("city")) == "")
    | (upper(trim(col("city"))) == "UNKNOWN")
)


# =========================================================
# GOLD DATA PRODUCT
# =========================================================

gold_df = (
    silver_df
    .withColumn(
        "dq_status",
        when(
            missing_city,
            lit("WARNING")
        ).otherwise(
            lit("OK")
        )
    )
    .withColumn(
        "dq_warnings",
        when(
            missing_city,
            lit("MISSING_OR_UNKNOWN_CITY")
        ).otherwise(
            lit("")
        )
    )
    .select(
        "chapter_id",
        "chapter_name",
        "city",
        "state",
        "longitude",
        "latitude",
        "dq_status",
        "dq_warnings",
        "ingest_run_id"
    )
)


# =========================================================
# Data Quality Counts
# =========================================================

rows_quarantined = quarantine_df.count()

rows_warned = (
    gold_df
    .filter(
        col("dq_status") == "WARNING"
    )
    .count()
)

rows_ok = (
    gold_df
    .filter(
        col("dq_status") == "OK"
    )
    .count()
)


# =========================================================
# Convert Spark DataFrames to Python records
#
# Python JSON writing is used because this assignment
# runs locally on Windows without Hadoop winutils.
# =========================================================

silver_records = [
    row.asDict()
    for row in silver_df.collect()
]


gold_records = [
    row.asDict()
    for row in gold_df.collect()
]


quarantine_records = [
    row.asDict()
    for row in quarantine_df.select(
        "chapter_id",
        "chapter_name",
        "city",
        "state",
        "longitude",
        "latitude",
        "quarantine_reason",
        "ingest_run_id",
        "raw_payload"
    ).collect()
]


# =========================================================
# Helper function
# =========================================================

def write_json_output(path, records):

    os.makedirs(path, exist_ok=True)

    output_file = os.path.join(
        path,
        "part-00000.json"
    )

    with open(
        output_file,
        "w",
        encoding="utf-8"
    ) as f:

        for record in records:

            f.write(
                json.dumps(
                    record,
                    ensure_ascii=False
                )
                + "\n"
            )


# =========================================================
# Write SILVER
# =========================================================

write_json_output(
    SILVER_PATH,
    silver_records
)


# =========================================================
# Write GOLD
# =========================================================

write_json_output(
    GOLD_PATH,
    gold_records
)


# =========================================================
# Write QUARANTINE
# =========================================================

write_json_output(
    QUARANTINE_PATH,
    quarantine_records
)


# =========================================================
# Pipeline Summary
# =========================================================

print()
print("========================================")
print("University Chapters Pipeline")
print("========================================")

print(f"ingest_run_id    : {INGEST_RUN_ID}")
print(f"rows_in          : {rows_in}")
print(f"silver_rows      : {silver_df.count()}")
print(f"rows_quarantined : {rows_quarantined}")
print(f"rows_warned      : {rows_warned}")
print(f"rows_ok          : {rows_ok}")

print("========================================")
print("Silver transformation completed")
print("Silver output created")
print("Gold output created")
print("Quarantine output created")
print("========================================")


# =========================================================
# Stop Spark
# =========================================================

spark.stop()