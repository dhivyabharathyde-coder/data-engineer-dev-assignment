# University Chapters Data Engineering Assignment

## Overview

This project implements a small Azure-oriented medallion data engineering pipeline using Python, PySpark, and pytest.

The pipeline retrieves university chapter data from a public ArcGIS FeatureServer API, stores the raw API response as Bronze data, transforms and validates the data using PySpark, and publishes a consumer-facing Gold data product.

The pipeline implements:

* Bronze raw ingestion
* Silver typed and deduplicated transformation
* Gold consumer data product
* Data quality quarantine and warning handling
* Automated tests
* Data Product Contract
* Local Spark execution using an ADLS-style folder layout

## Source API

Public ArcGIS FeatureServer:

`https://services2.arcgis.com/5I7u4SJE1vUr79JC/arcgis/rest/services/UniversityChapters_Public/FeatureServer/0`

The pipeline uses only the following states:

* CA - California
* OR - Oregon
* WA - Washington

OR and WA are allowed to have zero records because an empty result is a valid source-data condition.

## Architecture

```text
                 Public ArcGIS FeatureServer
                           |
                           v
                    Python Ingestion
                           |
                           v
                       BRONZE
                 Raw API JSON + metadata
                           |
                           v
                      PySpark
                           |
              +------------+------------+
              |                         |
              v                         v
        QUARANTINE                   SILVER
   Invalid coordinates        Typed + deduplicated
                                      |
                                      v
                                    GOLD
                           Consumer Data Product
```

### Medallion Layers

**Bronze**

Raw or near-raw API payload with ingestion metadata.

**Silver**

Typed, flattened and deduplicated chapter-level data. Invalid coordinate records are removed from the valid Silver/Gold flow and written to quarantine.

**Gold**

Consumer-facing data product containing clean and warning rows only. Quarantined records never appear in Gold.

**Quarantine**

Records failing the hard data-quality rule are stored separately with a reason code and raw payload for debugging.

## Pipeline

```text
API
 |
 v
Bronze
 |
 v
PySpark Silver Transformation
 |
 +---- invalid coordinates ----> Quarantine
 |
 v
Silver
 |
 v
Gold
```

## Data Quality Rules

### DQ-Q1: Invalid Coordinates — Hard Failure

A record is quarantined when:

* longitude is null
* latitude is null
* longitude is outside -180 to 180
* latitude is outside -90 to 90
* coordinates are non-numeric after type conversion

The quarantine record contains:

* `chapter_id`
* `chapter_name`
* `city`
* `state`
* `longitude`
* `latitude`
* `quarantine_reason`
* `ingest_run_id`
* `raw_payload`

The reason code is:

`INVALID_COORDINATES`

Quarantined records do not enter the Gold data product.

### DQ-W1: Missing City — Warning

If City is:

* null
* blank
* `UNKNOWN` (case insensitive)

the record is not quarantined.

It continues to Gold with:

```text
dq_status = WARNING
dq_warnings = MISSING_OR_UNKNOWN_CITY
```

Clean records use:

```text
dq_status = OK
dq_warnings = empty
```

## Data Quality Metrics

Each pipeline run logs:

* `rows_in`
* `rows_quarantined`
* `rows_warned`
* `rows_ok`

The live API may contain only clean records. Synthetic fixture data is used to test both the quarantine and warning paths.

## Data Product Contract

### Product

**Name:** University Chapters Data Product

**Owner:** Data Engineering Team

**Consumer use cases:** Analytics, reporting, and university chapter reporting.

### Interface

**Gold path:**

```text
gold/university_chapters/
```

**Grain:**

One row per `chapter_id`.

### Gold Schema

| Column        | Type   | Description                     |
| ------------- | ------ | ------------------------------- |
| chapter_id    | string | Stable chapter business key     |
| chapter_name  | string | University chapter display name |
| city          | string | City                            |
| state         | string | USPS two-letter state code      |
| longitude     | double | WGS84 longitude                 |
| latitude      | double | WGS84 latitude                  |
| dq_status     | string | `OK` or `WARNING`               |
| dq_warnings   | string | Data quality warning reason     |
| ingest_run_id | string | Pipeline execution identifier   |

### Freshness

Intended SLA:

**Data refreshed daily by 06:00 UTC.**

The current implementation is manually runnable for this take-home assignment.

### Quality Contract

* DQ-Q1 invalid coordinates are quarantined.
* DQ-W1 missing/blank/UNKNOWN city is warning-only.
* Quarantined records must never appear in Gold.
* OR and WA are allowed to contain zero rows.
* An entirely empty batch should fail or raise an alert rather than silently publish an empty Gold dataset.
* A CA row count of zero should fail or raise an alert because CA is expected to have data.

### Versioning

The current Gold contract is **v1**.

Breaking schema changes will be released under a new version such as:

```text
gold/university_chapters/v2/
```

The existing v1 contract will not be changed in place for breaking changes.

### Classification

The source data is public.

No PII is expected in this data product.

## Idempotency

Gold and Quarantine outputs are overwritten for each local pipeline execution.

This prevents repeated local executions from creating duplicate or unusable Gold output.

In a production implementation, an explicit partition or MERGE strategy could be used.

## Project Structure

```text
data-engineer-dev-assignment/
|
├── bronze/
│   └── university_chapters_raw.json
|
├── silver/
│   └── university_chapters/
│       └── part-00000.json
|
├── gold/
│   └── university_chapters/
│       └── part-00000.json
|
├── quarantine/
│   └── university_chapters/
│       └── part-00000.json
|
├── fixtures/
│   └── test_university_chapters.json
|
├── src/
│   ├── ingest.py
│   └── silver_transform.py
|
├── tests/
│   └── test_transform.py
|
├── requirements.txt
├── .gitignore
└── README.md
```

## How to Run

### 1. Create and activate the virtual environment

Windows CMD:

```cmd
.venv\Scripts\activate.bat
```

### 2. Install dependencies

```cmd
pip install -r requirements.txt
```

### 3. Run automated tests

```cmd
pytest -v
```

The tests use fixture data and validate the transformation and data-quality behaviour without depending on the live API.

### 4. Run ingestion

```cmd
python src\ingest.py
```

### 5. Run Silver → Gold transformation

```cmd
python src\silver_transform.py
```

The pipeline creates:

```text
silver/university_chapters/
gold/university_chapters/
quarantine/university_chapters/
```

## Testing

Fixture-driven tests cover:

* required transformation behaviour
* CA/OR/WA scope
* coordinate validation
* DQ-Q1 quarantine behaviour
* DQ-W1 warning behaviour
* quarantined records excluded from Gold
* warned records retained in Gold

The current test suite passes successfully.

## Technology

* Python
* PySpark
* pytest
* REST API
* JSON
* Local filesystem using an Azure/ADLS-style medallion layout

The Spark transformation layer is intentionally kept compatible with local execution for this assessment.

## Production Considerations

For a production Azure implementation, I would add:

* Azure Data Lake Storage Gen2
* Microsoft Fabric or Azure Databricks
* scheduled orchestration
* monitoring and alerting
* Azure DevOps CI/CD
* data catalog and lineage
* stronger schema enforcement
* partitioning by ingestion date
* production MERGE/idempotency strategy
* centralized logging
* retry and API failure handling

These items are intentionally outside the scope of this take-home assignment.

## Trade-offs

The assignment is implemented locally to keep the solution reproducible without requiring cloud credentials or infrastructure.

PySpark is used for the Silver transformation so the transformation logic can be moved to Databricks, Fabric Spark, or Synapse Spark with minimal conceptual change.

The local folder structure follows an ADLS-style Bronze/Silver/Gold/Quarantine layout.

No secrets or credentials are required because the source API is public.
