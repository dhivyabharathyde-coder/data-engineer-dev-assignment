# Data Product Contract — University Chapters Gold

## 1. Product Overview

**Product Name:** University Chapters Gold Data Product

**Owner:** Data Engineering Team

**Purpose:**
Provide a clean and consumer-ready dataset of university chapters for California, Oregon, and Washington for analytics and reporting use cases.

**Source:** Public ArcGIS University Chapters FeatureServer API.

---

## 2. Architecture

The pipeline follows a Medallion architecture:

```text
Public ArcGIS FeatureServer
            |
            v
      Python Ingestion
            |
            v
         BRONZE
   Raw API response
            |
            v
         SILVER
 Flattened + typed + scoped
            |
            v
      Data Quality Rules
         /          \
        v            v
   QUARANTINE       GOLD
 Invalid records   Valid records
```

### Bronze

Stores the raw API response and ingestion information.

### Silver

Contains flattened, typed, scoped and deduplicated chapter records.

### Gold

Contains the consumer-facing data product. Records that fail the hard coordinate rule are excluded from Gold.

### Quarantine

Contains records that fail the hard data-quality rule, together with the reason and raw payload for investigation.

---

## 3. Scope

The data product contains records for:

```text
CA - California
OR - Oregon
WA - Washington
```

Records outside these states are excluded from the data product.

OR and WA may legitimately contain zero records because the source API may return no records for those states.

---

## 4. Gold Grain

One Gold record represents one university chapter identified by:

```text
chapter_id
```

Duplicate records for the same `chapter_id` are removed during the Silver transformation.

---

## 5. Gold Schema

| Column          | Type   | Description                            |
| --------------- | ------ | -------------------------------------- |
| `chapter_id`    | string | University chapter business identifier |
| `chapter_name`  | string | University chapter name                |
| `city`          | string | City associated with the chapter       |
| `state`         | string | Two-letter state code                  |
| `longitude`     | double | Geographic longitude                   |
| `latitude`      | double | Geographic latitude                    |
| `dq_status`     | string | `OK` or `WARNING`                      |
| `dq_warnings`   | string | Data-quality warning reason            |
| `ingest_run_id` | string | Identifier for the pipeline execution  |

---

## 6. Data Quality Rules

### DQ-Q1 — Invalid Coordinates

A record is quarantined when:

* longitude is null
* latitude is null
* longitude is outside `-180` to `180`
* latitude is outside `-90` to `90`

The quarantine reason is:

```text
INVALID_COORDINATES
```

The record is excluded from Gold.

The quarantine output retains the relevant record information and raw payload for debugging.

### DQ-W1 — Missing or Unknown City

A record receives a warning when:

* city is null
* city is blank
* city is `UNKNOWN` (case-insensitive)

The record is **not quarantined** and can continue to Gold.

Gold contains:

```text
dq_status = WARNING
dq_warnings = MISSING_OR_UNKNOWN_CITY
```

Records without a city warning contain:

```text
dq_status = OK
dq_warnings = empty
```

---

## 7. Data Quality Metrics

Each pipeline execution reports:

* `rows_in`
* `silver_rows`
* `rows_quarantined`
* `rows_warned`
* `rows_ok`

Synthetic fixture data is used to demonstrate both required data-quality paths because the live API may contain only clean records.

---

## 8. Empty Batch and API Failure Policy

An API failure should not be treated as a successful empty ingestion.

An entirely empty source result should be treated as a failure condition rather than silently publishing an empty Gold data product.

For production implementation, the pipeline should generate an alert when the expected source data is unavailable or unexpectedly empty.

---

## 9. Freshness / SLA

The intended production freshness target is:

```text
Daily refresh by 06:00 UTC
```

The current take-home implementation is manually executable and does not include a production scheduler.

---

## 10. Idempotency

The local implementation uses deterministic output locations and overwrite publishing for generated outputs.

This allows repeated executions against the same input to replace the previous generated output rather than continuously appending duplicate files.

For a production implementation, an explicit partitioning or MERGE strategy could be used.

---

## 11. Versioning

The current Gold contract is:

```text
v1
```

Breaking schema changes should be released as a new contract version rather than modifying the existing contract in place.

For example:

```text
gold/university_chapters/v2/
```

---

## 12. Data Classification

The source data is publicly available.

No credentials, authentication tokens, or secrets are stored in the data product.

No PII is expected in this dataset based on the source and assignment scope.

---

## 13. Test Evidence

The repository contains fixture data covering:

1. Valid record
2. Invalid coordinate record
3. Missing city record
4. `UNKNOWN` city record

Expected behavior:

```text
Invalid coordinates
        |
        v
   Quarantine
```

and:

```text
Missing / UNKNOWN city
        |
        v
Gold with WARNING
```

The live API can contain only clean records, so fixture-based testing provides deterministic evidence for both data-quality paths.

---

## 14. Production Evolution

The current implementation is designed to run locally without cloud credentials.

For production Azure deployment, the same logical architecture can be implemented using:

* Azure Data Lake Storage Gen2 for Bronze, Silver and Gold storage
* Azure Databricks or Microsoft Fabric for Spark processing
* Azure Data Factory or another orchestrator for scheduling
* Azure Key Vault for secrets where required
* Azure DevOps or GitHub for CI/CD
* Monitoring and alerting
* Data catalog and lineage
* Schema enforcement and evolution controls
* Production-grade partitioning and MERGE strategies

The business rules and Gold Data Product Contract would remain consistent while the execution and storage infrastructure scale to production.
