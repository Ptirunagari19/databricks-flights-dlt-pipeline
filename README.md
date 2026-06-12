# ✈️ Databricks Flights Data Pipeline — End-to-End Project

A production-style, end-to-end data engineering pipeline built on **Databricks Delta Live Tables (DLT)**, implementing the **Medallion Architecture** (Bronze → Silver → Gold) with **real-time streaming**, **CDC (Change Data Capture)**, **data quality enforcement**, and **DBT transformations**.

---

## 📌 Project Overview

This project simulates a real-world airline data platform that ingests raw flight operations data, cleans and transforms it through structured layers, and serves it to analysts and BI tools via a unified Gold layer.

**Datasets processed:**
- ✈️ Flights
- 🪑 Bookings
- 👤 Passengers
- 🏢 Airports

---

## 🏗️ Architecture

```
┌─────────────────────────────────────────────────────────────┐
│                     BRONZE LAYER                            │
│         Raw Delta files landing in Databricks Volumes       │
│   bookings/ │ flights/ │ customers/ │ airports/             │
└──────────────────────┬──────────────────────────────────────┘
                       │  Spark Structured Streaming
                       ▼
┌─────────────────────────────────────────────────────────────┐
│                     SILVER LAYER (DLT)                      │
│                                                             │
│  silver_bookings  ──── 3-step chain (stage → trans → silver)│
│  silver_flights   ──── CDC flow (SCD Type 1)                │
│  silver_passengers──── CDC flow (SCD Type 1)                │
│  silver_airports  ──── CDC flow (SCD Type 1)                │
└──────────────────────┬──────────────────────────────────────┘
                       │  Joins across all 4 silver tables
                       ▼
┌─────────────────────────────────────────────────────────────┐
│                      GOLD LAYER (DBT)                       │
│                                                             │
│  silver_business  ──── Wide analytical table (all joined)   │
│  my_first_dbt_model ── Total booking amount by country      │
└─────────────────────────────────────────────────────────────┘
```

---

## 🛠️ Tech Stack

| Tool | Purpose |
|---|---|
| **Databricks** | Cloud data platform |
| **Delta Live Tables (DLT)** | Declarative pipeline orchestration |
| **Apache Spark / PySpark** | Distributed data processing |
| **Spark Structured Streaming** | Real-time streaming ingestion |
| **Autoloader** | Incremental file ingestion from Volumes |
| **Delta Lake** | ACID-compliant storage format |
| **DBT (Data Build Tool)** | Gold layer SQL transformations |
| **Python** | Pipeline logic |

---

## 📂 Repository Structure

```
databricks-flights-dlt-pipeline/
│
├── README.md
│
├── pipeline/
│   └── DltPipeline.py                  # Full DLT pipeline (Bronze → Silver → Business)
│
├── dbt/
│   └── models/
│       └── example/
│           └── my_first_dbt_model.sql  # Gold layer: total booking amount by country
│   └── dbt_project.yml
│   └── schema.yml
│
├── notebooks/
│   └── Databrick_End_to_End_Flights_Project.dbc   # Databricks notebook export
│
└── screenshots/
    └── pipeline_run.png                # Successful DLT pipeline DAG
```

---

## ⚙️ Pipeline Details

### 🥉 Bronze Layer
Raw airline data lands in **Databricks Volumes** as Delta files via Autoloader. No transformations — data is preserved exactly as received.

```
/Volumes/workspace/bronze/bronzevolume/bookings/data/
/Volumes/workspace/bronze/bronzevolume/flights/data/
/Volumes/workspace/bronze/bronzevolume/customers/data/
/Volumes/workspace/bronze/bronzevolume/airports/data/
```

---

### 🥈 Silver Layer

#### Bookings — 3-Step Chain
```
stage_bookings → trans_bookings → silver_bookings
```
- **stage_bookings**: Reads raw stream, drops `_rescued_data`
- **trans_bookings**: Casts `amount` to Double, adds `modifiedDate`, parses `bookingdate`
- **silver_bookings**: Applies data quality rules — drops records where `booking_id` or `passenger_id` is NULL

```python
rules = {
    "rule1": "booking_id IS NOT NULL",
    "rule2": "passenger_id IS NOT NULL"
}
@dlt.expect_all_or_drop(rules)
```

#### Flights, Passengers, Airports — CDC Flows
Each uses `dlt.create_auto_cdc_flow()` (SCD Type 1):
- Automatically **upserts** records based on primary key
- Handles inserts and updates without duplicates
- `modifiedDate` used as the sequence column to order changes

---

### 🥇 Business / Gold Layer

#### `silver_business` (DLT)
Joins all 4 silver tables into one wide analytical table:
```python
bookings
  .join(flights, "flight_id", "left")
  .join(passengers, "passenger_id", "left")
  .join(airports, "airport_id", "left")
```

#### `my_first_dbt_model` (DBT)
Reads from `workspace.gold` and aggregates total booking amount by country:
```sql
SELECT country, SUM(amount) AS total_amount
FROM workspace.gold.FactBookings F
LEFT JOIN workspace.gold.DimAirports D
  ON F.DimAirportsKey = D.DimAirportsKey
GROUP BY country
```

---

## ✅ Pipeline Run Results

| Table | Records | Status |
|---|---|---|
| stage_bookings | 1,300 | ✅ Completed |
| trans_bookings | 1,300 | ✅ Completed |
| silver_bookings | 1,300 | ✅ Completed (2 expectations passed) |
| silver_flights | 110 upserted | ✅ Completed |
| silver_passengers | 225 upserted | ✅ Completed |
| silver_airports | 55 upserted | ✅ Completed |
| silver_business | 1,300 | ✅ Completed |

---

## 🚀 How to Run

### Prerequisites
- Databricks workspace (Free Edition or above)
- Unity Catalog enabled
- DBT Cloud account connected to Databricks

### Steps
1. Import `Databrick_End_to_End_Flights_Project.dbc` into your Databricks workspace
2. Upload raw CSV/JSON source files to the Bronze Volume paths
3. Create a new **DLT Pipeline** in Databricks → attach `DltPipeline.py`
4. Set target schema to your Silver catalog
5. Click **Run pipeline**
6. For DBT: open DBT Cloud → run `dbt build`

---

## 💡 Key Concepts Demonstrated

- **Medallion Architecture** — structured Bronze → Silver → Gold data flow
- **Delta Live Tables** — declarative, auto-managed pipeline with lineage tracking
- **Spark Structured Streaming** — continuous real-time data processing
- **CDC with SCD Type 1** — automatic upserts without manual MERGE statements
- **Data Quality Enforcement** — row-level expectations with drop-on-fail logic
- **DBT on Databricks** — SQL-based Gold layer transformation with version control

---

## 👩‍💻 Author

**Pranusha Tirunagari**  
Data Engineer | Azure | Databricks | PySpark | Power BI  
📍 [LinkedIn](https://www.linkedin.com/in/pranusha-tirunagari-a583a63a8/) | [GitHub](https://github.com/Ptirunagari19)
