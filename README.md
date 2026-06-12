# Databricks Flights Data Pipeline

This is an end-to-end data engineering project I built using Databricks Lakeflow Declarative Pipelines. It processes real airline data — flights, bookings, passengers, and airports — through a structured pipeline that cleans, transforms, and joins everything together so it's ready for reporting.

---

## What this project does

Raw airline data comes in messy and unstructured. This pipeline takes that raw data, cleans it up in stages, handles any updates or changes automatically, and finally combines everything into one clean table that analysts can use for reporting.

The whole thing runs in real time using Spark Structured Streaming — meaning as new data arrives, the pipeline picks it up and processes it automatically.

---

## How the data flows

**Bronze** — Raw data lands here exactly as it comes in. No changes, no cleaning. Just a safe copy of everything.

**Silver** — This is where the real work happens. Each dataset goes through its own cleaning process:

- Bookings go through three steps — first the raw data is staged, then transformed (fixing data types, adding timestamps), then validated. Any booking missing a booking ID or passenger ID gets dropped automatically.
- Flights, passengers, and airports each go through a CDC (Change Data Capture) flow. This means if a record gets updated in the source, the pipeline automatically updates it here too — no duplicates, no manual merging needed.

**Gold** — All four cleaned tables get joined together into one wide table. On top of that, a DBT model aggregates total booking amounts by country for reporting.

---

## Tech used

- Databricks (Lakeflow Declarative Pipelines)
- PySpark and Spark Structured Streaming
- Delta Lake
- DBT (Data Build Tool)
- Python

---

## Pipeline results

When I ran the full pipeline, here's what came out:

- 1,300 bookings processed and validated
- 110 flight records upserted
- 225 passenger records upserted
- 55 airport records upserted
- Final business table: 1,300 records combining all four datasets

---

## How to run it yourself

You'll need a Databricks workspace with Unity Catalog enabled, and a DBT Cloud account connected to Databricks.

1. Import the `.dbc` notebook file into your Databricks workspace
2. Upload your source data files to the Bronze volume paths
3. Create a new Lakeflow Declarative Pipeline and attach `DltPipeline.py`
4. Run the pipeline
5. In DBT Cloud, run `dbt build` to generate the Gold layer model

---

## What I learned from this

Building this helped me get hands-on with real streaming pipelines rather than just batch jobs. The CDC flows were particularly interesting — instead of writing manual MERGE statements, Databricks handles the upsert logic automatically based on the primary key. The data quality expectations were also a clean way to enforce rules at the pipeline level rather than fixing bad data downstream.

---

## 👩‍💻 Author

**Pranusha Tirunagari**  
Data Engineer | Azure | Databricks | PySpark | Power BI  
📍 [LinkedIn](https://www.linkedin.com/in/pranusha-tirunagari-a583a63a8/) | [GitHub](https://github.com/Ptirunagari19)
