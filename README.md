# Real-Time Flight Monitoring System


## Overview

This project implements a real-time global flight monitoring system using data streamed from the OpenSky Network API. It processes, cleans, and enriches flight data using Apache Spark Structured Streaming and batch jobs, orchestrated using Apache Airflow. The system is designed to operate continuously and generate insights on global air traffic in near real-time.

---

## Data Flow

1. **Bronze Layer (Raw Ingestion)**:

   * Polls the OpenSky REST API in a continuous loop.
   * Ingests raw flight state data.
   * Writes as-is to Delta Lake format.

2. **Silver Layer (Stream Processing)**:

   * Spark Structured Streaming job continuously reads from Bronze.
   * Applies cleaning transformations:

     * Drops nulls in critical fields.
     * Filters out invalid coordinates and altitudes.
     * Removes entries with invalid or missing ICAO codes.
     * Deduplicates by aircraft ID and timestamp.
     * Adds ingestion timestamps.
   * Writes cleaned data to Silver Delta table.

3. **Gold Layer (Batch Enrichment and Aggregation)**:

   * Runs every 5 minutes as a batch Spark job.
   * Enriches flight data with derived columns:

     * Horizontal speed, UTC timestamps, date/hour fields.
     * Flags for flight phase, high altitude, and high speed.
   * Performs aggregations grouped by origin country, date, and hour.
   * Writes aggregated insights to Gold Delta table.

---

## Airflow Orchestration

<insert Airflow DAG diagram>

* **bronze\_ingestion\_dag**: Ensures the Bronze ingestion script is running continuously.
* **silver\_streaming\_dag**: Monitors and triggers the Silver stream processing.
* **gold\_aggregation\_dag**: Triggers every 5 minutes to run batch enrichment and aggregation.

Each DAG includes logic to check whether associated scripts are running and relaunches them if necessary.

---

## Folder Structure

```bash
├── README.md
├── config
│   └── config.yaml
├── datalake
│   ├── _checkpoints
│   │   └── silver
│   ├── bronze
│   │   └── opensky
│   ├── gold
│   │   └── opensky
│   └── silver
│       └── opensky
├── ingestion
│   └── spark_ingestion_job
│       └── bronze_ingestion.py
├── orchestration
│   ├── dags
│   │   ├── bronze_ingestion_dag.py
│   │   ├── gold_aggregation_dag.py
│   │   └── silver_streaming_dag.py
│   └── scripts
│       ├── bronze_process.py
│       └── silver_process.py
├── processing
│   ├── bronze_to_silver
│   │   └── clean_flights_stream.py
│   └── silver_to_gold
│       └── enrich_flights_batch.py
├── requirements.txt
├── schemas
│   ├── bronze.ddl
│   ├── gold.ddl
│   └── silver.ddl
├── utils
│   ├── config_loader.py
│   ├── delta_writer.py
│   ├── opensky_api_reader.py
│   ├── process_life_check.py
│   ├── schema_loader.py
│   ├── spark_session.py
│   └── streaming_query_listener.py
```

---

## Requirements

* Python 3.10+
* Apache Spark 3.x
* Apache Airflow 3.x
* Delta Lake
* OpenSky Network API Access

Install dependencies:

```bash
pip install -r requirements.txt
```

---

## Setup & Run

### 1. Initialize Airflow

```bash
export AIRFLOW_HOME=~/airflow
airflow db init
airflow users create \
  --username admin \
  --firstname FIRST \
  --lastname LAST \
  --role Admin \
  --email admin@example.com
```

### 2. Start Airflow Services

```bash
airflow scheduler &
airflow webserver &
```

### 3. Trigger DAGs from UI

Visit `http://localhost:8080` and trigger the DAGs:

* `bronze_ingestion_dag`
* `silver_streaming_dag`
* `gold_aggregation_dag`

---

## Sample Output

* Bronze Dataframe![image](https://github.com/user-attachments/assets/d7c87c3a-4195-4bbc-a1af-8fee9815a065)
* Silver Dataframe![image](https://github.com/user-attachments/assets/70dade75-fb10-41c6-b084-02a49e42b1f4)
* Gold Dataframe![image](https://github.com/user-attachments/assets/1415ea17-735c-4af2-9069-507ced8b0a4b)
* Airflow![image](https://github.com/user-attachments/assets/3521e603-df90-4f4d-bed4-aa47dd2020f5)![image](https://github.com/user-attachments/assets/523633ec-88ba-4454-812d-deeb4d6f0077)![image](https://github.com/user-attachments/assets/7b554b80-43b3-4f30-9cfd-20a7c925ecb0)![Screenshot 2025-05-18 174559](https://github.com/user-attachments/assets/9cc12e4f-22de-4acc-8152-f7159fa47c46)


---

## Acknowledgements

* OpenSky Network for live flight data.
* Apache Spark and Delta Lake community.
* Apache Airflow project maintainers.
