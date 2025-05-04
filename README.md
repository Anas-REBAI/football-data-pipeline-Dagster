# 🧠 Dagster StatsBomb Data Pipeline

> End-to-end data pipeline using open-source stack: **Dagster**, **MinIO**, **ClickHouse**, **DBT** & **Docker Compose** to ingest, transform and analyze StatsBomb football data.

---

## 🚀 Project Overview

This project builds a modular, containerized data pipeline for football data analytics using StatsBomb's open dataset. It covers:

* ✅ **Data Extraction** from GitHub
* ✅ **Object Storage** in MinIO (like AWS S3)
* ✅ **Ingestion into ClickHouse** (analytical DB)
* 🛠️ **Transformation via DBT** (next step)
* 📊 **Dashboarding with Metabase/Superset** (next step)

---

## 🧱 Stack

| Layer          | Tool                             |
| -------------- | -------------------------------- |
| Orchestration  | Dagster 🌀                       |
| Object Storage | MinIO 📦                         |
| Data Warehouse | ClickHouse ⚡                    |
| Transformation | DBT 🧮                           |
| Visualization  | Metabase / Superset 📊           |
| Infrastructure | Docker Compose 🐳                |

---

## 📁 Project Structure

```bash
DATA_PIPELINE/
├── etl_pipeline/              # Dagster project with assets, configs, resources
│   ├── assets/                # Asset definitions (Git clone, MinIO upload, ClickHouse load)
│   ├── config/                # MinIO & ClickHouse credentials loader
│   ├── resources/             # Dagster resource definitions
│   ├── definitions.py         # Dagster Definitions()
│   ├── requirements.txt       # Python dependencies
│   └── Dockerfile             # Dagster image builder
│
├── docker-compose.yml         # Services
├── .env                       # Secrets and credentials
├── .gitignore                 # Clean git repo
└── README.md                  # Project overview (this file)
```

---

## ⚙️ How to Run

### 1. ✅ Build and Start Services

```bash
docker-compose build
docker-compose up -d
```

### 2. 📂 Access Dagster UI

Open [http://localhost:3000](http://localhost:3000)

### 3. ⚙️ Trigger Assets

Run assets in Dagit:

* `transfer_statsbomb_to_minio` → GitHub → MinIO
* `load_minio_to_clickhouse_split` → MinIO → ClickHouse

## 📦 Data Source: StatsBomb Open Data

GitHub: [https://github.com/statsbomb/open-data](https://github.com/statsbomb/open-data)

## 👨‍💻 Author

Built by Anas — as a training project in data engineering & orchestration. Feel free to fork or contribute!


