# Real-Time Fraud Detection Lakehouse

Production-grade, end-to-end data engineering platform that detects fraudulent financial transactions in real time. Built on Azure, Apache Spark Structured Streaming, Delta Lake (Medallion architecture), and Kafka — with automated data quality validation and schema drift detection baked into every layer.

## Architecture

```
Transaction Sources (Kafka/Event Hubs)
        |
        v
  Bronze Layer (raw ingestion, Delta Lake)
        |
        v
  Silver Layer (cleansed, deduplicated, enriched)
        |
        v
  Gold Layer (aggregated fraud-risk features)
        |
        v
  Real-Time Scoring Service --> Alerting (sub-minute latency)
```

## Key Features

- **Streaming ingestion** from Kafka/Event Hubs using Spark Structured Streaming with exactly-once semantics
- **Medallion architecture** (Bronze/Silver/Gold) implemented on Delta Lake for ACID-compliant, versioned data
- **Automated data quality framework** — schema drift detection, null-rate thresholds, referential integrity checks, and quarantine routing for bad records
- **Feature engineering pipeline** for fraud-risk scoring (velocity checks, geo-anomaly detection, transaction-amount z-scores)
- **Sub-minute fraud alerting** via streaming aggregations and watermarking
- **Infrastructure as Code** for Azure resource provisioning (Data Lake Gen2, Databricks, Event Hubs)
- **CI/CD ready** with automated testing (pytest) for transformation logic

## Tech Stack

| Layer | Technology |
|---|---|
| Ingestion | Kafka / Azure Event Hubs |
| Processing | PySpark, Spark Structured Streaming |
| Storage | Delta Lake, Azure Data Lake Gen2 |
| Orchestration | Databricks Workflows / Airflow |
| Data Quality | Custom PySpark validation framework |
| Testing | Pytest |
| Infra | Azure CLI / Terraform |

## Project Structure

```
.
├── src/
│   ├── ingestion/
│   │   └── kafka_stream_reader.py
│   ├── transformations/
│   │   ├── bronze_to_silver.py
│   │   └── silver_to_gold.py
│   ├── quality/
│   │   └── data_quality_checks.py
│   ├── scoring/
│   │   └── fraud_risk_scorer.py
│   └── utils/
│       └── spark_session.py
├── tests/
│   └── test_data_quality_checks.py
├── config/
│   └── pipeline_config.yaml
├── requirements.txt
└── README.md
```

## Getting Started

```bash
git clone https://github.com/Ashok98765vvs/realtime-fraud-detection-lakehouse.git
cd realtime-fraud-detection-lakehouse
pip install -r requirements.txt
```

Configure your Azure Event Hubs / Kafka connection strings in `config/pipeline_config.yaml`, then run:

```bash
spark-submit src/ingestion/kafka_stream_reader.py
```

## Data Quality Framework

Every batch written to the Silver layer passes through automated checks:

- Null-rate threshold enforcement per column
- Schema drift detection against the registered Delta schema
- Duplicate-key detection with configurable dedup strategy
- Quarantine table routing for records failing validation

## Author

**Ashok Shankarappa** — Data Engineer specializing in real-time streaming pipelines, Azure-based Medallion Lakehouse architecture, and production-grade data quality systems.

- LinkedIn: [ashok-s1](https://linkedin.com/in/ashok-s1)
- GitHub: [Ashok98765vvs](https://github.com/Ashok98765vvs)
