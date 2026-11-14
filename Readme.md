# Airline Operations & Revenue Intelligence Platform

## Overview

An end-to-end Data Engineering portfolio project designed to simulate airline operations, process flight and booking data, and build analytical datasets for operational and revenue insights.

The platform will use Python, Apache Spark, Google Cloud Platform (GCP), BigQuery, and Apache Airflow to demonstrate scalable batch data processing, orchestration, data quality, and infrastructure automation.

**Project Status:** Under development

## Objectives

- Generate realistic synthetic airline operations and booking datasets.
- Ingest source data into a raw storage layer.
- Clean, validate, and transform data using PySpark.
- Build analytical datasets in BigQuery for flight operations and revenue reporting.
- Orchestrate batch pipelines using Apache Airflow / Cloud Composer.
- Implement automated testing, data quality checks, and infrastructure as code.

## Planned Architecture

```text
Synthetic Data Generators (Python)
             |
             v
      Raw Data Storage
        (Local / GCS)
             |
             v
    Data Processing (PySpark)
             |
             v
     Curated Data Storage
             |
             v
       BigQuery Warehouse
             |
             v
       Analytics & Reports
```

Apache Airflow / Cloud Composer will orchestrate the pipeline, while Terraform will manage GCP infrastructure.

## Technology Stack

| Category | Technology |
|---|---|
| Programming | Python, SQL |
| Data Processing | Apache Spark, PySpark |
| Cloud Platform | Google Cloud Platform |
| Storage | Google Cloud Storage |
| Data Warehouse | BigQuery |
| Orchestration | Apache Airflow, Cloud Composer |
| Spark Infrastructure | Dataproc |
| Infrastructure as Code | Terraform |
| Testing | pytest |
| Version Control | Git, GitHub |

## Repository Structure

```text
airline-operations-revenue-platform/
├── config/                 # Application configuration
├── generators/             # Synthetic data generation
├── ingestion/              # Source extraction and ingestion
├── spark/
│   ├── jobs/               # Executable Spark jobs
│   ├── transformations/    # Reusable transformations
│   └── utils/              # Shared Spark utilities
├── dags/                   # Airflow DAGs
├── data/
│   ├── raw/                # Local raw datasets
│   ├── processed/          # Local processed datasets
│   └── sample/             # Sample datasets
├── data_quality/           # Data quality checks
├── warehouse/              # BigQuery SQL and data models
├── infrastructure/
│   └── terraform/          # Infrastructure as code
├── tests/                  # Unit and integration tests
├── scripts/                # Development utilities
├── docs/                   # Architecture and data contracts
└── README.md
```

## Local Development

### Prerequisites

- Python 3.11
- Java 17
- Git
- A compatible local Apache Spark / PySpark environment

### Setup

Clone the repository and navigate to the project directory.

Create and activate a virtual environment on Windows PowerShell:

```powershell
python -m venv .venv
.\.venv\Scripts\Activate.ps1
```

Install the project's Python dependencies using its dependency configuration.

### Spark Smoke Test

Run the existing smoke test from the project root:

```powershell
python spark/jobs/spark_smoke_test.py
```

The test verifies that the local Spark environment can start and execute a basic Spark workload.

## Development Roadmap

1. **Project Foundation:** Repository structure, configuration, and data contracts.
2. **Data Generation:** Synthetic flight, booking, and ticket datasets.
3. **Data Processing:** PySpark cleaning, validation, and transformations.
4. **Data Warehouse:** BigQuery tables and analytical models.
5. **Orchestration:** Airflow DAGs and Cloud Composer integration.
6. **Production Engineering:** Terraform, automated testing, monitoring, and CI/CD.
7. **Analytics:** Operational and revenue reporting.

## Engineering Principles

- Separation of concerns between generation, ingestion, processing, and orchestration.
- Configuration-driven execution without hardcoded environment-specific paths.
- Modular, reusable, and testable Python code.
- Clear data contracts and validation rules.
- Reproducible development and deployment workflows.

## Current Progress

- [x] Git repository initialized
- [x] Local PySpark smoke test executed successfully
- [ ] Repository foundation finalized
- [ ] Configuration management implemented
- [ ] Data contracts defined
- [ ] Synthetic data generators implemented
- [ ] End-to-end pipeline implemented

## Getting Started

For local environment setup, data generation, and test
execution, see [Local Setup Guide](docs/local_setup.md).
```