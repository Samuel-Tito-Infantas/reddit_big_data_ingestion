# CDC Sandbox project

This project implements a robust ETL (Extract, Transform, Load) pipeline using **Apache Spark (AWS Glue)**, **PostgreSQL**, and **AWS S3** (simulated via **LocalStack**). It is designed to handle Big Data ingestion patterns, specifically focusing on Change Data Capture (CDC) strategies for financial data (Customers, Accounts, Transactions).

## Overview

The pipeline ingests data from a relational database (PostgreSQL), processes it using PySpark, and stores it in a Data Lake (S3) in Parquet format. It supports various ingestion strategies including full loads, incremental inserts, and updates using partition overwrites.

**Key Features:**

* **Infrastructure as Code (IaC):** Terraform is used to provision S3 buckets.
* **Local AWS Simulation:** LocalStack simulates AWS S3 and Glue environments for cost-free local development.
* **CDC Support:** Handles `INSERT`, `UPDATE`, and `REFRESH` operations efficiently using partition pruning and `replaceWhere` logic.
* **Data Quality:** Includes unit tests for critical ETL functions.

## Tech Stack

* **Language:** Python 3.11+
* **Processing:** Apache Spark (PySpark), AWS Glue
* **Database:** PostgreSQL 13.4
* **Cloud (Simulated):** AWS S3 (LocalStack), AWS Glue (Docker)
* **Containerization:** Docker, Docker Compose
* **IaC:** Terraform
* **Testing:** Pytest

## Architecture

1. **Source:** PostgreSQL database running in a Docker container.
2. **Ingestion:** PySpark job (AWS Glue) reads data via JDBC.
3. **Transformation:** Data is partitioned by `year`, `month`, and `day`.
4. **Load:** Data is written to an S3 Bucket (LocalStack) in Parquet format.

## Project Structure

```text
reddit_big_data_ingestion-feature-setup/
├── docker-compose.yml          # Services: Postgres, LocalStack, Glue-Jupyter
├── etl_process/                # Core ETL logic
│   ├── auxiliar_functions/     # DB & Spark helper functions
│   ├── insert_step/            # Insert pipeline logic
│   ├── update_step/            # Update pipeline logic (CDC)
│   ├── refresh_step/           # Full refresh logic
│   └── parameters.py           # Configs (DB creds, S3 paths)
├── infra-iac/                  # Terraform for AWS S3 setup
├── utils/postgres_scripts/     # SQL scripts for DB initialization
├── test/                       # Unit tests
├── main.py                     # Entry point for the Glue Job
└── pyproject.toml              # Dependencies and Tool config

```

## Environment Setup

### Prerequisites

* [Docker](https://www.docker.com/) & Docker Compose
* [Terraform](https://www.terraform.io/) (Optional, for IaC execution)
* Python 3.11+ (for local testing)

### 1. Start Services

Launch the local infrastructure including PostgreSQL, LocalStack, and the Glue container.

```bash
docker-compose up -d

```

### 2. Configure Infrastructure (S3)

Initialize the S3 bucket using Terraform or manually via AWS CLI targeting LocalStack.

**Using Terraform:**

```bash
cd infra-iac
terraform init
terraform apply

```

**Or using AWS CLI (LocalStack):**

```bash
aws --endpoint-url=http://localhost:4566 s3 mb s3://brozen-data-lake-bucket

```

### 3. Initialize Database

Connect to the `postgre_localdb` container and run the initialization scripts located in `utils/postgres_scripts/`.

* **Credentials:** `myusers` / `passwords`
* **Database:** `postgres_db`

You must run the scripts in this order:

1. `01-create_data.sql` (Creates tables and triggers)
2. `02-insert-data.sql` (Populates initial data)

## Usage

The ETL job is triggered via `main.py`. It accepts arguments to control the execution mode.

### Execution Arguments

| Argument | Description | Options |
| --- | --- | --- |
| `ETL_ENVIRONMENT` | Environment flag | `local`, `aws` |
| `ACTION` | Type of ETL operation | `insert`, `update`, `refresh` |
| `TASK_MODE` | Scope of the task | `full`, `partial` |
| `TABLE_TARGET_NAME` | Specific table to process | e.g., `customers` (or `None` for all) |

### Running the Job (Local)

You can run the job inside the `glue-jupyter` container or locally if you have PySpark configured to point to LocalStack.

**Example Command:**

```bash
python main.py \
  --JOB_NAME "test_job" \
  --ETL_ENVIRONMENT "local" \
  --ACTION "insert" \
  --TASK_MODE "partial" \
  --TABLE_TARGET_NAME "customers"

```

### ETL Modes Explained

* **Insert:** Appends new records based on the `created_at` timestamp.
* **Update:** Identifies changed records using `last_updated_at`. It performs a "Copy-on-Write" operation using Spark's `replaceWhere` option to overwrite specific partitions in S3 without rewriting the whole table.
* **Refresh:** Completely overwrites the target table data in S3.

## Testing

Unit tests are located in `test/uni_test/`. To run them, ensure you have the dev dependencies installed.

```bash
pip install .   # Installs dependencies from pyproject.toml
pytest          # Runs the test suite

```

## License

This project is licensed under the Apache License 2.0. See the [LICENSE](https://www.apache.org/licenses/LICENSE-2.0) file for details.