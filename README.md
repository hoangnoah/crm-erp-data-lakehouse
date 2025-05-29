# Data Lakehouse Project

This project implements a Data Lakehouse architecture using a combination of open-source technologies, orchestrated with Docker Compose. It provides a foundation for storing, processing, and analyzing large datasets with capabilities for both batch and analytical workloads.

## Technologies Used

- **Apache Spark:** Unified analytics engine for large-scale data processing.
- **Apache Iceberg:** An open table format for huge analytic datasets, providing features like schema evolution, time travel, and partition evolution. (Implied by Lakehouse concept and interaction with Spark/Hive)
- **Apache Hive Metastore:** A central repository for metadata about data stored in the data lake.
- **MinIO:** High-performance, S3 compatible object storage, used here as the storage layer for the data lake.
- **PostgreSQL:** Relational database used as the backend for the Hive Metastore and Apache Airflow.
- **Apache Airflow:** Platform to programmatically author, schedule, and monitor workflows (DAGs).
- **Dremio:** Open-source data lakehouse query engine.
- **Jupyter Notebook:** Interactive computing environment for data exploration and analysis, connected to Spark.

## Architecture Overview

The project sets up the following components:

- **MinIO** serves as the object storage layer.
- **Apache Iceberg** (managed via Spark/Hive Metastore) provides the table format on top of MinIO.
- **Hive Metastore** stores schema information for Iceberg tables, backed by a **PostgreSQL** database.
- **Apache Spark** connects to the Hive Metastore and MinIO to process data.
- **Dremio** connects to the Hive Metastore and MinIO, allowing SQL queries directly on the data lake.
- **Apache Airflow** is used for orchestrating data pipelines, loading data, running transformations, etc. Airflow uses its own **PostgreSQL** backend.
- **Jupyter Notebook** is provided for interactive data analysis using Spark.

All components are containerized and orchestrated using **Docker Compose**.

## Setup Guide

Follow these steps to get the project running on your local machine.

### Prerequisites

- [Docker](https://www.docker.com/get-started/)
- [Docker Compose](https://docs.docker.com/compose/install/)

### Steps

1.  **Clone the repository:**

    ```bash
    git clone [<repository_url>](https://github.com/hoangnoah/ute-big-data.git)
    cd <repository_folder>
    ```

2.  **Environment Variables:**
    The project uses a `.env` file for configuration (e.g., Spark access keys for MinIO). Create a `.env` file in the root directory. Refer to `docker-compose.yml` for expected variables (e.g., `MINIO_ACCESS_KEY`, `MINIO_SECRET_KEY`, `SPARK_EXTRA_CONF_minio_access_key`, `SPARK_EXTRA_CONF_minio_secret_key`).

3.  **Build and Run Docker Containers:**
    Build the necessary custom images and start all services defined in `docker-compose.yml`.

    ```bash
    docker compose up --build -d
    ```

    Use `-d` to run the containers in detached mode (in the background).

4.  **Initial Setup (MinIO Bucket):**
    Access the MinIO console to create the necessary buckets.

    - Go to: [http://localhost:9001/](http://localhost:9001/)
    - Login with default credentials:
      - User: `minioadmin`
      - Password: `minioadmin`
    - Create a bucket named `warehouse`.

5.  **Initialize Hive Metastore Schema:**
    The `hive-metastore` service in `docker-compose.yml` includes a command (`schematool -initSchema`) to initialize the schema in the PostgreSQL database on startup. Ensure this command runs successfully. You can check container logs:

    ```bash
    docker logs hive-metastore
    ```

## Accessing Services

Once the containers are running, you can access the UIs for various services:

- **MinIO Console:** [http://localhost:9001/](http://localhost:9001/) (Default user/pass: minioadmin/minioadmin)
- **Spark Master UI:** [http://localhost:8080/](http://localhost:8080/)
- **Airflow UI:** [http://localhost:8081/](http://localhost:8081/) (Default user/pass might need to be set up or found in Airflow docs/config)
- **Dremio UI:** [http://localhost:9047/](http://localhost:9047/) (Initial setup required on first access)
- **Jupyter Notebook:** [http://localhost:8888/](http://localhost:8888/) (No token configured by default in docker-compose)

## Project Structure

- `./docker/`: Contains Dockerfile(s) for custom images (e.g., Airflow).
- `./airflow/`: Airflow configuration and DAGs.
- `./spark/scripts/`: Spark scripts (like `test.py` mentioned in original README).
- `./sql/`: Potential directory for SQL scripts (for Dremio or other uses).
- `./data/`: Placeholder for sample data.
- `./notebooks/`: Jupyter notebooks for analysis.

## Usage Examples

- **Running a Spark Job:** Use `docker compose exec spark-master spark-submit ...` as shown in the original README or submit jobs to the Spark master.
- **Querying Data with Dremio:** Connect Dremio to the Hive Metastore and MinIO, then use the Dremio UI or clients to query your data lake.
- **Creating Airflow DAGs:** Place Python scripts defining workflows in `./airflow/dags/` to have Airflow pick them up.
- **Interactive Analysis:** Use the Jupyter notebook to connect to Spark and analyze data.

## Data

The project utilizes CRM and ERP related data stored in the data lake. This data is accessed by Spark and Dremio for processing and analysis.

The primary datasets include:

- `fact_sales`: Contains sales transaction details.
- `dim_customers`: Contains customer information (demographics, join date).
- `dim_products`: Contains product details.
- `dim_dates`: Contains date-related dimensions for temporal analysis.

These datasets are expected to be available in the MinIO `warehouse` bucket, likely structured using Apache Iceberg or a similar table format to leverage the Data Lakehouse capabilities. The `crm_erp_model_analysis.ipynb` notebook provides examples of how this data is used for customer analysis and modeling using Spark.
