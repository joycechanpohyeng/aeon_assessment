# SpaceX Data Pipeline Assessment

*   **Participant:** Joyce Chan
*   **Position:** Data Engineer
*   **Status:** Ingestion Layer Complete | Transformation Layer Designed | Infrastructure Handoff Pending

---

## Project Overview
This repository contains a ELT data pipeline utilizing **Apache Airflow, dbt, Docker, and DuckDB** to process multi-feed data streams from the SpaceX API ([API Specification Reference](https://pipeworx.io/blog/spacex-api-revived/)). 


---

## Pre-requisite
| Ubuntu: https://ubuntu.com/tutorials/install-ubuntu-desktop 
| Docker desktop: https://docs.docker.com/desktop/setup/install/windows-install/


## Repository Layout & Folder Structure
The source files are organized into the following local project paths:

| Component Description | Local Project File Path |
| :--- | :--- |
| **Airflow Orchestration DAG** | `..\aeon\airflow_project\dags\spacex_dag.py` |
| **API Profiles & Variables Config** | `..\aeon\airflow_project\config\spacex_profile.yml` |
| **Bronze Layer Data Model Schema** | `..\aeon\airflow_project\dbt\models\spacex\bronze\_stg_spacex__sources.yml` |
| **Silver Layer Transformations** | `..\aeon\airflow_project\dbt\models\spacex\silver\` |

---

## Environment Initialization & Setup Log

### Step 1: Download Deployment Manifests
-	Open Ubuntu to install apache airflow composer
```bash
curl -LfO 'https://airflow.apache.org/docs/apache-airflow/3.3.0/docker-compose.yaml
```

### Step 2: Enable WSL Integration
Ensure **Docker Desktop → Settings → Resources → WSL Integration** is enabled for your active Ubuntu distribution.

### Step 3: Security & Environment Controls (`.env`)
A secured configurations file was declared at the root location to isolate instance parameters:
```env
AIRFLOW_UID=50000
_AIRFLOW_WWW_USER_USERNAME=admin
_AIRFLOW_WWW_USER_PASSWORD=admin
FERNET_KEY=<generated_fernet_key>
```
*The encryption token was generated securely inside PowerShell via:*
```powershell
python -c "from cryptography.fernet import Fernet; print(Fernet.generate_key().decode())"
```

### Step 4: System Scaffolding & Shared Access Adjustments
```bash
docker compose up airflow-init
sudo apt install curl -y
mkdir -p ./dags ./logs ./plugins ./config
```

#### Volume Mount Optimization
To enable concurrent read/write processing capabilities across the separate Airflow Webserver, Scheduler, and Worker container boundaries, volume definitions were modified to append **`:z`** shared content access control flags:
```yaml
volumes:
  - ${AIRFLOW_PROJ_DIR:-.}/dags:/opt/airflow/dags:z
  - ${AIRFLOW_PROJ_DIR:-.}/logs:/opt/airflow/logs:z
  - ${AIRFLOW_PROJ_DIR:-.}/config:/opt/airflow/config:z
  - ${AIRFLOW_PROJ_DIR:-.}/plugins:/opt/airflow/plugins:z
```

### Step 5: System Launch & Airflow Variable Mapping
The orchestration stack was initialized using `docker compose up`. The following variables were registered within the Airflow management interface:

| Airflow Variable Key | Variable Value |
| :--- | :--- |
| `SOURCE_BUCKET` | `/opt/airflow/data/source_raw` |
| `ENVIRONMENT` | `DEV` |
| `CONFIG_PATH` | `/opt/airflow/config` |
| `docker_dbt_image` | `docker-desktop://dashboard/build/desktop-linux/desktop-linux/hzj3tiuqc3vc7jwcc7dpws2cv` |

---

## Pipeline Architecture Design Decisions

```text
[ SpaceX API ] 
      │ (Airflow Python Ingestion Loop via Requests)
      ▼
[ Bronze Layer (Raw Storage) ] 
      │ Target Schema: bronze_staging (read_json_auto Schema-on-Read)
      ▼
[ Silver Layer (Cleaned Entities) ] 
        Incremental Overwrite (Delete-and-Insert by intakeDate)
```

### 1. High-Performance Schema-on-Read (DuckDB)
DuckDB's engine is mounted downstream to parse unstructured JSON data pools instantly using `read_json_auto(format='unstructured')`. (previously, I'm using snowflake and storage integration to load file(s3) into snowflake external stage location)


### 2. Date Partitioning & Idempotency
`intakeDate` string matching Airflow's execution date (`kwargs["ds"]`) is passed from the orchestrator into the dbt compilation scope. 

Downstream Silver staging models are built using an `incremental` materialization with an `insert+delete` strategy. This sets up a **Delete-and-Insert** data structure: if an execution block for a specific date is re-triggered, previous historical entries for that partition are automically dropped before the fresh records are merged, preventing duplicate values.