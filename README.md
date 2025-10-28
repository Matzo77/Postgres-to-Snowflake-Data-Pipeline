# Data Pipeline Project — Postgres → Snowflake → dbt → Metabase

An end-to-end **cloud-based data pipeline** built using **AWS Lambda**, **Snowflake**, **dbt**, **Airbyte**, and **Metabase**.  
This project demonstrates how to design and automate a modern **ELT (Extract → Load → Transform)** pipeline that integrates data ingestion, transformation, and visualization in the cloud.

---

##  Overview

This project showcases a **serverless and automated data workflow** that:
1. **Ingests** data from **PostgreSQL** to **Snowflake** using **AWS Lambda** and **Airbyte**.
2. **Automates** ingestion with **Amazon EventBridge** for daily scheduling.
3. **Transforms** data models using **dbt** for modular and version-controlled transformations.
4. **Visualizes** the final analytical layer using **Metabase** connected to Snowflake.

---

## Architecture

```text
         ┌──────────────┐               ┌────────────────────┐
         │  PostgreSQL  │               │  Amazon S3 Bucket  │
         └──────┬───────┘               └─────────┬──────────┘
                │                                 │
        (Extract via Airbyte)      (Extract via Lambda + Eventbrdige)
                │                                 │
                ▼                                 │
         ┌──────────────┐                         │
         │   Snowflake  │  ◀---------------------│
         │Staging Layer │
         └──────┬───────┘
                │
       (Transform with dbt )
                │
                ▼
        ┌──────────────┐
        │   Snowflake  │
        │   Analytics  │
        │     Layer    │
        └──────┬───────┘
               │
        (Query via Metabase)
               │
               ▼
        ┌──────────────┐
        │  Dashboard   │
        │  & Insights  │
        └──────────────┘
```


## Tech Stack

| Component                      | Purpose                                                  |
| ------------------------------ | -------------------------------------------------------- |
| **AWS Lambda**                 | Serverless ingestion function                            |
| **AWS EventBridge**            | Scheduled trigger for ingestion                          |
| **Snowflake**                  | Cloud data warehouse                                     |
| **Airbyte**                    | ELT data integration between Postgres and Snowflake      |
| **dbt(Data Build Tool)**       | Transformation and modeling layer                        |
| **Metabase**                   | Data visualization and dashboards                        |
| **PostgreSQL**                 | Source database                                          |
| **Python**                     | Lambda runtime for connecting and uploading to Snowflake |


## Project Structure
```text
.
├── README.md
├── lambda_function
│   ├── config.toml
│   ├── lambda_function.py
│   └── snowflake_lambda_layer.zip
├── postgres.txt
└── wcd_capstone_dbt
    ├── README.md
    ├── analyses
    ├── dbt_project.yml
    ├── macros
    │   ├── custom_schema.sql
    │   └── get_columns_without_prefix.sql
    ├── models
    │   ├── intermediate
    │   ├── marts
    │   └── staging
    ├── package-lock.yml
    ├── packages.yml
    ├── seeds
    ├── snapshots
    │   └── stg_customer_snapshot.sql
    └── tests
```
---

## Data Flow

| Step | Task                    | Tool                 | Output                                |
| ---- | ----------------------- | -------------------- | ------------------------------------- |
| 1    | Extract source tables   | Airbyte + Postgres   | Ingest to Snowflake RAW schema        |
| 2    | Extract inventory table | Lambda + Eventbridge | Ingest to Snowflake RAW schema        |
| 3    | Data transformation     | Snowflake + dbt      | Load into Snowflake Analytics schema  |
| 4    | Visualize results       | Metabase             | Interactive dashboards                |

---

## Running the Project

### Prerequisites
- AWS account with permissions to create Lambda and EventBridge resources
- Snowflake account with a database, schema, and user credentials
- Python 3.13+
- Airbyte container (either locally or on an EC2 instance)
- Metabase container
- Docker (for Airbyte and Metabase)

### 1. Configure AWS Lambda
1. Create a new Lambda function with Python 3.13 runtime.
2. Attach the policy AmazonS3FullAccess (or least-privilege equivalent).
3. Upload the Lambda Layer, which includes:
```bash
snowflake-connector-python
toml
```
4. Update configuration:
- Edit `config.toml` with your Snowflake account details.
- Set your Snowflake password as an **environment variable** in the Lambda function.
5. Create an **EventBridge rule** to trigger Lambda daily (e.g., `2 AM EST`).

### 2. Connect Postgres → Snowflake via Airbyte
1. Install and run Airbyte
2. In the Airbyte UI:
- Add Postgres as a source. (using the postgres connection infromation, see postgres.txt)
- Add Snowflake as a destination.
- Configure sync frequency

### 3. Set Up dbt for Transformations
1. Clone the repository:
```bash
git clone https://github.com/Matzo77/Postgres-to-Snowflake-Data-Pipeline.git
cd Postgres-to-Snowflake-Data-Pipeline
```
2. Install dbt and Snowflake adapter:
```bash
pip install dbt-core
pip install dbt-snowflake
```
3. Configure Snowflake connection profile in `profiles.yml` file.
4. Run the following command to initiate dbt transfomration:
```bash
dbt build
```

### 3. Build Visualizations in Metabase
1. Run Metabase container, example below:
```bash
docker run -d -p 3000:3000 metabase/metabase
```
2. Access Metabase at http://localhost:3000
3. Connect to your Snowflake database.
4. Build dashboards and charts using your dbt-transformed tables.

## References
- [AWS Lambda Documentation](https://docs.aws.amazon.com/lambda/)
- [Snowflake Documentation](https://docs.snowflake.com/en/)
- [dbt Documentation](https://docs.getdbt.com/docs/introduction)
- [Airbyte Documentation](https://docs.airbyte.com/)
- [Metabase Documentation](https://www.metabase.com/docs/latest/)

