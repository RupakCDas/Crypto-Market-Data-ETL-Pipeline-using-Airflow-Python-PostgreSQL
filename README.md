# Crypto Market Data ETL Pipeline using Airflow, Python-PostgreSQL.
## Data Architecture

<img width="1055" height="401" alt="arc" src="https://github.com/user-attachments/assets/038ce189-ea1e-4b29-9392-ca640ab2dab8" />



### Project Overview
Designed and implemented a automated ETL data pipeline that orchestrates the daily ingestion of cryptocurrency market intelligence using Apache Airflow. The pipeline extracts market data from the CoinGecko API, applies  data transformation steps, and loads the structured data into  PostgreSQL database. It provides analysis-ready data for popular digital assets like Bitcoin, Ethereum, and Solana.


### Tech Stack & Key Concepts
- Orchestration: Apache Airflow (DAGs, Operators, XComs)  
- Database: PostgreSQL 
- Languages & Libraries: SQL, Python, Requests, Logging  

### ETL Tasks

| Task        | What it does                                                         |
|-------------|----------------------------------------------------------------------|
| `extract`   | Calls `/coins/markets` on CoinGecko; pushes JSON to XCom            |
| `transform` | Flattens fields, renames columns, adds `fetched_at` timestamp        |
|`create table`| Create table in PostgreSQL database                                 |
| `load`      | Upserts rows into `crypto_market_data` via `ON CONFLICT DO UPDATE`   |


### System Architecture & DAG Workflow
The pipeline runs on an automated hourly scheduled, structured as a non-overlapping DAG with four sequential tasks: 

[ Extract ] ──> [ Transform ] ──> [ Create Table (DDL) ] ──> [ Load ]




