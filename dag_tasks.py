"""
CoinGecko ETL Pipeline - Airflow DAG
Extracts crypto data from CoinGecko API and loads into PostgreSQL
"""
import requests
import logging
import pandas as pd
from bs4 import BeautifulSoup
import json
import ssl
import urllib.parse
import urllib.request
import certifi
from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.hooks.postgres_hook import PostgresHook
from airflow.providers.common.sql.operators.sql import SQLExecuteQueryOperator
from airflow.models import Variable

logger = logging.getLogger("airflow.task")


# ── Task 1: Extract ───────────────────────────────────────────────────────────
def extract(**context):
    ti = context["ti"]
    params = urllib.parse.urlencode(
        {
            "start": "1",
            "limit": "15",
            "convert": "USD",
        }
    )
    request = urllib.request.Request(
        f"https://pro-api.coinmarketcap.com/v1/cryptocurrency/listings/latest?{params}",
        headers={
            "Accept": "application/json",
            "X-CMC_PRO_API_KEY": "c1d79d5d81aa4890af1b30c61395ed51",
        },
    )
    
    ssl_context = ssl.create_default_context(cafile=certifi.where())
    
    with urllib.request.urlopen(request, context=ssl_context) as response:
        data = json.load(response)

    
    ti.xcom_push(key="raw_data", value=data)
    
    return data

# ── Task 2: Transform ─────────────────────────────────────────────────────────

def transform(**context):
    """Clean and reshape raw API data into DB-ready rows."""
    ti = context["ti"]
    raw_data = ti.xcom_pull(key="raw_data", task_ids="extract")
    
    if not raw_data:
        logger.error("No raw_data found in XCom from 'extract' task!")
        return 0

    transformed = []
    fetched_at = datetime.utcnow().isoformat()

    coin_list = raw_data.get("data", [])

    for coin in coin_list:
        row = {
            "coin_id": coin.get("id"),
            "symbol": coin.get("symbol", "").upper(),
            "name": coin.get("name"),
            "fetched_at": fetched_at,  # Recommended: track when you processed it
        }
        transformed.append(row)

    logger.info("Transformed %d records", len(transformed))

    ti.xcom_push(key="transformed_data", value=transformed)

    return len(transformed)

# ── Task 3: Load ──────────────────────────────────────────────────────────────
def load(**context):
    """Upsert transformed records into PostgreSQL."""
    ti = context["ti"]
    records = ti.xcom_pull(key="transformed_data", task_ids="transform")

    if not records:
        logger.warning("No records to load.")
        return 0

    hook = PostgresHook(postgres_conn_id="api_connection")

    upsert_sql = """
        INSERT INTO coins (
            coin_id, symbol, name
        ) VALUES (
            %(coin_id)s, %(symbol)s, %(name)s
        )
        ON CONFLICT (coin_id) 
        DO UPDATE SET
            symbol = EXCLUDED.symbol,
            name = EXCLUDED.name;
    """

    conn = hook.get_conn()
    cursor = conn.cursor()
    inserted = 0

    try:
        for record in records:
            cursor.execute(upsert_sql, record)
            inserted += 1
        conn.commit()
        logger.info("Successfully loaded %d records", inserted)
    except Exception as e:
        conn.rollback()
        logger.error("Load failed: %s", str(e))
        raise
    finally:
        cursor.close()
        conn.close()

    return inserted

# ── Default DAG args ──────────────────────────────────────────────────────────
default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 3,
    "retry_delay": timedelta(minutes=5),
}

# ── DAG Definition ────────────────────────────────────────────────────────────
with DAG(
    dag_id="coingecko_etl",
    default_args=default_args,
    description="Extract crypto market data from CoinGecko and load into PostgreSQL",
    schedule_interval="@hourly",       # change to e.g. "*/15 * * * *" for 15-min
    start_date=datetime(2024, 1, 1),
    catchup=False,
    tags=["etl", "crypto", "coingecko"],
) as dag:

    t_extract = PythonOperator(
        task_id="extract",
        python_callable=extract,
        provide_context=True,
    )

    t_transform = PythonOperator(
        task_id="transform",
        python_callable=transform,
        provide_context=True,
    )
    
    create_table_task = PostgresOperator(
        task_id='create_table',
        postgres_conn_id='api_connection',
        sql="""
        CREATE TABLE IF NOT EXISTS coins (
            coin_id INT PRIMARY KEY,
            symbol TEXT NOT NULL,
            name TEXT
        );
        """,
    )

    t_load = PythonOperator(
        task_id="load",
        python_callable=load,
        provide_context=True,
    )

    t_extract >> t_transform >> create_table_task >> t_load
