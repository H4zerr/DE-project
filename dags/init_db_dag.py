from airflow import DAG
from airflow.providers.standard.operators.python import PythonOperator
from datetime import datetime
import pandas as pd
from sqlalchemy import create_engine
import psycopg2
import os

from sqlalchemy_utils import table_name

USER = os.getenv("POSTGRES_USER")
PASSWORD = os.getenv("POSTGRES_PASSWORD")
DB = os.getenv("POSTGRES_DB")
HOST = 'mydb'
GA_SESSIONS_TABLE = "ga_sessions"
GA_HITS_TABLE = "ga_hits"

DATA_PATH = "/opt/airflow/data/diploma_main_dataset"
CHUNK_SIZE_SESSION = 60000
CHUNK_SIZE_HITS = 30000

DB_CONN_STR = f'postgresql+psycopg2://{USER}:{PASSWORD}@{HOST}:5432/{DB}'

def change_date(df, column):
    df[column] = pd.to_datetime(df[column])

def change_time(df, column):
    df[column] = pd.to_datetime(df[column], format='%H:%M:%S').dt.time

def create_query(sql_query, conn):
    with conn.cursor() as cursor:
        cursor.execute(sql_query)
        conn.commit()

def load_data(df, table_name, chunk_size):
    engine = create_engine(DB_CONN_STR)
    len_df = len(df)
    count_chunks = int(len_df // chunk_size) + 1
    for i in range(0, len(df), chunk_size):
        df_chunk = df[i:i + chunk_size]
        df_chunk.to_sql(table_name, con=engine, if_exists='append', index=False, method='multi')
        print(f'Часть {i // chunk_size + 1} из {count_chunks} загружено в таблицу {table_name()}')

def return_query(sql_query, connect):
    with connect.cursor() as cursor:
        cursor.execute(sql_query)
        result = cursor.fetchall()
    return result

def init_database():
    conn = psycopg2.connect(
        HOST=HOST,
        PORT=5432,
        user=USER,
        password=PASSWORD,
        dbname=DB,
    )

    sql_session = """
        CREATE TABLE IF NOT EXISTS ga_sessions(
            session_id               VARCHAR(60) PRIMARY KEY,
            client_id                VARCHAR(50),
            visit_date               DATE,
            visit_time               TIME,
            visit_number             INT,
            utm_source               VARCHAR(30),
            utm_medium               VARCHAR(30),
            utm_campaign             VARCHAR(30),
            utm_adcontent            VARCHAR(30),
            utm_keyword              VARCHAR(30),
            device_category          VARCHAR(10),
            device_os                VARCHAR(20),
            device_brand             VARCHAR(20),
            device_model             VARCHAR(30),
            device_screen_resolution VARCHAR(20),
            device_browser           VARCHAR(40),
            geo_country              VARCHAR(30),
            geo_city                 VARCHAR(40)
    );
    """

    sql_hits = """
        CREATE TABLE IF NOT EXISTS ga_hits(
            id             SERIAL PRIMARY KEY,
            session_id     VARCHAR(60),
            hit_date       DATE,
            hit_time       REAL,
            hit_number     INT,
            hit_type       VARCHAR(10),
            hit_referer    VARCHAR(30),
            hit_page_path  VARCHAR(2000),
            event_category VARCHAR(40),
            event_action   VARCHAR(50),
            event_label    VARCHAR(30),
            event_value    VARCHAR(10)
    );
    """

    tables = [x[0] for x in
              return_query("SELECT table_name FROM information_schema.tables WHERE table_schema='public';", conn)]

    if GA_SESSIONS_TABLE in tables:
        print(f"Таблица {GA_SESSIONS_TABLE} уже в базе")
    else:
        df_sessions = pd.read_parquet(os.path.join(DATA_PATH, 'ga_sessions.parquet.gz'))
        change_date(df_sessions, 'visit_date')
        change_time(df_sessions, 'visit_time')
        create_query(sql_session, conn)
        load_data(df_sessions, GA_SESSIONS_TABLE, CHUNK_SIZE_SESSION)

    if GA_HITS_TABLE in tables:
        print(f"Таблица {GA_HITS_TABLE} уже в базе")
    else:
        df_hits = pd.read_parquet(os.path.join(DATA_PATH, 'ga_hits.parquet.gz'))
        change_date(df_hits, 'hit_date')
        create_query(sql_hits, conn)
        load_data(df_hits, GA_HITS_TABLE, CHUNK_SIZE_HITS)

    print("Изначальные данные успешно загружены")

with DAG(
    'init_db_dag',
    start_date=datetime(2025, 10, 12),
    schedule=None,
    catchup=False,
    description="Однократная инициализация БД с исходными данными",
) as dag:

    init_database_dag = PythonOperator(
        task_id='init_database_dag',
        python_callable=init_database,
    )
