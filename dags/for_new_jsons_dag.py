import os
from airflow import DAG
from airflow.providers.standard.operators.python import PythonOperator
import pandas as pd
import json

from numpy.matlib import empty
from sqlalchemy import create_engine
import psycopg2
from datetime import datetime, timedelta

DATA_PATH = "/opt/airflow/data/diploma_extra_dataset"
DB_HOST = 'mydb'
DB_PORT = 5432
DB_USER = os.getenv("POSTGRES_USER")
DB_PASSWORD = os.getenv("POSTGRES_PASSWORD")
DB_NAME = os.getenv("POSTGRES_DB")

def load_json():
    engine = create_engine(f'postgresql+psycopg2://{DB_USER}:{DB_PASSWORD}@{DB_HOST}:{DB_PORT}/{DB_NAME}')

    processed_files_path = "/opt/airflow/data/processed_files.txt"
    os.makedirs(os.path.dirname(processed_files_path), exist_ok=True)

    if not os.path.isfile(processed_files_path):
        open(processed_files_path, 'w').close()

    with open(processed_files_path, 'r') as f:
        processed_files = set(f.read().splitlines())

    new_files = [f for f in os.listdir(DATA_PATH) if f.endswith('.json') and f not in processed_files]
    if not new_files:
        print("Новых файлов нет.")
        return

    for filename in new_files:
        filepath = os.path.join(DATA_PATH, filename)

        try:
            with open(filepath, 'r', encoding='utf-8') as f:
                data = json.load(f)
        except json.JSONDecodeError as e:
            print(f"Ошибка чтения {filename}:{e}")
            continue
        if not data or (isinstance(data, dict) and all(len(v) == 0 for v in data.values())):
            print(f'Файл {filename} пустой или некорректный - пропущен.')
            continue

        all_arrows = []
        for v in data.values():
            all_arrows.extend(v)
        df = pd.DataFrame(all_arrows)

        if df.empty:
            print(f'Файл {filename} не содержит данных - пропущен')
            continue

        if 'session' in filename.lower():
            table_name = "ga_sessions"
        elif 'hit' in filename.lower():
            table_name = "ga_hits"
        else:
            print(f'Пропущен неизвестный файл:{filename}')
            continue

        print(f'Загружаем {filename} в таблицу {table_name}')

        if "visit_date" in df.columns:
            df['visit_date'] = pd.to_datetime(df['visit_date'], errors='coerce').dt.date
        if 'visit_time' in df.columns:
            df['visit_time'] = pd.to_datetime(df['visit_time'], errors='coerce').dt.time
        if 'hit_date' in df.columns:
            df['hit_date'] = pd.to_datetime(df['hit_date'], errors='coerce').dt.date

        df.to_sql(table_name, con=engine, if_exists='append', index=False, method='multi')

        print(f'Файл {filename} успешно загружен в {table_name}')

        with open(processed_files_path, 'a') as f:
            f.write(filename + '\n')

default_args = {
    'owner' : 'airflow',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=2),
}

with DAG(
    'for_new_jsons_dag',
    default_args=default_args,
    description="Загрузка новых JSON-файлов в PostgreSQL",
    schedule=timedelta(minutes=10),
    start_date=datetime(2025, 10, 12),
    catchup=False,
) as dag:

    load_json_task = PythonOperator(
        task_id='load_json',
        python_callable=load_json,
    )

    load_json_task