from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.decorators import task
from airflow.models import Variable
from airflow.providers.postgres.hooks.postgres import PostgresHook

from datetime import datetime, timedelta
import requests
import psycopg2
import logging

# Redshift 접속 함수 정의
# PostgresHook 를 통해 보안을 강화할 수 있다.
def get_Redshift_connection(autocommit=True):
    hook = PostgresHook(postgres_com_id='redshift_dev_db')
    conn = hook.get_conn()
    conn.autocommit = autocommit
    return conn.cursor()

# Extract 함수 정의
@task
def extract():
    response = requests.get("https://restcountries.com/v3.1/all")
    data = response.json()
    return data

# Transform 함수 정의 
@task
def transform(data):
    # 조건에 맞는 데이터만 선택해오는것이 이 함수의 목적

    transformed_data = [
        {
            'country' : country['name']['official'],
            'population' : country['population'],
            'area' : country['area']
        } for country in data
    ] 
    return transformed_data

# Load 함수 정의
@task
def load(tranformed_data,schema):
    logging.info("Load Start !")
    cur = get_Redshift_connection()
    
    # Table 생성
    cur.execute(f"""
        CREATE TABLE IF NOT EXISTS {schema}.countries (
            country VARCHAR(256),
            population INTEGER,
            area FLOAT
        )
    """)
    # Full refresh 로 구성 
    try : 
        cur.execute("BEGIN")
        cur.execute(f"DELETE FROM {schema}.countries")
        for row in transformed_data:
            cur.execute(f"""INSERT INTO {schema}.countries ( country, population, area)
                        VALUES ({row['country']},{row['population']},{row['area']})""")
        cur.execute("COMMIT")
    except (Exception, psycopg2.DatabaseError) as error: 
        print(error)
        cur.execute("ROLLBACK;")  
    logging.info("Load DONE !")

with DAG(
    dag_id = "ETL-COUNTRY-DATA",
    schedule = "30 6 * * 6", # 분,시,일,월,요일 ??
    start_date = datetime(2024,5,22),
    catchup=False,
    default_args = {
        'retries' : 1,  # 재시도 횟수
        'retry_delay' : timedelta(minutes=3) # 3분 마다
    }
) as dag : 
    schema = "hylee_secta9"
    data = extract()
    transformed_data = transform(data)
    load(transformed_data,schema)