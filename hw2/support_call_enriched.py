from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.mysql.hooks.mysql import MySqlHook
from airflow.models import Variable
from datetime import datetime
import pandas as pd
import duckdb
import os
import json

DUCKDB_PATH = "/usr/local/airflow/data/hw2.duckdb"
MYSQL_CONN_ID = "mysql_hw"

default_args = {
    "owner": "airflow",
    "start_date": datetime(2024, 3, 1),
    "catchup": False
}

dag = DAG(
    "support_call_enrichment_dag",
    default_args=default_args,
    schedule="@hourly",
    catchup=False,
)

def detect_new_calls(ti):
    last_loaded_time = Variable.get("last_call_time")
    mysql_hook = MySqlHook(mysql_conn_id=MYSQL_CONN_ID)

    sql = f"""
        SELECT call_id 
        FROM calls 
        WHERE call_time > '{last_loaded_time}'
        ORDER BY call_time;
    """

    conn = mysql_hook.get_conn()
    df= pd.read_sql(sql,conn)
    conn.close()
   
    new_call_id = []
    for i in df["call_id"]:
        new_call_id.append(i)
    ti.xcom_push(key="new_call_id", value=new_call_id)

task_detect_new_calls = PythonOperator(
    task_id='detect_new_calls',
    python_callable=detect_new_calls,
    dag=dag,
)

def load_telephony_details(ti):
    call_ids = ti.xcom_pull(task_ids="detect_new_calls", key="new_call_id")
    if call_ids==None:
        call_ids=[]
    telephony_data = []
    telephony_jsons = "/usr/local/airflow/dags/telephony_jsons"
    for call_id in call_ids:
        file_name = "call_"+ str(call_id)+".json"
        path = telephony_jsons+"/"+file_name
        with open(path, "r") as text:
            result= json.load(text)
        telephony_data.append(result)
    ti.xcom_push(key="records",value=telephony_data)

task_load_telephony_details = PythonOperator(
    task_id='load_telephony_details',
    python_callable=load_telephony_details,
    dag=dag,
)
task_detect_new_calls >> task_load_telephony_details

def transform_and_load_duckdb(ti):
    call_ids = ti.xcom_pull(task_ids="detect_new_calls", key="new_call_id")
    telephony_data = ti.xcom_pull(task_ids="load_telephony_details", key="records")
    mysql_hook = MySqlHook(mysql_conn_id=MYSQL_CONN_ID)
    query = """SELECT c.call_id, c.call_time, c.phone, c.direction, c.status, e.employee_id, e.full_name, e.team
               FROM calls c
               JOIN employees e ON c.employee_id=e.employee_id
               """
    conn = mysql_hook.get_conn()
    df = pd.read_sql(query, conn)
    conn.close()
    df2 = pd.DataFrame(telephony_data)
    df_merge = pd.merge(df, df2, how="inner", on="call_id")

    conn = duckdb.connect(DUCKDB_PATH)
    conn.execute("""
                CREATE TABLE IF NOT EXISTS result_ (
                call_id INTEGER,
                call_time DATETIME,
                phone VARCHAR,
                direction VARCHAR,
                status VARCHAR,
                employee_id INTEGER,
                full_name VARCHAR,
                team VARCHAR,
                duration_sec INTEGER,
                short_description VARCHAR);              
""")
    conn.execute("INSERT INTO result_ SELECT * FROM df_merge")
    conn.close()

task_transform_and_load_duckdb = PythonOperator(
    task_id='transform_and_load_duckdb',
    python_callable=transform_and_load_duckdb,
    dag=dag,
)

task_detect_new_calls >> task_load_telephony_details >> task_transform_and_load_duckdb