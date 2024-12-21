from airflow import DAG
from datetime import datetime, timedelta
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator
from airflow.providers.postgres.operators.postgres import PostgresOperator
from airflow.hooks.base import BaseHook
from airflow.models import Variable
from cryptography.fernet import Fernet
import pandas as pd
import psycopg2
from psycopg2.extras import execute_values
import subprocess

# Kunci enkripsi email
ENCRYPTION_KEY = Fernet.generate_key()
fernet = Fernet(ENCRYPTION_KEY)

default_args = {
    'owner': 'Zaqiy',
    'depends_on_past': False,
    'email': ['zaqiyaryono@gmail.com'],
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 0,
    'retry_delay': timedelta(minutes=1)
}

dag = DAG(
    'tugas_akhir',
    default_args=default_args,
    description='scouting_player',
    schedule_interval=timedelta(days=1),
    start_date=datetime(2024, 12, 19),
    catchup=False 
)

# Menjalankan script spark
spark_script_path = "/home/zaqiy/spark-script/player_analysis.py" 
run_spark_job = BashOperator(
    task_id='run_spark_overall_score',
    bash_command=f"spark-submit --master local[*] --jars /home/zaqiy/postgresql-42.7.4.jar {spark_script_path}",
    dag=dag,
)

# Masking nomor telepon
def mask_phone_number(data):
    data["contact"] = data["contact"].apply(
        lambda x: str(x)[:2] + "*" * (len(str(x))-4) + str(x)[-2:] if isinstance(x, str) or isinstance(x, int) else x
    )
    return data

# Mengenkripsi email
def encrypt_email(data):
    data['email'] = data["email"].apply(
        lambda x: fernet.encrypt(x.encode()).decode()
    )
    return data

# Menghindari apostrof pada SQL
def escape_apostrophes(value):
    if isinstance(value, str):
        return value.replace("'", "''") 
    return value

# Memasukkan data ke PostgreSQL
def insert_data_to_postgresql(df, table_name):
    try:
        conn_id = 'postgres_conn' 
        conn = BaseHook.get_connection(conn_id)
        
        conn = psycopg2.connect(
            dbname=conn.schema, 
            user=conn.login, 
            password=conn.password, 
            host=conn.host, 
            port=conn.port
        )
        
        cursor = conn.cursor()

        # Menyiapkan data dan query SQL
        records = df.to_records(index=False)
        cols = list(df.columns)
        insert_query = f"INSERT INTO {table_name} ({', '.join(cols)}) VALUES %s"

        # Menjalankan query
        execute_values(cursor, insert_query, records)
        conn.commit()

        cursor.close()
        conn.close()

        print(f"Data successfully inserted into {table_name} table.")

    except Exception as e:
        print(f"Error inserting data into PostgreSQL: {e}")

# Memproses Data
def process_data(**kwargs):
    file_path = '/home/zaqiy/overall_score_analysis.csv'
    
    data = pd.read_csv(file_path)
    
    data = mask_phone_number(data)
    data = encrypt_email(data)
    
    for col in ['player_name', 'team', 'position', 'agent_name', 'contact', 'email']:
        data[col] = data[col].apply(escape_apostrophes)
    
    kwargs['ti'].xcom_push(key='data_rows', value=data.to_dict(orient='records'))

process_data_task = PythonOperator(
    task_id='process_data',
    python_callable=process_data,
    provide_context=True,
    dag=dag,
)

# Membuat tabel di PostgreSQL
create_table_sql = """
CREATE TABLE IF NOT EXISTS player_data (
    id SERIAL PRIMARY KEY,
    player_name VARCHAR(100),
    position VARCHAR(100),
    height INT,
    age INT,
    appearance INT,
    fitness_score FLOAT,
    overall_score FLOAT,
    current_value INT,
    currency VARCHAR(100)
);

CREATE TABLE IF NOT EXISTS agent_data (
    id SERIAL PRIMARY KEY,
    agent_name VARCHAR(100),
    contact VARCHAR(300),
    email VARCHAR(300),
    start_contract VARCHAR(100),
    end_contract VARCHAR(100)
);

CREATE TABLE IF NOT EXISTS team_data (
    id SERIAL PRIMARY KEY,
    team VARCHAR(100),
    league VARCHAR(300),
    nation VARCHAR(300)
);
"""

create_table_task = PostgresOperator(
    task_id="create_table",
    sql=create_table_sql,
    postgres_conn_id='postgres_conn', 
    autocommit=True,
    dag=dag,
)

# Memasukkan data ke PostgreSQL
insert_table_task = PostgresOperator(
    task_id='import_table',
    postgres_conn_id='postgres_conn',
    sql="""    
    INSERT INTO player_data (
        player_name, position, height, age, appearance, 
        fitness_score, overall_score, current_value, currency
    )
    VALUES
    {% for row in ti.xcom_pull(task_ids='process_data', key='data_rows') %} 
        ('{{ row['player_name'] }}', '{{ row['position'] }}', {{ row['height'] }},
         {{ row['age'] }}, {{ row['appearance'] }}, {{ row['fitness_score'] }}, 
         {{ row['overall_score'] }}, {{ row['current_value'] }}, '{{ row['currency'] }}')
        {% if not loop.last %}, {% endif %}
    {% endfor %};

    INSERT INTO agent_data (
        agent_name, contact, email, start_contract, end_contract
    )
    VALUES
    {% for row in ti.xcom_pull(task_ids='process_data', key='data_rows') %} 
        ('{{ row['agent_name'] }}', '{{ row['contact'] }}', '{{ row['email'] }}', 
        '{{ row['start_contract'] }}', '{{ row['end_contract'] }}')
        {% if not loop.last %}, {% endif %}
    {% endfor %};

    INSERT INTO team_data (
        team, league, nation
    )
    VALUES
    {% for row in ti.xcom_pull(task_ids='process_data', key='data_rows') %} 
        ('{{ row['team'] }}', '{{ row['league'] }}', '{{ row['nation'] }}')
        {% if not loop.last %}, {% endif %}
    {% endfor %};
    """
)

run_spark_job >> create_table_task >> process_data_task >> insert_table_task