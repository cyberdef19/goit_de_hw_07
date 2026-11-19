import random
import time

from airflow import DAG
from datetime import datetime
from airflow.providers.common.sql.operators.sql import SQLExecuteQueryOperator
from airflow.providers.common.sql.sensors.sql import  SqlSensor
from airflow.operators.python import BranchPythonOperator, PythonOperator

default_args = {
    'owner': 'airflow',
    'start_date': datetime(2026, 11, 18, 0, 0), 
}

def choose_branch(**kwargs):
    ti = kwargs['ti']
    medal = ti.xcom_pull(task_ids='pick_medal')
    
    if medal == 'gold':
        return 'calc_gold'
    elif medal == 'silver':
        return 'calc_silver'
    else:
        return 'calc_bronze'

connection_name = "airflow"

with DAG(
        'working_with_mysql_airflow_oleksiy',
        default_args=default_args,
        schedule_interval=None,  # DAG не має запланованого інтервалу виконання
        catchup=False,  # Вимкнути запуск пропущених задач
        tags=["oleksiy"]  # Теги для класифікації DAG
) as dag:
    
    # Завдання для створення схеми бази даних (якщо не існує)
    create_schema = SQLExecuteQueryOperator(
        task_id='create_schema',
        conn_id=connection_name,
        sql="CREATE DATABASE IF NOT EXISTS airflow;"
    )
    
    #Завдання для створення таблиці (якщо не існує)
    create_table = SQLExecuteQueryOperator(
        task_id='create_table',
        conn_id=connection_name,
        sql=
        """
        CREATE TABLE IF NOT EXISTS airflow.medals (
            id INT AUTO_INCREMENT PRIMARY KEY,
            medal_type VARCHAR(10) NOT NULL,
            count INT,
            created_at DATETIME
        );
        """
    )
    
    # Завдання для випадкового вибору медалі
    pick_medal = PythonOperator(
        task_id='pick_medal',
        python_callable=lambda: random.choice(['gold', 'silver', 'bronze'])
    )
    
    # Завдання для розгалуження на основі вибраної медалі
    pick_medal_task = BranchPythonOperator(
        task_id='pick_medal_task',
        python_callable=choose_branch
    )
    
    # Завдання для підрахунку Bronze і запису у таблицю
    calc_bronze = SQLExecuteQueryOperator(
        task_id='calc_bronze',
        conn_id=connection_name,
        sql="""
        INSERT INTO airflow.medals (medal_type, count, created_at)
        SELECT 'bronze', COUNT(*), NOW()
        FROM airflow.medals
        WHERE medal_type = 'bronze';
        """
    )
    
    # Завдання для підрахунку Silver і запису у таблицю
    calc_silver = SQLExecuteQueryOperator(
        task_id='calc_silver',
        conn_id=connection_name,
        sql="""
        INSERT INTO airflow.medals (medal_type, count, created_at)
        SELECT 'silver', COUNT(*), NOW()
        FROM airflow.medals
        WHERE medal_type = 'silver';
        """
    )
    
    # Завдання для підрахунку Gold і запису у таблицю
    calc_gold = SQLExecuteQueryOperator(
        task_id='calc_gold',
        conn_id=connection_name,
        sql="""
        INSERT INTO airflow.medals (medal_type, count, created_at)
        SELECT 'gold', COUNT(*), NOW()
        FROM airflow.medals
        WHERE medal_type = 'gold';
        """
    )
    
    # Завдання для генерації затримки
    generate_delay = PythonOperator(
        task_id='generate_delay',
        python_callable=lambda: time.sleep(35),  # Затримка на 35 секунд
	trigger_rule="none_failed_min_one_success"
    )
    
    # Завдання для перевірки правильності даних у таблиці `airflow.medals`
    check_for_correctness = SqlSensor(
        task_id='check_for_correctness',
        conn_id=connection_name,
        sql="""
        SELECT COUNT(*) > 0 AS is_fresh
        FROM airflow.medals
        WHERE created_at >= NOW() - INTERVAL 60 SECOND;
        """,
        mode='poke',  # Режим перевірки: періодична перевірка умови
        poke_interval=5,
        timeout=60,
    )   
    
    # Побудова залежностей (логіка виконання)
    
    create_schema >> create_table >> pick_medal >> pick_medal_task
    
    # після розгалуження виконується тільки одна з трьох задач
    pick_medal_task >> [calc_bronze, calc_silver, calc_gold]
    
    # після будь-якої з них — затримка і сенсор
    [calc_bronze, calc_silver, calc_gold] >> generate_delay >> check_for_correctness
    
    
    
    
