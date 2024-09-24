from airflow import DAG
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator
from airflow.utils.task_group import TaskGroup
from datetime import datetime, timedelta


# Параметры DAG
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,    
    'retries': 1,
    'retry_delay': timedelta(minutes=5)
}

# Определение DAG
with DAG(
    dag_id='av_dag',
    default_args=default_args,
    schedule_interval='0 7 * * *',
    start_date=datetime(2024, 9, 24),
    catchup=False
) as dag:

    #Группа задач для обработки каждого дня
    with TaskGroup('daily_proccesing') as daily_processing:
        for i in range(7):
            # Динамически передаем по одной дате из недели
            day = "{{ macros.ds_add(ds, -" + str(7 - i) + ") }}"
            daily_agg_task = SparkSubmitOperator(
                task_id = f'daily_task_{i+1}',
                application='/home/al.vorontsova/scripts/daily_agg.py',
                conn_id='spark',
                conf={
                    'spark.executor.memory': '2g',
                    'spark.executor.cores': '2'
                },
                application_args = [day]
                )

    # Задача агрегирования недельных данных
    input_date = "{{ ds }}"   # Дата запуска DAG  
    aggregate_week = SparkSubmitOperator(
        task_id='aggregate_weekly',
        application="/home/al.vorontsova/scripts/weekly_agg.py",
        conn_id='spark',
        conf={
            'spark.executor.memory': '2g',
            'spark.executor.cores': '2'
        },
        application_args=[input_date]
    )

    # Определение последовательности выполнения задач
    daily_processing >> aggregate_week