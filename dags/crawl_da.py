# import sys
# sys.path.append('/opt/airflow/scripts')

from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
from airflow.operators.dummy_operator import DummyOperator
from airflow.contrib.operators.spark_submit_operator import SparkSubmitOperator
from crawler import *

from datetime import datetime, timedelta
import logging
# from airflow.utils import timezone


postgres_driver_jar = "/opt/airflow/tmps/jars/postgresql-42.7.3.jar"

now = datetime.now()
# start_date=timezone.datetime(2023, 3, 1).astimezone(pytz.timezone('Asia/Saigon'))

with DAG(
    dag_id='Data_crawling_etl_process',
    start_date=datetime(now.year, now.month, now.day),
    # schedule_interval='0 17 * * *',
    schedule_interval='@daily',
    catchup=False,
    default_args = {
    'retries': 1
    },
    tags=['data_pipline', 'nba_stasts']
) as dag:
    
    start = DummyOperator(task_id="start_pipeline")

    task_2 = PythonOperator(
        task_id="crawl_nba_player_leaderboard",
        python_callable=get_nba_stats_data,
    )

    task_3 = PythonOperator(
        task_id="crawl_nba_team_box_scores",
        python_callable=get_team_box_scores_rs,
    )

    task_4 = PythonOperator(
        task_id="crawl_nbaget_player_box_scores",
        python_callable=get_player_box_scores_rs,
    )

    task_5 = PythonOperator(
        task_id="crawl_nba_team_leaderboard",
        python_callable=get_nba_team_stats,
    )
    spark_job_load_postgres = SparkSubmitOperator(
        task_id="python_job",
        conn_id="spark-conn",
        application="jobs/python/etl_minio.py",
        jars=postgres_driver_jar,
        driver_class_path=postgres_driver_jar,
        dag=dag
    )

    end = DummyOperator(task_id="end")

    start  >> [task_2, task_3, task_4, task_5 ]>> spark_job_load_postgres >> end