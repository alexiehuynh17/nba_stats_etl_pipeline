# import os
# from airflow import DAG
# from airflow.operators.dummy_operator import DummyOperator
# from airflow.contrib.operators.spark_submit_operator import SparkSubmitOperator
# from datetime import datetime, timedelta


# postgres_driver_jar = "/opt/airflow/tmps/jars/postgresql-42.7.3.jar"
# # movies_file = "/opt/airflow/tmps/data/nba_player_leader.csv"
# # # movies_file = "/opt/bitnami/spark/tmps/data/movies.csv"
# # postgres_db = "jdbc:postgresql://postgres:5432/da_nba"
# # postgres_user = "airflow"
# # postgres_pwd = "airflow"


# now = datetime.now()

# default_args = {
#     "owner": "airflow",
#     "depends_on_past": False,
#     "start_date": datetime(now.year, now.month, now.day),
#     "email": ["airflow@airflow.com"],
#     "email_on_failure": False,
#     "email_on_retry": False,
#     "retries": 1,
#     "retry_delay": timedelta(minutes=1)
# }

# with DAG(
#     dag_id="ETL_prgress",
#     description="This DAG is a sample of integration between Spark and DB. It reads CSV files, load them into a Postgres DB and then read them from the same Postgres DB.",
#     default_args=default_args,
#     schedule_interval=timedelta(1)
# ) as dag:

#     start = DummyOperator(task_id="start")


#     spark_job_load_postgres = SparkSubmitOperator(
#         task_id="python_job",
#         conn_id="spark-conn",
#         application="jobs/python/etl_minio.py",
#         jars=postgres_driver_jar,
#         driver_class_path=postgres_driver_jar,
#         dag=dag
#     )


#     end = DummyOperator(task_id="end")

#     start >> end