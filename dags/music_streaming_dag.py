from airflow.decorators import dag, task
from datetime import datetime, timedelta
from airflow.operators.bash import BashOperator
import os
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator

default_args = {
    "owner": "airflow",
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}

@dag(
    schedule="@daily", 
    start_date=datetime(2024, 1, 1), 
    catchup=False, 
    default_args=default_args, 
    tags=["music", "ETL", "stream"]
)
def music_stream_etl():
    # compute_hourly_metrics = AwsGlueJobOperator(
    #     task_id="compute_hourly_metrics",
    #     job_name="compute_hourly_metrics",
    #     script_location="s3://your-bucket/scripts/compute_hourly_metrics.py",
    #     aws_conn_id="aws_default",
    #     region_name="us-east-1"
    # )

    # extract_and_tranforms = BashOperator(
    #     task_id='extract_and_tansformed_tracks',
    #     bash_command='spark-submit --master local[*] --jars /opt/spark/resources/postgresql-42.7.3.jar /opt/spark/apps/etl.py'
    # )

    extract_and_tranforms = SparkSubmitOperator(
        task_id="extract_and_tansformed_tracks",
        application="/opt/spark/apps/extract_transform_data.py",  # path in the Spark container
        conn_id="spark_default",               # must match your Airflow connection ID
        jars="/opt/spark/resources/postgresql-42.7.3.jar",
        conf={"spark.executor.memory": "1g", "spark.driver.memory": "1g"},
    )



    validate_tranformed_data = SparkSubmitOperator(
        task_id="kpi_implementation_tracks",
        application="/opt/spark/apps/validate_transformed_data",  # path in the Spark container
        conn_id="spark_default",               # must match your Airflow connection ID
        jars="/opt/spark/resources/postgresql-42.7.3.jar",
        conf={"spark.executor.memory": "1g", "spark.driver.memory": "1g"},
    )

        


    kpi_implentation = SparkSubmitOperator(
        task_id="kpi_implementation_tracks",
        application="/opt/spark/apps/kpi_implementation.py",  # path in the Spark container
        conn_id="spark_default",               # must match your Airflow connection ID
        jars="/opt/spark/resources/postgresql-42.7.3.jar",
        conf={"spark.executor.memory": "1g", "spark.driver.memory": "1g"},
    )

    # kpi_implentation = BashOperator(
    #     task_id='kpi_implementation_tracks',
    #     bash_command='spark-submit --master local[*] --jars /opt/spark/resources/postgresql-42.7.3.jar /opt/spark/apps/etl.py'
    # )

    extract_and_tranforms >> validate_tranformed_data >> kpi_implentation

dag_instance = music_stream_etl()
