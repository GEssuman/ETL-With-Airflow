from airflow.decorators import dag, task
from datetime import datetime, timedelta
from airflow.operators.bash import BashOperator
import os
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator
from airflow.providers.amazon.aws.operators.glue import GlueJobOperator

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
    extract_and_tranform = GlueJobOperator(
        task_id="ExtractandCurateData",
        job_name="extract_transform",
        script_location="s3://aws-glue-assets-309797288544-eu-north-1/scripts/extract_transform_job",
        job_desc="Extractin gand  curate",
        script_args={},
        num_of_dpus=2,
        region_name="eu-north-1",
        iam_role_name="AWSGlueServiceRole-MusicETLJob"
    )
    

    kpi_implentation = GlueJobOperator(
        task_id = "ImplementKPIs",
        job_name="implement_kpi",
        script_location="s3://aws-glue-assets-309797288544-eu-north-1/scripts/implement_kpi.py",
        job_desc="implenting Kpis",
        script_args={},
        num_of_dpus=2,
        region_name="eu-north-1",
        iam_role_name="AWSGlueServiceRole-MusicETLJob"
    )
    



  
    extract_and_tranform >> kpi_implentation

dag_instance = music_stream_etl()
