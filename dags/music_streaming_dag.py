from airflow.decorators import dag, task
from datetime import datetime, timedelta
from airflow.operators.bash import BashOperator
import os
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator


##Load env vairables
AWS_ACCESS_KEY_ID = os.getenv("AWS_ACCESS_KEY_ID")
AWS_SECRET_ACCESS_KEY = os.getenv("AWS_SECRET_ACCESS_KEY")
RDS_POSTGRES_PASSWORD = os.getenv("RDS_POSTGRES_PASSWORD")
AWS_S3_BUCKET = os.getenv("AWS_S3_BUCKET")
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
        application="/usr/local/airflow/include/spark-job/apps/extract_transform_data.py",  # path in the Spark container
        conn_id="spark_default",               # must match your Airflow connection ID
        jars="/usr/local/airflow/include/spark-job/resources/postgresql-42.7.3.jar,/usr/local/airflow/include/spark-job/resources/aws-java-sdk-bundle-1.12.541.jar,/usr/local/airflow/include/spark-job/resources/hadoop-aws-3.3.5.jar",
       conf={
        "spark.executor.memory": "1g",
        "spark.driver.memory": "1g",
        "spark.hadoop.fs.s3a.impl": "org.apache.hadoop.fs.s3a.S3AFileSystem",
        "spark.hadoop.fs.s3a.access.key": AWS_ACCESS_KEY_ID,
        "spark.hadoop.fs.s3a.secret.key": AWS_SECRET_ACCESS_KEY,
        "spark.hadoop.fs.s3a.endpoint": "s3.amazonaws.com",
        "spark.sql.streaming.stateStore.maintenanceInterval": "60000",  # ms
        "spark.network.timeout": "60000",
        "spark.hadoop.fs.s3a.connection.timeout": "60000",  # 
        "spark.hadoop.fs.s3a.connection.establish.timeout": "60000",
        }
    )



    # validate_tranformed_data = SparkSubmitOperator(
    #     task_id="validate_tranformed_data",
    #     application="/usr/local/airflow/include/spark-job/apps/validate_transformed_data",  # path in the Spark container
    #     conn_id="spark_default",               # must match your Airflow connection ID
    #     jars="/usr/local/airflow/include/spark-job/resources/postgresql-42.7.3.jar",
    #     conf={"spark.executor.memory": "1g", "spark.driver.memory": "1g"},
    # )

        


    # kpi_implentation = SparkSubmitOperator(
    #     task_id="kpi_implementation_tracks",
    #     application="/usr/local/airflow/include/spark-job/apps/kpi_implementation.py",  # path in the Spark container
    #     conn_id="spark_default",               # must match your Airflow connection ID
    #     jars="/usr/local/airflow/include/spark-job/resources/postgresql-42.7.3.jar",
    #     conf={"spark.executor.memory": "1g", "spark.driver.memory": "1g"},
    # )

    # kpi_implentation = BashOperator(
    #     task_id='kpi_implementation_tracks',
    #     bash_command='spark-submit --master local[*] --jars /opt/spark/resources/postgresql-42.7.3.jar /opt/spark/apps/etl.py'
    # )

    extract_and_tranforms #>> validate_tranformed_data >> kpi_implentation

dag_instance = music_stream_etl()
