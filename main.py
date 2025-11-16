import sys

from awsglue.utils import getResolvedOptions
from awsglue.context import GlueContext
from awsglue.job import Job

from pyspark.sql import SparkSession
from pyspark.context import SparkContext

# Charrging the S3 libs

from etl_process.auxiliar_functions.spark_objetct_information import SparkEtlParametes
from etl_process.update_step.full_partial_update_pipeline import (
    full_partial_update_pipeline_step,
)
from etl_process.insert_step.full_partial_insert_pipeline import (
    full_partial_insert_pipeline_step,
)
from etl_process.refresh_step.full_partial_refresh_pipeline import (
    full_partial_pipeline_step,
)


args = getResolvedOptions(
    sys.argv,
    [
        "JOB_NAME",
        "ETL_ENVIRONMENT",
        "ACTION",
        "TASK_MODE",
        "TABLE_TARGET_NAME",
    ],
)

print(f"Starting Job: {args['JOB_NAME']}")
print(f"ETL Environment: {args['ETL_ENVIRONMENT']}")
print(f"Action: {args['ACTION']}")
print(f"Task Mode: {args['TASK_MODE']}")

action = args["ACTION"]
task_mode = args["TASK_MODE"]  # partial or full
table_target_name = args["TABLE_TARGET_NAME"]  # if None, all tables will be processed
etl_environment = args["ETL_ENVIRONMENT"]

if etl_environment == "local":
    print("Running in Local Environment")
    # You can set local-specific configurations here

    spark = (
        SparkSession.builder.appName("S3WriteTest")
        .config("spark.hadoop.fs.s3a.endpoint", "http://localstack:4566")
        .config("spark.hadoop.fs.s3a.access.key", "test")
        .config("spark.hadoop.fs.s3a.secret.key", "test")
        .config("spark.hadoop.fs.s3a.path.style.access", "true")
        .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
        .config(
            "spark.hadoop.fs.s3a.aws.credentials.provider",
            "org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider",
        )
        .getOrCreate()
    )

    glueContext = GlueContext(spark.sparkContext)

else:
    print("Running in AWS Glue Environment")
    sc = SparkContext()
    glueContext = GlueContext(sc)
    spark = glueContext.spark_session

job = Job(glueContext)
job.init(args["JOB_NAME"], args)

session_object = SparkEtlParametes(glueContext)


try:
    if (action in ["update", "insert", "refresh"]) & (
        task_mode in ["partial", "full"]
    ):
        print(
            f"Action: {action} | Mode: {task_mode} | \
                Table Target Name: {table_target_name}"
        )

        if action == "update":
            full_partial_update_pipeline_step(
                object_parameter=session_object,
                mode=task_mode,
                table_target_name=table_target_name,
            )

        elif action == "insert":
            # full_partial_insert_pipeline_step(object_parameter=session_object, 
            # mode="partial", table_target_name='customers')
            full_partial_insert_pipeline_step(
                object_parameter=session_object,
                mode=task_mode,
                table_target_name=table_target_name,
            )

        else:  # action == "refresh"
            # full_partial_pipeline_step(object_parameter=session_object, 
            # mode="partial", table_target_name='customers')
            full_partial_pipeline_step(
                object_parameter=session_object,
                mode=task_mode,
                table_target_name=table_target_name,
            )

        print(f"Ending Job: {args['JOB_NAME']}")
        job.commit()  # This tells Glue the job finished and to save the bookmark

except Exception as e:
    print(
        "Invalid Action or Task Mode provided. Please check the parameters \
        and try again."
    )
    print(f"Error details: {e}")

finally:
    print("Stopping Spark session ...")
    if etl_environment == "local":
        spark.stop()
        session_object.connector.stop_spark_session()
