# from pyspark.sql.functions import col, to_date, month, year, dayofmonth, lit, concat

# from etl_process.parameters import ETL_REFRESH_MODE_OPTIONS

from etl_process.auxiliar_functions.spark_objetct_information import SparkEtlParametes
from etl_process.auxiliar_functions.cross_functions_spark_operation import (
    loop_treatment_data_base_pipeline,
)

# from etl_process.auxiliar_functions.cross_functions_db import 
# get_db_table_data_looper, get_table_name_list


def full_partial_insert_pipeline_step(
    object_parameter: SparkEtlParametes,
    mode: str,
    table_target_name: [None, str] = None,
):
    loop_treatment_data_base_pipeline(
        object_parameter=object_parameter,
        mode=mode,
        table_target_name=table_target_name,
        pipe_action="insert",
    )
