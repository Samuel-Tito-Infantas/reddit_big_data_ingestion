from etl_process.auxiliar_functions.spark_objetct_information import SparkEtlParametes
from etl_process.auxiliar_functions.cross_functions_spark_operation import (
    loop_treatment_data_base_pipeline,
)


def full_partial_update_pipeline_step(
    object_parameter: SparkEtlParametes,
    mode: str,
    table_target_name: [None, str] = None,
):
    loop_treatment_data_base_pipeline(
        object_parameter=object_parameter,
        mode=mode,
        table_target_name=table_target_name,
        pipe_action="update",
    )
