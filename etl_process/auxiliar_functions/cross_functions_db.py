from etl_process.auxiliar_functions.spark_objetct_information import SparkEtlParametes


def get_db_table_data_looper(
    object_parameter: SparkEtlParametes,
    loop_reading: bool = False,
    table_name: [str, None] = None,
    table_list: [list, None] = None,
):
    if not isinstance(loop_reading, bool):
        raise Exception("Parameters loop_reading hast to be boolean")

    if (table_name is None) & (table_list is None):
        raise Exception(
            f"Parameters not defined! Please check it out -> loop_reading: \
            '{loop_reading}', table_name:'{table_name}', table_list: \
            '{table_list}'"
        )

    if loop_reading:
        if table_list is None:
            raise Exception("The list is empty")

        result_list = []
        for element in table_list:
            db_table = element
            temporary_df = spark_connection_db_query(db_table, object_parameter)
            result_list.append(temporary_df)
        return result_list

    else:
        print("print")
        print(f"{table_name}")
        temporary_df = spark_connection_db_query(table_name, object_parameter)
        return temporary_df


def get_table_name_list(object_parameter: SparkEtlParametes) -> [list, None]:
    db_table_query = (
    """(SELECT table_name FROM information_schema.tables \ 
        WHERE table_schema = 'public') \ 
        AS table_name_list"""
    )
    table_list = spark_connection_db_query(db_table_query, object_parameter)
    result = table_list.select("table_name").rdd.flatMap(lambda x: x).collect()
    return result


def spark_connection_db_query(db_table: str, object_parameter: SparkEtlParametes):
    try:
        df = object_parameter.connector.read.jdbc(
            url=object_parameter.db_url,
            table=db_table,
            properties=object_parameter.db_properties,
        )
        return df
    except Exception as e:
        print("Error reading from PostgreSQL:", e)
        raise
