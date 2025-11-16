from pyspark.sql.functions import col, to_date, month, year, dayofmonth, lit, concat


from etl_process.auxiliar_functions.spark_objetct_information import SparkEtlParametes
from etl_process.parameters import ETL_ACTION_PARAMETERS
from etl_process.auxiliar_functions.cross_functions_db import (
    get_db_table_data_looper,
    get_table_name_list,
)


def read_data_from_s3(
    object_parameter: SparkEtlParametes,
    path: [None, str] = None,
    list_path: [None, list] = None,
):
    if list_path:
        df = object_parameter.connector.read.parquet(*list_path)
        # connector.catalog.clearCache()
    else:
        df = object_parameter.connector.read.parquet(path)
        # connector.catalog.clearCache()
    return df


def prepare_save_table(
    object_parameter: SparkEtlParametes,
    df,
    source_data_partition_column: str,
    table_name: str,
    write_mode: str,
):
    s3_path = f"{object_parameter.s3_path_root}/{table_name}/"
    print(s3_path)

    df = create_partition_date(df, source_data_partition_column)
    df.show()
    save_data_s3(df, s3_path, write_mode)


def create_partition_date(df, source_data_partition_column: str):
    df = df.withColumn(
        "partition_date", to_date(df[f"{source_data_partition_column}"], "yyyy-MM-dd")
    )
    df = df.withColumn("year", year(df["partition_date"]))
    df = df.withColumn("month", month(df["partition_date"]))
    df = df.withColumn("day", dayofmonth(df["partition_date"]))
    return df


def save_data_s3(df, s3_path: str, write_mode: str) -> None:
    # 1. Definição das colunas de partição
    partition_cols = ["year", "month", "day"]

    # 2. Particionamento e escrita com replaceWhere
    if write_mode == "overwrite" and partition_cols:
        df_ready = df.cache()
        df_ready.count()

        df_ready = df_ready.repartition(2)

        # Cria a condição de sobrescrita 
        # (ex: 'year=2022 AND month=6 AND day=10 OR ...')
        replace_condition = create_replace_where_condition(df_ready, partition_cols)

        print(f"Executing Partitions Overwrite with Condition: {replace_condition}")

        # Executa a escrita transacional usando a opção replaceWhere
        df_ready.write.option("replaceWhere", replace_condition).mode(
            "overwrite"
        ).partitionBy(*partition_cols).parquet(s3_path)

        # BOA PRÁTICA: Libera o cache do DataFrame
        df_ready.unpersist()

    else:
        # Lógica de fallback, se write_mode for diferente ou sem particionamento
        df.write.partitionBy(*partition_cols).mode(write_mode).parquet(s3_path)


def create_replace_where_condition(df, partition_cols: list) -> str:
    """
    Cria a string de condição SQL para o replaceWhere, garantindo que
    os valores de partição sejam tratados como STRINGS no SQL.
    """
    # 1. Certifica-se de que as colunas são Strings (você já fez isso no 
    # create_partition_date, mas é um bom redundância)
    df_str = df.select(
        [col(c).cast("string").alias(c) for c in partition_cols]
    ).distinct()

    partitions_to_overwrite = df_str.collect()

    conditions = []
    for row in partitions_to_overwrite:
        # **CORREÇÃO CRÍTICA**: Adicionar aspas simples aos valores
        # Ex: "year = '2022' AND month = '03' AND day = '24'"
        cond = " AND ".join([f"{col} = '{row[col]}'" for col in partition_cols])
        conditions.append(f"({cond})")

    replace_where_condition = " OR ".join(conditions)

    if not replace_where_condition:
        return "1 = 0"

    return replace_where_condition


def loop_treatment_data_base_pipeline(
    object_parameter: SparkEtlParametes,
    pipe_action: str,
    mode: str,
    table_target_name: str | None = None,
):
    mode_options, source_data_partition_column, write_mode = extract_parameters(
        pipe_action
    )

    # if mode not in ETL_REFRESH_MODE_OPTIONS:
    if mode not in mode_options:
        raise Exception(
            f"ERROR: input parameter '{mode}' is wrong, please check it out!"
        )

    if mode == "full":
        print("Start FULL Pipeline")
        result_tables_names = get_table_name_list(object_parameter)

        for table_name_item in result_tables_names:
            print(f"processing table: {table_name_item} ...")
            single_step_pipeline(
                pipe_action,
                object_parameter,
                table_name=table_name_item,
                write_mode=write_mode,
                source_data_partition_column=source_data_partition_column,
            )

        print("FULL Pipeline has been end with Sucess!")

    if mode == "partial":
        if table_target_name:
            print("Start PARTIAL Pipeline")
            single_step_pipeline(
                pipe_action,
                object_parameter,
                table_name=table_target_name,
                write_mode=write_mode,
                source_data_partition_column=source_data_partition_column,
            )
            return

        else:
            raise Exception("ERROR: table_target not informed to full refresh!")


def extract_parameters(parameters_obj: str) -> (list, str, str):
    if parameters_obj in ETL_ACTION_PARAMETERS.keys():
        dict_parameters = ETL_ACTION_PARAMETERS.get(parameters_obj, None)

        mode_options = dict_parameters.get("mode_options", None)
        source_data_partition_column = dict_parameters.get(
            "source_data_partition_column", None
        )
        write_mode = dict_parameters.get("write_mode", None)

        return mode_options, source_data_partition_column, write_mode


def single_step_pipeline(
    action,
    object_parameter: SparkEtlParametes,
    table_name: str,
    write_mode: str,
    source_data_partition_column: str,
):
    object_parameter.connector.catalog.clearCache()

    if action == "insert":
        print("Action is Append.")
        table_target_name = costum_query_base(
            table_name=table_name,
            time_column=source_data_partition_column,
            day_interval=1,
        )
        table_result = get_db_table_data_looper(
            object_parameter,
            loop_reading=False,
            table_name=table_target_name,
            table_list=None,
        )

        # save_data()
        prepare_save_table(
            object_parameter,
            table_result,
            source_data_partition_column,
            table_name,
            write_mode=write_mode,
        )

    elif action == "update":
        print("Action is Update.")
        id_column_name = (
            ETL_ACTION_PARAMETERS.get("update")
            .get("id_column_table_name")
            .get(table_name, None)
        )

        table_target_name = costum_query_base(
            table_name=table_name,
            time_column=source_data_partition_column,
            day_interval=1,
        )
        table_result = get_db_table_data_looper(
            object_parameter,
            loop_reading=False,
            table_name=table_target_name,
            table_list=None,
        )
        print(f"table_result: {table_result}")

        final_df = update_s3_table_process(
            object_parameter,
            table_name,
            id_column_name,
            source_data_partition_column,
            write_mode,
        )

        if final_df:
            prepare_save_table(
                object_parameter,
                final_df,
                source_data_partition_column,
                table_name,
                write_mode=write_mode,
            )
            print(f"Table {table_name} save with sucess!")

    elif action == "refresh":
        print("Action is Refresh.")
        table_result = get_db_table_data_looper(
            object_parameter,
            loop_reading=False,
            table_name=table_name,
            table_list=None,
        )

        prepare_save_table(
            object_parameter,
            table_result,
            source_data_partition_column,
            table_name,
            write_mode=write_mode,
        )

    else:
        print("Unknown action.")


def costum_query_base(table_name: str, time_column: str, day_interval: int = 1):
    table_name = f"""(
               SELECT *  FROM public.{table_name}
               WHERE {time_column} > (CURRENT_DATE - INTERVAL '{day_interval} day')
                   AND {time_column} < (CURRENT_DATE + INTERVAL '{day_interval} day')
               ) AS sql_table
            """
    return table_name


def update_s3_table_process(
    object_parameter: SparkEtlParametes,
    table_name: str,
    id_column_name: str,
    source_data_partition_column: str,
    write_mode: str,
):
    s3_bucket_target = f"{object_parameter.s3_path_root}/{table_name}/"

    # [INSERÇÃO AQUI] -> Reparação dos Metadados (ANTES DA LEITURA)
    # print(f"Executando MSCK REPAIR TABLE {table_name} para sincronização 
    # do catálogo...")
    # connector.sql(f"MSCK REPAIR TABLE {table_name}").show()
    # connector.catalog.clearCache() # Limpa o cache interno do Spark

    df_parquet = read_data_from_s3(
        object_parameter, path=s3_bucket_target, list_path=None
    )

    table_target_name = costum_query_base(
        table_name=table_name,
        time_column=source_data_partition_column,
        day_interval=1,
    )
    database_df = get_db_table_data_looper(
        object_parameter,
        loop_reading=False,
        table_name=table_target_name,
        table_list=None,
    )

    database_df = create_partition_date(database_df, source_data_partition_column)

    if database_df.count() > 0:
        print("database_df")
        list_path = get_parquet_partition_path(
            object_parameter, df_parquet, database_df, id_column_name, table_name
        )
        print(f"list_path: {list_path}")
        df_final = split_filter_final_df(
            object_parameter, database_df, list_path, id_column_name
        )
        df_final.show()
        return df_final
    else:
        print(f"No data for process on table{table_name}")


def get_parquet_partition_path(
    object_parameter: SparkEtlParametes,
    df_s3,
    df_db,
    id_column_name: str,
    table_name: str,
) -> list:
    base_path = f"{object_parameter.s3_path_root}/{table_name}/"

    partition_df = df_s3.join(
        df_db.select(id_column_name), on=id_column_name, how="inner"
    )
    path_df = (
        partition_df.select("year", "month", "day")
        .distinct()
        .withColumn(
            "full_path",
            concat(
                lit(base_path),
                lit("/year="),
                partition_df["year"].cast("string"),
                lit("/month="),
                partition_df["month"].cast("string"),
                lit("/day="),
                partition_df["day"].cast("string"),
            ),
        )
        .select("full_path")
    )
    return [row.full_path for row in path_df.collect()]


def split_filter_final_df(
    object_parameter: SparkEtlParametes,
    database_df,
    list_path: list,
    id_column_name: str,
):
    df_s3_partition = read_data_from_s3(
        object_parameter, path=None, list_path=list_path
    )
    df_s3_no_changed = get_s3_no_changed_data(
        df_s3_partition, database_df, id_column_name
    )  # "customer_id")

    df_final = df_s3_no_changed.unionByName(
        database_df.select(*df_s3_no_changed.columns)
    )
    df_final.show()
    return df_final


def get_s3_no_changed_data(df_s3_partition, df_db, id_column_name: str):
    df = df_s3_partition.alias("existing").join(
        df_db.alias("new"), on=[id_column_name], how="left_anti"
    )
    return df