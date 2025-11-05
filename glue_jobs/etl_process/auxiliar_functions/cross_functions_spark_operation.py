from pyspark.sql.functions import col, to_date, month, year, dayofmonth 

from etl_process.auxiliar_functions.spark_objetct_information import SparkEtlParametes
from etl_process.parameters import ETL_ACTION_PARAMETERS
from etl_process.auxiliar_functions.cross_functions_db import get_db_table_data_looper, get_table_name_list


def read_data_from_s3(object_parameter:SparkEtlParametes, path:[None, str]=None, list_path:[None, list]=None):
    if list_path:
        df = object_parameter.connector.read.parquet(*list_path)
        #connector.catalog.clearCache()
    else:
        df = object_parameter.connector.read.parquet(path)
        #connector.catalog.clearCache()
    return df
    

def prepare_save_table(object_parameter:SparkEtlParametes, df, source_data_partition_column:str, table_name:str, write_mode:str):
    s3_path = f"{object_parameter.s3_path_root}/{table_name}/"
    print(s3_path)
    
    df = create_partition_date(df, source_data_partition_column)
    df.show()
    save_data_s3(df, s3_path, write_mode)
    

def create_partition_date(df, source_data_partition_column:str):
    df = df.withColumn("partition_date", to_date(df[f"{source_data_partition_column}"], "yyyy-MM-dd"))
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
        
        # O repartition(2) é mantido para garantir que o particionamento funcione (vimos que era um problema)
        df_ready = df_ready.repartition(2)
        
        # Cria a condição de sobrescrita (ex: 'year=2022 AND month=6 AND day=10 OR ...')
        replace_condition = create_replace_where_condition(df_ready, partition_cols)
        
        print(f"Executing Partitions Overwrite with Condition: {replace_condition}")

        # Executa a escrita transacional usando a opção replaceWhere
        df_ready.write\
            .option("replaceWhere", replace_condition)\
            .mode("overwrite")\
            .partitionBy(*partition_cols)\
            .parquet(s3_path)

        # BOA PRÁTICA: Libera o cache do DataFrame
        df_ready.unpersist()
            
    else:
        # Lógica de fallback, se write_mode for diferente ou sem particionamento
        df.write\
            .partitionBy(*partition_cols)\
            .mode(write_mode)\
            .parquet(s3_path)


def create_replace_where_condition(df, partition_cols: list) -> str:
    """
    Cria a string de condição SQL para o replaceWhere, garantindo que
    os valores de partição sejam tratados como STRINGS no SQL.
    """
    # 1. Certifica-se de que as colunas são Strings (você já fez isso no create_partition_date, mas é um bom redundância)
    df_str = df.select([col(c).cast("string").alias(c) for c in partition_cols]).distinct()

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


def loop_treatment_data_base_pipeline(object_parameter:SparkEtlParametes, pipe_action:str, mode:str, table_target_name:[None, str] = None):  

    mode_options, source_data_partition_column, write_mode = extract_parameters(pipe_action)
    
    #if mode not in ETL_REFRESH_MODE_OPTIONS:
    if mode not in mode_options:
         raise Exception(f"ERROR: input parameter '{mode}' is not in {MODE_OPTIONS}, please check it out!")
    
    if mode == "full":
        print("Start FULL Pipeline")
        tables_list_result = get_table_name_list(object_parameter)
        list_result = get_db_table_data_looper(object_parameter)
        return list_result
        

    if mode == "partial":
        if table_target_name:
            print("Start PARTIAL Pipeline")
            # No início do seu Glue Job, antes de ler o S3:
            #spark.sql(f"REFRESH TABLE {table_target_name}") 
            # Ou: 
            object_parameter.connector.catalog.clearCache()

            table_result = get_db_table_data_looper(object_parameter, loop_reading=False, table_name=table_target_name, table_list=None)
            #table_result.show()
            #source_data_partition_column = "last_updated_at"
            prepare_save_table(object_parameter, table_result, source_data_partition_column, table_target_name, write_mode=write_mode)
            return table_result
        
        else:
            raise Exception(f"ERROR: table_target not informed to full refresh!")


def extract_parameters(parameters_obj:str) -> (list, str, str):
    if parameters_obj in ETL_ACTION_PARAMETERS.keys():
        dict_parameters = ETL_ACTION_PARAMETERS.get(parameters_obj, None)
        
        mode_options = dict_parameters.get("mode_options", None) 
        source_data_partition_column = dict_parameters.get("source_data_partition_column", None)
        write_mode = dict_parameters.get("write_mode", None)

        return mode_options, source_data_partition_column, write_mode  


'''
from pyspark.sql import SparkSession
from awsglue.context import GlueContext
from pyspark.sql.functions import col, to_date, month, year, dayofmonth, lit, concat 

import itertools


def get_table_name_list(connector, db_url:str, db_properties:str):
    db_table = """(SELECT table_name FROM information_schema.tables WHERE table_schema = 'public') AS talbe_name_list """
    table_list = spark_connection_db_query(connector, db_url, db_table, db_properties)
    result = table_list.select("table_name").rdd.flatMap(lambda x : x).collect()
    return result


def get_table_information(
    loop_reading:bool=False, 
    table_name:[str, None]=None, 
    table_list:[list, None]=None, 
    connector=None, 
    db_properties:str=None, 
    db_url:str=None
):
    if (not isinstance(loop_reading,bool)):
        raise Exception(f"Parameters loop_reading hast to be boolean")

    if (table_name is None) & (table_list is None):
        raise Exception(f"Parameters not defined! Please check it out -> loop_reading: '{loop_reading}', table_name:'{table_name}', table_list: '{table_list}'")

    if loop_reading==True:
        if table_list is None:
            raise Exception("The list is empty")
            
        result_list = []
        for element in table_list:
            db_table = element
            temporary_df = spark_connection_db_query(connector, db_url, db_table, db_properties)
            result_list.append(temporary_df)
        return result_list
        
    else:
        print("print")
        print(f"{table_name}")
        temporary_df = spark_connection_db_query(connector, db_url, table_name, db_properties)
        return temporary_df


# def save_data_s3(df, s3_path:str, write_mode:str)-> None:
#     partition_cols = ["year", "month", "day"]
# 
#     if write_mode == "overwrite" and partition_cols:
#         df_ready = df.repartition(col("year"), col("month"), col("day"))
#         replace_condition = create_replace_where_condition(df_ready, partition_cols)
#         print(f"Executing Partitions Overwrite with Condition: {replace_condition}")
# 
#         # 2. Executa a escrita usando a opção replaceWhere
#         df_ready.write \
#             .option("replaceWhere", replace_condition) \
#             .mode("overwrite") \
#             .parquet(s3_path)
#     
#     else:
#         df_ready.write\
#             .partitionBy(*partition_cols)\
#             .mode(write_mode)\
#             .parquet(s3_path)
# 





#def read_data_from_s3(connector, path:[None, str]=None, list_path:[None, list]=None):
#    if list_path:
#        df = connector.read.parquet(*list_path)
#        connector.catalog.clearCache()
#    else:
#        df = connector.read.parquet(path)
#        connector.catalog.clearCache()
#    return df
    


 

def retrive_update_data_db(connector, db_url:str, table_name:str, db_properties:str, day_interval:int=1):
    table_name = f"""(
               SELECT *  FROM public.{table_name}
               WHERE last_updated_at > (CURRENT_DATE - INTERVAL '{day_interval} day')
                   AND last_updated_at < (CURRENT_DATE + INTERVAL '{day_interval} day')
               ) AS sql_table
            """
    database_df = spark_connection_db_query(connector, db_url, table_name, db_properties)
    return database_df


def get_parquet_partition_path(df_s3, df_db, id_column_name:str, root_path:str, table_name:str)-> list:
    base_path = f"{root_path}/{table_name}"
    
    partition_df = df_s3.join(df_db.select(id_column_name),
                              on = id_column_name,
                              how = "inner"
                             )

    path_df = (
        partition_df
            .select("year", "month", "day")
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
                    )
            ).select("full_path")
        
    )
    return [row.full_path for row in path_df.collect()]

'''