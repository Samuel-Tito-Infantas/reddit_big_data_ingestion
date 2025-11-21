import pytest
import operator
import re

from etl_process.auxiliar_functions.cross_functions_spark_operation import costum_query_base, create_replace_where_condition
case_01 = """(
               SELECT *  FROM public.customers
               WHERE created_at > (CURRENT_DATE - INTERVAL '30 day')
                   AND created_at < (CURRENT_DATE + INTERVAL '30 day')
               ) AS sql_table
            """

case_02 = """(
               SELECT *  FROM public.orders
               WHERE order_date > (CURRENT_DATE - INTERVAL '7 day')
                   AND order_date < (CURRENT_DATE + INTERVAL '7 day')
               ) AS sql_table
            """

case_03 = """(
               SELECT *  FROM public.products1
               WHERE added_on > (CURRENT_DATE - INTERVAL '15 day')
                   AND added_on < (CURRENT_DATE + INTERVAL '15 day')
               ) AS sql_table
            """

@pytest.mark.parametrize(
    "table_name, time_column, day_interval, expected_substring, comparison_op",[
        pytest.param("customers", "created_at", 30, case_01, operator.eq, id="case_01"),
        pytest.param("orders", "order_date", 7, case_02, operator.eq, id="case_02"),
        pytest.param("products", "added_on", 15, case_03, operator.ne, id="case_03"),
    ]
)
def test_costum_query_base(table_name, time_column, day_interval, expected_substring, comparison_op):
    query_result = costum_query_base(table_name, time_column, day_interval)
    assert comparison_op(clean_sql(expected_substring) in clean_sql(query_result), True)

def clean_sql(sql_string):
    return re.sub(r'\s+', ' ', sql_string).strip()


from etl_process.auxiliar_functions.spark_objetct_information import SparkEtlParametes
from etl_process.parameters import DB_PARAMETERS, S3_PARAMETERS

@pytest.mark.parametrize(
    "spark_connector",
    [pytest.param("spark_connector", id="spark_connector_placeholder")],
)
def test_SparkETLParameters_initialization(spark_connector):
    spark_etl_params = SparkEtlParametes(spark_connector)

    assert spark_etl_params.connector == spark_connector
    assert spark_etl_params.db_properties == DB_PARAMETERS.get("db_properties", None)
    assert spark_etl_params.db_url == DB_PARAMETERS.get("db_url", None)
    assert spark_etl_params.s3_path_root == S3_PARAMETERS.get("s3_path_root", None)
    assert spark_etl_params.s3_path_target == S3_PARAMETERS.get("s3_path_target", None)




from etl_process.auxiliar_functions.cross_functions_spark_operation import create_partition_date
df_1 = [("2021-01-19 19:14:44.000",), ("2023-12-17 09:26:33.000",), ("2025-02-10 15:59:53.000",)]

df_1_expected = {"rows": [("2021-01-19 19:14:44.000", "2021-01-19", 2021, 1, 19), ("2023-12-17 09:26:33.000","2023-12-17", 2023, 12, 17), ("2025-02-10 15:59:53.000", "2025-02-10", 2025, 2, 10)] ,
                 "columns": ["created_at", "partition_date", "year", "month", "day"]}


df_2 = [("2019-1-19 19:14:44.000",), ("2023-12-17 09:26:33.000",), ("2025-2-10 15:59:53.000",)]
df_2_expected = {"rows":  [("2019-1-19 19:14:44.000", "2019-1-19", 2019, 1, 19), ("2023-12-17 09:26:33.000", "2023-12-17", 2023, 12, 17), ("2025-2-10 15:59:53.000", "2025-2-10", 2025, 2, 10)] ,
                 "columns": ["last_updated_at", "partition_date", "year", "month", "day"]}

df_3 = [("2027-08-08",), ("1998-03-01 09:26:33.000",), ("2021-2-15",)]
df_3_expected = {"rows":[("2027-08-08","2027-08-08",2027,8,8), ("1998-03-01 09:26:33.000","1998-03-01", 1998, 3, 1), ("2021-2-15","2021-2-15",2021,2,15)],
                    "columns": ["created_at", "partition_date", "year", "month", "day"]}

df_4 = [("2021-01",), ("2023/12/17",), ("2025-02-10 15:59:53.000",)]
df_4_expected = {"rows":  [("2021-01", "2021-01-01", 2021, 1, 1), ("2023/12/17", "2023-12-17", 2023, 12, 17), ("2025-02-10 15:59:53.000", "2025-02-10", 2025, 2, 10)] ,
                 "columns": ["last_updated_at", "partition_date", "year", "month", "day"]}

@pytest.mark.parametrize("df, source_data_partition_column, result_expected, comparing_op", [
    pytest.param(df_1, "created_at", df_1_expected, operator.eq,id="df_1_case"),
    pytest.param(df_2, "last_updated_at", df_2_expected, operator.eq,id="df_2_case"),
    pytest.param(df_3, "created_at", df_3_expected, operator.eq,id="df_3_case"),
    pytest.param(df_4, "last_updated_at", df_4_expected, operator.ne,id="df_4_case"),
])
def test_create_partition_date(spark_session_for_tests, df, source_data_partition_column, result_expected, comparing_op):
    spark = spark_session_for_tests
    result = spark.createDataFrame(df, [source_data_partition_column])
    expected = spark.createDataFrame(result_expected.get("rows"), result_expected.get("columns"))

    result = create_partition_date(result, source_data_partition_column)
    print("---- RESULT VS EXPECTED ----")
    result.show()
    expected.show()
    print("---- RESULT VS EXPECTED SELECTED ----")
    print(result.select("year", "month", "day").collect())
    print(expected.select("year", "month", "day").collect())
    assert comparing_op(result.select("year", "month", "day").collect(), expected.select("year", "month", "day").collect())


from pyspark.sql import SparkSession
@pytest.fixture(scope="session")
def spark_session_for_tests():
    return SparkSession.builder.appName("Local-Pypsark-Test").getOrCreate()


