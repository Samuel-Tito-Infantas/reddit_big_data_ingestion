DB_PARAMETERS = {
    "db_properties" : {
        "user": "myusers",
        "password": "passwords",
        "driver": "org.postgresql.Driver"
    },
    "db_url" : "jdbc:postgresql://postgre_localdb:5432/postgres_db",
} 

S3_PARAMETERS = {
    "s3_path_root": "s3a://brozen-data-lake-bucket/postgres",
    "s3_path_target": "s3a://brozen-data-lake-bucket/postgres"
}

#ETL_REFRESH_MODE_OPTIONS = ["full", "partial"]
# information_schema
#db_table = "information_schema.tables"# "customers"

ETL_ACTION_PARAMETERS = {
    "insert":{
        "mode_options":["full", "partial"],
        "source_data_partition_column": "created_at",
        "write_mode": "append"
        },
    "update":{
        "mode_options":["full", "partial"],
        "source_data_partition_column": "last_updated_at",
        "write_mode": "append",
        "id_column_table_name": {
            "customers":"customer_id",
            "transactions":"transaction_id",
            "accounts":"account_id"
    }
        },
    "refresh":{
        "mode_options":["full", "partial"],
        "source_data_partition_column": "created_at",
        "write_mode": "overwrite"
        },
}