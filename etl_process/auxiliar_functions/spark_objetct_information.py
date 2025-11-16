from etl_process.parameters import DB_PARAMETERS, S3_PARAMETERS


class SparkEtlParametes:
    def __init__(self, connector):
        self.connector = connector
        self.db_properties = DB_PARAMETERS.get("db_properties", None)
        self.db_url = DB_PARAMETERS.get("db_url", None)
        self.s3_path_root = S3_PARAMETERS.get("s3_path_root", None)
        self.s3_path_target = S3_PARAMETERS.get("s3_path_target", None)
