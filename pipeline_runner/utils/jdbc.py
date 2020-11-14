import logging
from typing import List, Dict

from pyspark.sql import DataFrame

from utils.properties import CustomConfigParser


class JDBCUtils:

    @classmethod
    def logger(cls):

        return logging.getLogger(__name__)

    _SPARK_JDBC_DEFAULT_TYPE = "text"
    _SPARK_JDBC_TYPE_MAPPING = {

        "string": "text",
        "int": "int",
        "double": "double",
        "date": "date",
        "timestamp": "datetime"
    }

    @classmethod
    def create_db_if_not_exists(cls, mysql_cursor, db_name: str):

        logger = cls.logger()
        logger.info(f"Checking existence of JDBC database '{db_name}'")
        mysql_cursor.execute("SHOW DATABASES")

        # GET LIST OF EXISTING DATABASES
        existing_databases: List[str] = list(map(lambda x: x[0].lower(), mysql_cursor))

        # CHECK IF GIVEN DATABASE ALREADY EXISTS
        if db_name.lower() not in existing_databases:

            logger.warning(f"JDBC database '{db_name}' does not exist yet. Attempting to create it now")
            mysql_cursor.execute(f"CREATE DATABASE IF NOT EXISTS {db_name}")
            logger.info(f"Successfully created JDBC database '{db_name}'")

        else:

            logger.info(f"JDBC database '{db_name}' already exists. Thus, not much to do")

    @classmethod
    def get_jdbc_type(cls, spark_type: str) -> str:

        logger = cls.logger()
        if spark_type not in cls._SPARK_JDBC_TYPE_MAPPING:

            logger.warning(f"Datatype '{spark_type}' not defined within _SPARK_JDBC_TYPE_MAPPING. "
                                f"Returning default type ({cls._SPARK_JDBC_DEFAULT_TYPE})")

            return cls._SPARK_JDBC_TYPE_MAPPING.get(spark_type, cls._SPARK_JDBC_DEFAULT_TYPE)

    @classmethod
    def get_create_table_from_df(cls, df: DataFrame, db_name: str, table_name: str) -> str:

        create_table_statement: str = f"    CREATE TABLE IF NOT EXISTS {db_name}.{table_name} ( \n\n"
        columns_definitions: List[str] = list(map(lambda x: f"      {x[0]} {cls.get_jdbc_type(x[1])}", df.dtypes))
        return create_table_statement + ",\n".join(columns_definitions) + " )"

    @classmethod
    def get_connector_options(cls, job_properties: CustomConfigParser) -> Dict:

        jdbc_host = job_properties["jdbc.default.host"]
        jdbc_port = job_properties["jdbc.default.port"]
        jdbc_user = job_properties["jdbc.default.userName"]
        jdbc_password = job_properties["jdbc.default.passWord"]
        jdbc_use_ssl = bool(job_properties["jdbc.default.useSSL"])

        return {

            "host": jdbc_host,
            "port": jdbc_port,
            "user": jdbc_user,
            "password": jdbc_password,
            "ssl_disabled": jdbc_use_ssl,
            "raise_on_warnings": True,
        }

    @classmethod
    def get_spark_writer_jdbc_options(cls,
                                      job_properties: CustomConfigParser,
                                      url_key: str = None,
                                      driver_key: str = None,
                                      user_key: str = None,
                                      password_key: str = None,
                                      use_ssl_key: str = None) -> Dict[str, str]:

        def coalesce(input_key: str, default_key: str) -> str:

            return default_key if input_key is None else input_key

        jdbc_url = job_properties[coalesce(url_key, "jdbc.default.url")]
        jdbc_driver = job_properties[coalesce(driver_key, "jdbc.default.driver.className")]
        jdbc_user = job_properties[coalesce(user_key, "jdbc.default.userName")]
        jdbc_password = job_properties[coalesce(password_key, "jdbc.default.passWord")]
        jdbc_use_ssl = job_properties[coalesce(use_ssl_key, "jdbc.default.useSSL")].lower()

        return {

            "url": jdbc_url,
            "driver": jdbc_driver,
            "user": jdbc_user,
            "password": jdbc_password,
            "useSSL": jdbc_use_ssl
        }
