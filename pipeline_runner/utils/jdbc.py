import logging

import mysql
from mysql import connector
from configparser import ConfigParser
from typing import List, Dict

from pyspark.sql import DataFrame

logger = logging.getLogger(__name__)


def get_table_schema_from_df(df: DataFrame, db_name: str, table_name: str) -> str:

    COLUMN_TYPE_DICT = {

        "string": "text",
        "int": "int",
        "double": "double",
        "date": "date",
        "timestamp": "datetime"
    }

    create_table_statement: str = f"    CREATE TABLE IF NOT EXISTS {db_name}.{table_name} ( \n\n"
    columns_definitions: List[str] = list(map(lambda x: f"      {x[0]} {COLUMN_TYPE_DICT[x[1]]}", df.dtypes))
    return create_table_statement + ",\n".join(columns_definitions) + " )"


def get_jdbc_cursor(job_properties: ConfigParser) -> mysql.connector.connection.MySQLCursor:

    jdbc_host = job_properties["jdbc"]["jdbc.default.host"]
    jdbc_port = job_properties["jdbc"]["jdbc.default.port"]
    jdbc_user = job_properties["jdbc"]["jdbc.default.userName"]
    jdbc_password = job_properties["jdbc"]["jdbc.default.passWord"]
    jdbc_use_ssl = bool(job_properties["jdbc"]["jdbc.default.useSSL"])

    logger.info(f"JDBC details: host:port = '{jdbc_host}:{jdbc_port}', "
                f"userName = '{jdbc_user}', "
                f"passWord = '{jdbc_password}', "
                f"useSSL = {jdbc_use_ssl}")

    connector_options: dict = {

        "host": jdbc_host,
        "port": jdbc_port,
        "user": jdbc_user,
        "password": jdbc_password,
        "ssl_disabled": jdbc_use_ssl,
        "raise_on_warnings": True,
    }

    # MySQL Python CONNECTOR
    mysql_connection: mysql.connector.MySQLConnection = mysql.connector.connect(**connector_options)
    logger.info(f"Successfully estabilished connection to '{connector_options['host']}:{str(connector_options['port'])}' "
                       f"with credentials ('{connector_options['user']}', '{connector_options['password']}'")

    # MySQL Python CURSOR (FOR QUERY EXECUTION)
    mysql_cursor: mysql.connector.connection.MySQLCursor = mysql_connection.cursor()
    return mysql_cursor


def get_spark_writer_jdbc_options(job_properties: ConfigParser,
                                  url_key: str = None,
                                  driver_key: str = None,
                                  user_key: str = None,
                                  password_key: str = None,
                                  use_ssl_key: str = None) -> Dict[str, str]:

    def coalesce(input_key: str, default_key: str) -> str:

        return default_key if input_key is None else input_key

    jdbc_url = job_properties["jdbc"][coalesce(url_key, "jdbc.default.url")]
    jdbc_driver = job_properties["jdbc"][coalesce(driver_key, "jdbc.default.driver.className")]
    jdbc_user = job_properties["jdbc"][coalesce(user_key, "jdbc.default.userName")]
    jdbc_password = job_properties["jdbc"][coalesce(password_key, "jdbc.default.passWord")]
    jdbc_use_ssl = job_properties["jdbc"][coalesce(use_ssl_key, "jdbc.default.useSSL")].lower()

    return {

        "url": jdbc_url,
        "driver": jdbc_driver,
        "user": jdbc_user,
        "password": jdbc_password,
        "useSSL": jdbc_use_ssl
    }
