import logging
from abc import ABC, abstractmethod
from configparser import ConfigParser
from typing import List, Union, Dict

import mysql
from mysql import connector
from pyspark.sql import DataFrame, SparkSession

from pypeline.write.option import HiveTableDstOptions, JDBCTableDstOptions
from utils.jdbc import JDBCUtils


class AbstractWriter(ABC):

    def __init__(self,
                 job_properties: ConfigParser,
                 dst_options: Union[HiveTableDstOptions, JDBCTableDstOptions],
                 dst_type: str):

        self._logger = logging.getLogger(__name__)
        self._job_properties = job_properties
        self._dst_options = dst_options
        self._dst_type = dst_type

    def get(self, section: str, key: str):
        return self._job_properties[section][key]

    def get_or_else(self, section: str, key: str, default_value):
        return default_value if key is None else self.get(section, key)

    @abstractmethod
    def write(self, df: DataFrame) -> None: pass


class TableWriter(AbstractWriter, ABC):

    @abstractmethod
    def _create_db_if_not_exists(self, db_name: str) -> None:
        pass


class HiveTableWriter(TableWriter):

    def __init__(self,
                 job_properties: ConfigParser,
                 dst_options: HiveTableDstOptions,
                 spark_session: SparkSession):

        super().__init__(job_properties, dst_options, dst_type="hive")
        self._spark_session = spark_session

    def _create_db_if_not_exists(self, db_name: str) -> None:

        existing_databases: List[str] = [db.name.lower() for db in self._spark_session.catalog.listDatabases()]
        if db_name.lower() not in existing_databases:

            # CHECK CREATE DATABASE PATH
            create_db_location = self.get_or_else(self._dst_type, self._dst_options.db_location, None)
            create_db_statement = f"CREATE DATABASE IF NOT EXISTS {db_name}"
            create_db_statement_with_location = create_db_statement if create_db_location is None else \
                create_db_statement + f" LOCATION '{create_db_location}'"

            location_info = "default location" if create_db_location is None else f"location '{create_db_location}'"
            self._logger.info(f"Creating Hive database '{db_name}' at " + location_info)
            self._spark_session.sql(create_db_statement_with_location)
            self._logger.info(f"Successfully created Hive database '{db_name}' at " + location_info)

        else:

            self._logger.warning(f"Hive database '{db_name}' already exists. Thus, not much to do")

    def write(self, df: DataFrame) -> None:

        # Check coalesce option
        coalesce: int = self._dst_options.coalesce
        df_to_write_coalesce: DataFrame = df if coalesce is None else df.coalesce(coalesce)

        db_name = self.get(self._dst_type, self._dst_options.db_name)
        table_name = self.get(self._dst_type, self._dst_options.table_name)
        savemode: str = self.get(self._dst_type, self._dst_options.savemode)
        create_database = self.get_or_else(self._dst_type, self._dst_options.create_db_if_not_exists, True)

        if create_database:

            self._create_db_if_not_exists(db_name)

        existing_tables: List[str] = [tbl.name.lower() for tbl in self._spark_session.catalog.listTables(db_name)]
        if table_name.lower() in existing_tables:

            self._logger.info(f"Hive table '{db_name}.{table_name}' already exists. Starting to insert data within with savemode '{savemode}'")
            df_to_write_coalesce\
                .write\
                .mode(savemode)\
                .insertInto(f"{db_name}.{table_name}")

        else:

            self._logger.warning(f"Hive table '{db_name}.{table_name}' does not exist yet. Creating it now")

            # Check partitioning
            partition_by: List[str] = self._dst_options.partition_by
            df_writer_with_partitioning = df_to_write_coalesce.write if partition_by is None else \
                df_to_write_coalesce\
                    .write\
                    .partitionBy(partition_by)

            # CHECK TABLE HDFS LOCATION
            table_path = self.get_or_else(self._dst_type, self._dst_options.table_location, None)
            df_writer_with_path = df_writer_with_partitioning if table_path is None else \
                df_writer_with_partitioning\
                    .option("path", table_path)

            df_writer_with_path\
                .mode(savemode)\
                .saveAsTable(f"{db_name}.{table_name}")

        self._logger.info(f"Successfully inserted data into Hive table '{db_name}.{table_name}' with savemode '{savemode}'")


class JDBCTableWriter(TableWriter):

    def __init__(self,
                 job_properties: ConfigParser,
                 dst_options: JDBCTableDstOptions,
                 dst_type: str = "jdbc"):

        super().__init__(job_properties, dst_options, dst_type)

        self._mysql_connection = mysql.connector.connect(**JDBCUtils.get_connector_options(job_properties))
        self._logger.info(f"Successfully estabilished JDBC connection with default coordinates")
        self._mysql_cursor = self._mysql_connection.cursor()

    def _create_db_if_not_exists(self, db_name: str) -> None:

        JDBCUtils.create_db_if_not_exists(self._mysql_cursor, db_name)

    def write(self, df: DataFrame) -> None:

        db_name = self.get(self._dst_type, self._dst_options.db_name)
        table_name = self.get(self._dst_type, self._dst_options.table_name)
        savemode = self.get(self._dst_type, self._dst_options.savemode)
        create_database = self.get_or_else(self._dst_type, self._dst_options.create_db_if_not_exists, True)
        if create_database:

            self._create_db_if_not_exists(db_name)

        full_table_name: str = f"{db_name}.{table_name}"
        self._logger.info(f"Starting to insert data into JDBC table '{full_table_name}' using savemode '{savemode}'")

        spark_writer_options: Dict[str, str] = JDBCUtils\
            .get_spark_writer_jdbc_options(self._job_properties,
                                           url_key=self._dst_options.url,
                                           driver_key=self._dst_options.driver,
                                           user_key=self._dst_options.user,
                                           password_key=self._dst_options.pass_word,
                                           use_ssl_key=self._dst_options.use_ssl)

        df.write \
            .format("jdbc") \
            .options(**spark_writer_options) \
            .option("dbtable", full_table_name) \
            .mode(savemode) \
            .save()

        self._logger.info(f"Successfully inserted data into JDBC table '{full_table_name}' using savemode '{savemode}'")
