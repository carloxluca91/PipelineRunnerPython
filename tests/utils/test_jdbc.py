from collections import namedtuple
from datetime import datetime

import findspark
import mysql
import unittest

from typing import List, Tuple
from mysql import connector
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, IntegerType, StringType, DateType, TimestampType

from tests.abstract import AbstractTest
from utils.jdbc import JDBCUtils


class TestJDBCUtils(AbstractTest):

    def _get_db_list(self) -> List[str]:

        self._cursor.execute("SHOW DATABASES")
        return list(map(lambda y: y[0].lower(), self._cursor))

    def _get_tables_in_db(self, db_name: str) -> List[str]:

        self._cursor.execute(f"SHOW TABLES IN {db_name}")
        return list(map(lambda x: x[0].lower(), self._cursor))

    def setUp(self) -> None:

        self._mysql_connection = mysql.connector.connect(**JDBCUtils.get_connector_options(self._job_properties))
        self._cursor = self._mysql_connection.cursor()
        self._db_name = "test_db"

    def tearDown(self) -> None:

        self._cursor.execute(f"DROP DATABASE IF EXISTS {self._db_name}")
        self._mysql_connection.close()

    def test_create_db_if_not_exists(self):

        # Names and number of previously existing dbs
        prev_existing_dbs: List[str] = self._get_db_list()
        number_of_prev_existing_dbs: int = len(prev_existing_dbs)

        JDBCUtils.create_db_if_not_exists(self._cursor, self._db_name)

        # Names and number of currently existing dbs
        cur_existing_dbs: List[str] = self._get_db_list()
        number_of_cur_existing_dbs: int = len(cur_existing_dbs)
        if self._db_name.lower() in prev_existing_dbs:

            self.assertEqual(number_of_cur_existing_dbs, number_of_prev_existing_dbs)

        else:

            self.assertEqual(number_of_cur_existing_dbs, number_of_prev_existing_dbs + 1)
            self.assertTrue(self._db_name.lower() in cur_existing_dbs)

    def test_create_table_from_df(self):

        findspark.init()

        JDBCUtils.create_db_if_not_exists(self._cursor, self._db_name)
        self.assertTrue(self._db_name.lower() in self._get_db_list())

        table_name = "test_table"

        # Create a Dataframe with some data
        T = namedtuple("T", ["index", "name", "dt", "ts"])
        spark_session = SparkSession.builder.getOrCreate()

        data = [T(1, "luca", datetime.now().date(), datetime.now())]
        schema = StructType([StructField("index", IntegerType(), True),
                             StructField("name", StringType(), True),
                             StructField("dt", DateType(), True),
                             StructField("ts", TimestampType(), True)])

        df = spark_session.createDataFrame(data, schema)

        # Get CREATE TABLE statement related to the newly created df and execute it
        create_table_stm: str = JDBCUtils.get_create_table_from_df(df, self._db_name, table_name)
        self._cursor.execute(create_table_stm)
        self.assertTrue(table_name.lower() in self._get_tables_in_db(self._db_name))

        # Check that JDBC datatypes match with Spark datatypes
        self._cursor.execute(f"DESCRIBE {self._db_name}.{table_name}")
        jdbc_columns: List[Tuple[str, str]] = list(map(lambda x: (x[0], x[1]), self._cursor))
        self.assertEqual(len(jdbc_columns), len(df.dtypes))
        for df_column, jdbc_column in zip(df.dtypes, jdbc_columns):

            self.assertEqual(df_column[0], jdbc_column[0])
            self.assertEqual(JDBCUtils.get_jdbc_type(df_column[1]), jdbc_column[1])


if __name__ == '__main__':
    unittest.main()
