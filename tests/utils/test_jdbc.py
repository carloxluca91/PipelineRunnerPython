import mysql
import unittest

from typing import List
from mysql import connector
from tests.abstract import AbstractTest
from utils.jdbc import JDBCUtils


class TestJDBCUtils(AbstractTest):

    def setUp(self) -> None:

        self._mysql_connection = mysql.connector.connect(**JDBCUtils.get_connector_options(self._job_properties))
        self._cursor = self._mysql_connection.cursor()

    def tearDown(self) -> None:

        self._mysql_connection.close()

    def test_create_db_if_not_exists(self):

        db_name = "test_db"

        def get_db_list(cursor) -> List[str]:

            cursor.execute("SHOW DATABASES")
            return list(map(lambda y: y[0].lower(), cursor))

        prev_existing_dbs: List[str] = get_db_list(self._cursor)
        number_of_prev_existing_dbs: int = len(prev_existing_dbs)
        self.assertFalse(db_name in prev_existing_dbs)

        JDBCUtils.create_db_if_not_exists(self._cursor, db_name)

        cur_existing_dbs: List[str] = get_db_list(self._cursor)
        number_of_cur_existing_dbs: int = len(cur_existing_dbs)
        self.assertEqual(number_of_cur_existing_dbs, number_of_prev_existing_dbs + 1)
        self.assertTrue(db_name in cur_existing_dbs)
        self._cursor.execute(f"DROP DATABASE {db_name}")


if __name__ == '__main__':
    unittest.main()
