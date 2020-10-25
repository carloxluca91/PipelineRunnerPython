import unittest
from typing import List

from tests.abstract import AbstractTestCase


class TestReadOption(AbstractTestCase):

    @classmethod
    def setUpClass(cls) -> None:

        super().setUpClass()

    def setUp(self) -> None:

        self.read_options_list: List[dict] = self._load_json("read_options.json")["read_options"]

    def test_csv_option(self):

        from pipeline_runner.specification.read.options import ReadCsvOptions

        self.assertTrue(len(self.read_options_list) > 1)
        first_option: dict = self.read_options_list[0]
        with self.assertRaises(FileNotFoundError):

            ReadCsvOptions(**first_option)

        second_option: dict = self.read_options_list[1]
        read_csv_option = ReadCsvOptions(**second_option)

        self.assertTrue(read_csv_option.path, "path")
        self.assertEqual(read_csv_option.separator, ";")
        self.assertFalse(read_csv_option.header)
        self.assertEqual(len(read_csv_option.df_schema), 7)

    def test_parquet_options(self):

        from pipeline_runner.specification.read.options import ReadParquetOptions

        self.assertTrue(len(self.read_options_list) > 2)
        third_option: dict = self.read_options_list[2]

        read_parquet_options = ReadParquetOptions(**third_option)
        self.assertEqual(read_parquet_options.path, "path")


if __name__ == '__main__':
    unittest.main()
