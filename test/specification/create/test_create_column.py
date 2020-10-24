import unittest
from typing import List

from test.abstract import AbstractTestCase


class TestCreateColumn(AbstractTestCase):

    @classmethod
    def setUpClass(cls) -> None:

        super().setUpClass()
        cls.number_of_records = 1000

    def setUp(self) -> None:

        self.column_list: List[dict] = self._load_json("create_column.json")["dataframe_columns"]

    def test_timestamp_column(self):

        from datetime import date, datetime

        from pipeline_runner.specification.create.column import TimestampColumn
        from pipeline_runner.time.format import to_datetime, DEFAULT_TIMESTAMP_FORMAT

        ts_column_list: List[dict] = list(filter(lambda x: x["column_type"].lower() == "timestamp", self.column_list))
        for index, column in enumerate(ts_column_list):

            ts_column = TimestampColumn.from_dict(column)
            self._logger.info(f"Analyzing column # {index} (name = '{ts_column.name}', description = '{ts_column.description}')")

            ts_output_format = ts_column.metadata.output_format
            list_of_tss: List[str] = ts_column.create(self.number_of_records)

            # CHECK THAT upper_bound COINCIDES WITH CURRENT DATE IF None WAS PROVIDED
            upper_bound_str: str = column["metadata"]["upper_bound"]
            upper_bound_dt_expected: date = datetime.now().date() if upper_bound_str is None \
                else to_datetime(upper_bound_str, DEFAULT_TIMESTAMP_FORMAT).date()

            self.assertEqual(upper_bound_dt_expected, ts_column.metadata.upper_bound_dtt.date())

            # CHECK THAT, IF nullable, THE GENERATED DATA CONTAIN SOME None
            if ts_column.nullable:

                self.assertIsNotNone(ts_column.nullable_probability)
                self.assertNotEqual(0, len(list(filter(lambda x: x is None, list_of_tss))))

            # CHECK THAT ALL OF GENERATED DATA ARE WITHIN THE STATED RANGE
            # AND THAT, IF tampering IS SPECIFIED, SOME CORRUPTED DATA ARE PRESENT
            tamper_flag: bool = ts_column.metadata.tamper
            tamper_prob: float = ts_column.metadata.tamper_probability
            tamper_format: str = ts_column.metadata.tamper_format

            def is_out_of_range(ts: str, ts_format: str) -> bool:

                return False if ts is None \
                    else to_datetime(ts, ts_format) > ts_column.metadata.upper_bound_dtt \
                         or to_datetime(ts, ts_format) < ts_column.metadata.lower_bound_dtt

            if tamper_flag and tamper_prob and tamper_format:

                def has_format(ts: str, ts_format: str) -> bool:

                    try:
                        to_datetime(ts, ts_format)
                        return True
                    except (TypeError, ValueError):
                        return False

                list_of_tss_with_main_format = list(filter(lambda x: has_format(x, ts_output_format), list_of_tss))
                self.assertNotEqual(0, len(list_of_tss_with_main_format))
                self.assertEqual(0, len(list(filter(lambda x: is_out_of_range(x, ts_output_format), list_of_tss_with_main_format))))

                list_of_tss_with_tamper_format = list(filter(lambda x: has_format(x, tamper_format), list_of_tss))
                self.assertNotEqual(0, len(list_of_tss_with_tamper_format))
                self.assertEqual(0, len(list(filter(lambda x: is_out_of_range(x, tamper_format), list_of_tss_with_tamper_format))))

            else:

                self.assertEqual(0, len(list(filter(lambda x: is_out_of_range(x, ts_output_format), list_of_tss))))

            self._logger.info(f"Successfully analyzed column # {index} (name = '{ts_column.name}', description = '{ts_column.description}')")

    def test_random_column(self):

        from pipeline_runner.specification.create.column import RandomColumn

        for index, column in enumerate(self.column_list):

            column_type_lc: str = column["column_type"].lower()
            if column_type_lc != "timestamp":

                random_column = RandomColumn.from_dict(column)
                self._logger.info(f"Analyzing column # {index} (name = '{random_column.name}', description = '{random_column.description}')")
                if random_column.nullable:
                    self.assertIsNotNone(random_column.nullable_probability)

                values = random_column.metadata.values
                p = random_column.metadata.p

                self.assertIsNotNone(values)
                self.assertIsNotNone(p)
                self.assertEqual(len(p), len(values))
                self.assertEqual(sum(p), 1)

                self._logger.info(f"Successfully analyzed column # {index} (type = '{random_column.name}', description = '{random_column.description}')")


if __name__ == '__main__':
    unittest.main()
