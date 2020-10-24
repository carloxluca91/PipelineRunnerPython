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

        from pipeline_runner.specification.create.column import DateOrTimestampColumn
        from pipeline_runner.time.format import to_date, \
            to_datetime, \
            DEFAULT_TIMESTAMP_FORMAT, \
            DEFAULT_DATE_FORMAT

        # FILTER IN ORDER TO KEEP ONLY 'date' OR 'timestamp' COLUMN TYPES
        dt_or_ts_columns: List[dict] = list(filter(lambda x: x["column_type"].lower() in ["date", "timestamp"], self.column_list))
        for index, dict_column in enumerate(dt_or_ts_columns):

            dt_or_ts_column = DateOrTimestampColumn.from_dict(dict_column)
            column_type: str = dt_or_ts_column.column_type
            is_date: bool = column_type.lower() == "date"

            self._logger.info(f"Analyzing column # {index} (name = '{dt_or_ts_column.name}', description = '{dt_or_ts_column.description}')")

            list_of_dates_or_tss: List[str] = dt_or_ts_column.create(self.number_of_records)

            # CHECK THAT upper_bound COINCIDES WITH CURRENT DATE IF None WAS PROVIDED
            upper_bound_str: str = dict_column["metadata"]["upper_bound"]

            expected_upper_bound_dt: date = datetime.now().date() if upper_bound_str is None \
                else to_date(upper_bound_str, DEFAULT_DATE_FORMAT) if is_date \
                else to_datetime(upper_bound_str, DEFAULT_TIMESTAMP_FORMAT).date()

            actual_upper_bound_dt: date = dt_or_ts_column.metadata.upper_bound_dtt if is_date \
                else dt_or_ts_column.metadata.upper_bound_dtt.date()

            self.assertEqual(expected_upper_bound_dt, actual_upper_bound_dt)

            # CHECK THAT, IF nullable, THE GENERATED DATA CONTAIN SOME None
            if dt_or_ts_column.nullable:

                self.assertIsNotNone(dt_or_ts_column.nullable_probability)
                self.assertNotEqual(0, len(list(filter(lambda x: x is None, list_of_dates_or_tss))))

            # CHECK THAT ALL OF GENERATED DATA ARE WITHIN THE STATED RANGE
            # AND THAT, IF corruption IS SPECIFIED, SOME CORRUPTED DATA ARE PRESENT
            corrupt_flag: bool = dt_or_ts_column \
                .metadata \
                .corrupt_flag

            corrupt_probability: float = dt_or_ts_column \
                .metadata \
                .corrupt_probability

            python_corrupt_format: str = dt_or_ts_column \
                .metadata \
                .python_corrupt_format

            def is_out_of_range(ts: str, ts_format: str, lower_bound: date, upper_bound: date) -> bool:

                return False if ts is None \
                    else datetime.strptime(ts, ts_format).date() > upper_bound \
                         or datetime.strptime(ts, ts_format).date() < lower_bound

            python_output_format = dt_or_ts_column \
                .metadata \
                .python_output_format

            lower_bound_dt: date = dt_or_ts_column.metadata.lower_bound_dtt if is_date \
                else dt_or_ts_column.metadata.lower_bound_dtt.date()

            upper_bound_dt: date = dt_or_ts_column.metadata.upper_bound_dtt if is_date \
                else dt_or_ts_column.metadata.upper_bound_dtt.date()

            if corrupt_flag and corrupt_probability and python_corrupt_format:

                def has_format(ts: str, ts_format: str) -> bool:

                    try:
                        datetime.strptime(ts, ts_format)
                        return True
                    except (TypeError, ValueError):
                        return False

                list_with_main_format = list(filter(lambda x: has_format(x, python_output_format), list_of_dates_or_tss))
                self.assertNotEqual(0, len(list_with_main_format))

                list_out_of_range = list(filter(lambda x: is_out_of_range(x, python_output_format, lower_bound_dt, upper_bound_dt),
                                                list_with_main_format))
                self.assertEqual(0, len(list_out_of_range))

                list_with_corrupt_format = list(filter(lambda x: has_format(x, python_corrupt_format), list_of_dates_or_tss))
                self.assertNotEqual(0, len(list_with_corrupt_format))

            else:

                self.assertEqual(0, len(list(filter(lambda x: is_out_of_range(x, python_output_format, lower_bound_dt, upper_bound_dt),
                                                    list_of_dates_or_tss))))

            self._logger.info(f"Successfully analyzed column # {index} ("
                              f"name = '{dt_or_ts_column.name}', "
                              f"description = '{dt_or_ts_column.description}')")

    def test_random_column(self):

        from pipeline_runner.specification.create.column import RandomColumn

        for index, column in enumerate(self.column_list):

            column_type_lc: str = column["column_type"].lower()
            if column_type_lc not in ["timestamp", "date"]:

                random_column = RandomColumn.from_dict(column)
                self._logger.info(f"Analyzing column # {index} (name = '{random_column._name}', description = '{random_column._description}')")
                if random_column._nullable:
                    self.assertIsNotNone(random_column._nullable_probability)

                values = random_column.metadata.values
                p = random_column.metadata.p

                self.assertIsNotNone(values)
                self.assertIsNotNone(p)
                self.assertEqual(len(p), len(values))
                self.assertEqual(sum(p), 1)

                self._logger.info(f"Successfully analyzed column # {index} (type = '{random_column._name}', description = '{random_column._description}')")


if __name__ == '__main__':
    unittest.main()
