import unittest
from datetime import datetime, date
from typing import Any, Dict

from pypeline.create.metadata import TimeColumnMetadata
from tests.pypeline.create.abstract import ABCTestMetadata
from utils.time import TimeUtils


class TestTimeMetadata(ABCTestMetadata):

    @classmethod
    def setUpClass(cls) -> None:

        super().setUpClass()

        cls._default_upper_bound_dt: date = datetime.now().date()
        cls._default_upper_bound_dtt: datetime = datetime.now()

    def apply_test_for_dict(self, d: Dict[str, Any]):

        metadata = TimeColumnMetadata.from_dict(d)

        # Assert that the column type is correctly detected
        assert_is_date = self.assertTrue if TimeUtils.has_date_format(d["lowerBound"]) else self.assertFalse
        assert_is_date(metadata.is_date)

        lower_bound_dt: date = TimeUtils.to_date(d["lowerBound"]) if metadata.is_date else TimeUtils.to_datetime(d["lowerBound"]).date()
        upper_bound_dt: date = self._default_upper_bound_dt if "upperBound" not in d else (
            TimeUtils.to_date(d["upperBound"]) if metadata.is_date else TimeUtils.to_datetime(d["upperBound"]).date())

        # Assert that optional parsing to string is correctly detected
        as_string = ("asString" in d and d["asString"])
        assert_as_string = self.assertTrue if as_string else self.assertFalse
        assert_as_string(metadata.as_string)

        self.assertEqual(lower_bound_dt, metadata.lower_bound.date())
        self.assertEqual(upper_bound_dt, metadata.upper_bound.date())

        # Create data
        data = metadata.create_data(self._number_of_records)
        self.assertTrue(len(data) == self._number_of_records)

        # Assert that final data type is correcly detected
        all_of_type = (lambda l, t: all(isinstance(x, t) for x in l))
        self.assertTrue(all_of_type(data, str if as_string else (date if metadata.is_date else datetime)))

        if metadata.as_string:

            if not metadata.corrupt_flag:

                all_with_format = (lambda l, f: all(TimeUtils.has_format(x, f) for x in l))
                self.assertTrue(all_with_format(data, metadata.java_output_format))

            else:

                some_corrupted = (lambda l, f: any(TimeUtils.has_format(x, f) for x in l))
                self.assertTrue(some_corrupted(data, metadata.java_corrupt_format))

    def test_simple_date(self):

        d = {

            "lowerBound": "2020-11-01"
        }

        self.apply_test_for_dict(d)

    def test_date_as_string(self):

        d = {

            "lowerBound": "2020-11-01",
            "asString": True
        }

        self.apply_test_for_dict(d)

    def test_date_as_string_with_corruption(self):

        d = {

            "lowerBound": "2020-11-01",
            "upperBound": "2020-11-30",
            "asString": True,
            "asStringInfo": {
                "corruptFlag": True,
                "corruptProb": 0.1
            }
        }

        self.apply_test_for_dict(d)

    def test_simple_timestamp(self):

        d = {

            "lowerBound": "2020-11-01 12:30:00"
        }

        self.apply_test_for_dict(d)

    def test_timestamp_as_string(self):

        d = {

            "lowerBound": "2020-11-01 12:30:00",
            "asString": True
        }

        self.apply_test_for_dict(d)

    def test_timestamp_as_string_with_corruption(self):

        d = {

            "lowerBound": "2020-11-01 12:30:00",
            "upperBound": "2020-11-30 23:59:59",
            "asString": True,
            "asStringInfo": {
                "corruptFlag": True,
                "corruptProb": 0.1
            }
        }

        self.apply_test_for_dict(d)


if __name__ == '__main__':
    unittest.main()
