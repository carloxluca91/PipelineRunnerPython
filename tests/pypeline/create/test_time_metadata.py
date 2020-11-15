import unittest
from datetime import datetime, date
from typing import Any, Dict

from pypeline.create.metadata import TimeColumnMetadata
from tests.pypeline.create.abstract import ABCTestCreate
from utils.time import TimeUtils


class TestTimeMetadata(ABCTestCreate):

    _LOWER_BOUND_DT = ("lowerBound", "2020-11-01")
    _LOWER_BOUND_TS = ("lowerBound", "2020-11-01 12:30:00")
    _UPPER_BOUND_DT = ("upperBound", "2020-11-30")
    _UPPER_BOUND_TS = ("upperBound", "2020-11-30 23:59:59")
    _AS_STRING = ("asString", True)
    _AS_STRING_INFO = ("asStringInfo", {
                "corruptFlag": True,
                "corruptProb": 0.1})

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

        self.apply_test_for_dict(self.build_test_dict(self._LOWER_BOUND_DT))

    def test_date_as_string(self):

        self.apply_test_for_dict(self.build_test_dict(self._LOWER_BOUND_DT, self._AS_STRING))

    def test_date_as_string_with_corruption(self):

        self.apply_test_for_dict(self.build_test_dict(self._LOWER_BOUND_DT, self._UPPER_BOUND_DT, self._AS_STRING, self._AS_STRING_INFO))

    def test_simple_timestamp(self):

        self.apply_test_for_dict(self.build_test_dict(self._LOWER_BOUND_TS))

    def test_timestamp_as_string(self):

        self.apply_test_for_dict(self.build_test_dict(self._LOWER_BOUND_TS, self._AS_STRING))

    def test_timestamp_as_string_with_corruption(self):

        self.apply_test_for_dict(self.build_test_dict(self._LOWER_BOUND_TS, self._UPPER_BOUND_TS, self._AS_STRING, self._AS_STRING_INFO))


if __name__ == '__main__':
    unittest.main()
