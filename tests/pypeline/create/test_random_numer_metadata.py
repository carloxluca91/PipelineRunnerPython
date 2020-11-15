import unittest
from typing import Dict, Any

from pypeline.create.metadata import RandomNumberMetadata
from tests.pypeline.create.abstract import ABCTestCreate


class TestRandomNumberMetadata(ABCTestCreate):

    _LOWER_BOUND = ("lowerBound", 100)
    _UPPER_BOUND = ("upperBound", 1000)
    _OUTPUT_TYPE_INT = ("outputType", "int")
    _OUTPUT_TYPE_DOUBLE = ("outputType", "double")
    _OUTPUT_TYPE_STR = ("outputType", "str")

    def apply_test_for_dict(self, d: Dict[str, Any]):

        metadata = RandomNumberMetadata.from_dict(d)
        output_data = metadata.create_data(self._number_of_records)

        self.assertTrue(len(output_data) == self._number_of_records)
        output_type = metadata.output_function
        self.assertTrue(all(isinstance(x, output_type) for x in output_data))
        self.assertTrue(all(metadata.lower_bound <= float(x) <= metadata.upper_bound for x in output_data))

    def test_case_without_output_type(self):

        self.apply_test_for_dict(self.build_test_dict(self._LOWER_BOUND, self._UPPER_BOUND))

    def test_case_with_output_type(self):

        self.apply_test_for_dict(self.build_test_dict(self._LOWER_BOUND, self._UPPER_BOUND, self._OUTPUT_TYPE_INT))
        self.apply_test_for_dict(self.build_test_dict(self._LOWER_BOUND, self._UPPER_BOUND, self._OUTPUT_TYPE_DOUBLE))
        self.apply_test_for_dict(self.build_test_dict(self._LOWER_BOUND, self._UPPER_BOUND, self._OUTPUT_TYPE_STR))


if __name__ == '__main__':
    unittest.main()
