import unittest
from typing import Dict, Any

from pypeline.create.metadata import RandomColumnMetadata
from tests.pypeline.create.abstract import ABCTestMetadata


class TestRandomMetadata(ABCTestMetadata):

    def apply_test_for_dict(self, d: Dict[str, Any]):

        metadata = RandomColumnMetadata.from_dict(d)

        # Assert if random data are expected to be embedded
        has_embedded_data = d["dataOrigin"].lower() == "embedded"
        assert_has_embedded_data = self.assertTrue if has_embedded_data else self.assertFalse
        assert_has_embedded_data(metadata.has_embedded_data)

        # Assert that if not embedded, probabilities should be equally distributed
        has_embedded_probs = "probs" in d["dataInfo"]
        if not has_embedded_probs:

            self.assertTrue(all(p == 1 / len(metadata.embedded_values) for p in metadata.embedded_probs))

        if metadata.has_embedded_data:

            # Assert that ValueError in thrown whenever probs or values are mismatched
            should_raise_value_error = sum(metadata.embedded_probs) != 1 or len(metadata.embedded_probs) != len(metadata.embedded_values)
            if should_raise_value_error:

                with self.assertRaises(ValueError):
                    metadata.create_data(self._number_of_records, None)

            else:

                data = metadata.create_data(self._number_of_records, None)

                # Assert size of output data
                self.assertEqual(self._number_of_records, len(data))

                # Assert that output data match the expected type
                data_type = int if metadata.value_type == "int" else (float if metadata.value_type == "double" else str)
                self.assertTrue(all(isinstance(v, data_type) for v in data))

                # Assert that each output datum is among the provided values
                typed_values = [data_type(v) for v in metadata.embedded_values]
                self.assertTrue(all(v in [data_type(value) for value in typed_values] for v in data))

    def test_embedded_with_no_exception(self):

        d = {

            "dataOrigin": "embedded",
            "dataInfo": {

                "values": [1, 2, 3],
                "valueType": "int"
            }
        }

        self.apply_test_for_dict(d)

    def test_embedded_with_different_valuetype(self):

        d = {

            "dataOrigin": "embedded",
            "dataInfo": {

                "values": [1, 2, 3],
                "valueType": "str"
            }
        }

        self.apply_test_for_dict(d)

    def test_embedded_with_exception_on_probs_sum(self):

        d = {

            "dataOrigin": "embedded",
            "dataInfo": {

                "values": [1, 2, 3],
                "probs": [0.2, 0.2, 0.2],
                "valueType": "int"
            }
        }

        self.apply_test_for_dict(d)

    def test_embedded_with_exception_on_values_and_probs_mismatch(self):

        d = {

            "dataOrigin": "embedded",
            "dataInfo": {

                "values": [1, 2, 3],
                "probs": [0.5, 0.5],
                "valueType": "int"
            }
        }

        self.apply_test_for_dict(d)


if __name__ == '__main__':
    unittest.main()
