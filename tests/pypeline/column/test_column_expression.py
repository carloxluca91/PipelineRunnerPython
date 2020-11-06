import re
import unittest
from typing import List

from pypeline.transform.expression import SubstringExpression
from pypeline.transform.expression import ColumnExpressions
from tests.abstract import AbstractTestCase


class TestColumnExpression(AbstractTestCase):

    @classmethod
    def setUpClass(cls) -> None:

        super().setUpClass()

    def test_substring_transformation(self):

        substring_start_index = 0
        substring_length = 5
        column_name = "dt_inserimento"

        substring_regex = ColumnExpressions.SUBSTRING.regex
        expression_list: List[str] = [f"substring(col('{column_name}'), {substring_start_index}, {substring_length})",
                                      f"substring(lpad(col('{column_name}'), 10, '0'), {substring_start_index}, {substring_length})"]

        for index, expression in enumerate(expression_list):

            self._logger.info(f"Checking expression # {index}: '{expression}'")
            self.assertIsNotNone(re.match(substring_regex, expression))

            substring_transformation = SubstringExpression(expression)
            self.assertEqual(substring_start_index, substring_transformation.pos)
            self.assertEqual(substring_length, substring_transformation.length)

            self._logger.info(f"Successfully tested expression # {index}: '{expression}'")


if __name__ == '__main__':
    unittest.main()
