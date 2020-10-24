import unittest
from typing import List

import pandas as pd

from pipeline_runner.specification.create.step import CreateStep
from test.abstract import AbstractTestCase


class TestCreateStep(AbstractTestCase):

    @classmethod
    def setUpClass(cls) -> None:
        super().setUpClass()
        cls.number_of_records = 1000

    def setUp(self) -> None:

        self.create_steps: List[dict] = self._load_json("create_step.json")["create_steps"]

    def test_create_step(self):

        for index, raw_create_step in enumerate(self.create_steps):

            create_step = CreateStep.from_dict(raw_create_step)

            self._logger.info(f"Successfully initialized create step # {index} "
                              f"('{create_step.name}', "
                              f"description = '{create_step.description}')")

            pd_dataframe: pd.DataFrame = create_step.create()
            self.assertEqual(pd_dataframe.shape[0], create_step._number_of_records)
            self.assertEqual(pd_dataframe.shape[1], len(create_step._dataframe_columns))

            expected_colum_order: List[str] = \
                list(map(lambda x: x.name,
                         sorted(create_step._typed_columns,
                                key=lambda x: x.column_number)))

            for expected_value, actual_value in zip(expected_colum_order, pd_dataframe.columns):

                self.assertEqual(expected_value, actual_value)


if __name__ == '__main__':
    unittest.main()
