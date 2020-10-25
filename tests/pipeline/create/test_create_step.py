import unittest
from typing import List

import pandas as pd

from pipeline_runner.pipeline.create.step import CreateStep
from tests.abstract import AbstractTestCase


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
                              f"('{create_step._name}', "
                              f"description = '{create_step._description}')")

            pd_dataframe: pd.DataFrame = create_step.create()
            self.assertEqual(pd_dataframe.shape[0], create_step.number_of_records)
            self.assertEqual(pd_dataframe.shape[1], create_step.number_of_columns)

            expected_colum_order: List[str] = \
                list(map(lambda x: x._name,
                         sorted(create_step._typed_columns,
                                key=lambda x: x._column_number)))

            for expected_column, df_column in zip(expected_colum_order, pd_dataframe.columns):

                self.assertEqual(expected_column, df_column)


if __name__ == '__main__':
    unittest.main()
