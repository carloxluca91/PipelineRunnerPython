import logging
import pandas as pd
from typing import List, Dict

from pipeline_runner.specification.abstract import AbstractPipelineStep
from pipeline_runner.specification.create.column import TypedColumn, TimestampColumn

DATAFRAME_COLUMN_SWICTH = {

    "timestamp": TimestampColumn
}


class CreateStep(AbstractPipelineStep):

    def __init__(self,
                 name: str,
                 description: str,
                 step_type: str,
                 dataframe_id: str,
                 number_of_records: int,
                 dataframe_columns: List[dict]):

        super().__init__(name, description, step_type, dataframe_id)

        self._logger = logging.getLogger(__name__)
        self._number_of_records = number_of_records
        self._dataframe_columns = dataframe_columns
        self._typed_columns: List[TypedColumn] = self._parse_columns_specifications()

    def _parse_columns_specifications(self):

        list_of_typed_columns: List[TypedColumn] = []
        self._logger.info(f"Starting to process metadata for each of the {len(self._dataframe_columns)} column(s) "
                          f"of create step '{self.name}', "
                          f"({self.description})")

        for index, dataframe_column in enumerate(self._dataframe_columns):

            column_type_lc: str = dataframe_column["column_type"]
            column_name: str = dataframe_column["name"]
            column_description: str = dataframe_column["description"]

            typed_column: TypedColumn = DATAFRAME_COLUMN_SWICTH[column_type_lc].from_dict(dataframe_column)
            self._logger.info(f"Successfully parsed metadata for column # {index + 1} (columnName = '{column_name}', description = '{column_description}')")
            list_of_typed_columns.append(typed_column)

        self._logger.info(f"Successfully processed metadata for each of the {len(self._dataframe_columns)} column(s) "
                          f"of create step '{self.name}', "
                          f"({self.description})")

        return list_of_typed_columns

    def create(self) -> pd.DataFrame:

        dict_of_series: Dict[str, pd.Series] = {}
        sorted_typed_columns: List[TypedColumn] = sorted(self._typed_columns, key=lambda x: x.column_number)

        self._logger.info(f"Starting to populate each of the {len(sorted_typed_columns)} dataframe column(s)")
        for index, dataframe_column in enumerate(sorted_typed_columns):

            series = pd.Series(data=dataframe_column.create(self._number_of_records), name=dataframe_column.name)

            self._logger.info(f"Successfully populated column # {index} ("
                              f"name = '{dataframe_column.name}', "
                              f"description = '{dataframe_column.description}'")

            dict_of_series[dataframe_column.name] = series

        self._logger.info(f"Successfully populated each of the {len(sorted_typed_columns)} dataframe column(s)")
        pd_dataframe = pd.DataFrame.from_dict(dict_of_series)
        self._logger.info("Successfully turned the populated columns into a pd.DataFrame")
        return pd_dataframe
