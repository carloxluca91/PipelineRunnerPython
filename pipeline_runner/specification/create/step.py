import logging
import pandas as pd
from typing import List, Dict

from pipeline_runner.specification.abstract import AbstractPipelineStep
from pipeline_runner.specification.create.column import TypedColumn, \
    DateOrTimestampColumn, \
    RandomColumn

DATAFRAME_COLUMN_SWICTH = {

    "timestamp": DateOrTimestampColumn,
    "date": DateOrTimestampColumn,
    "string": RandomColumn,
    "int": RandomColumn
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
        self._typed_columns: List[TypedColumn] = self._parse_columns_specifications(dataframe_columns)

    @property
    def number_of_records(self) -> int:
        return self._number_of_records

    @property
    def number_of_columns(self) -> int:
        return len(self._typed_columns)

    def _parse_columns_specifications(self, dataframe_columns: List[dict]):

        list_of_typed_columns: List[TypedColumn] = []
        self._logger.info(f"Starting to process metadata for each of the {len(dataframe_columns)} column(s) "
                          f"of create step '{self._name}', "
                          f"({self._description})")

        for index, dataframe_column in enumerate(dataframe_columns):

            column_type_lc: str = dataframe_column["column_type"]
            column_name: str = dataframe_column["name"]
            column_description: str = dataframe_column["description"]

            typed_column: TypedColumn = DATAFRAME_COLUMN_SWICTH[column_type_lc].from_dict(dataframe_column)
            self._logger.info(f"Successfully parsed metadata for column # {index + 1} (columnName = '{column_name}', description = '{column_description}')")
            list_of_typed_columns.append(typed_column)

        self._logger.info(f"Successfully processed metadata for each of the {len(dataframe_columns)} column(s) "
                          f"of create step '{self._name}', "
                          f"({self._description})")

        return list_of_typed_columns

    def create(self) -> pd.DataFrame:

        dict_of_series: Dict[str, pd.Series] = {}
        sorted_typed_columns: List[TypedColumn] = sorted(self._typed_columns, key=lambda x: x.column_number)

        self._logger.info(f"Starting to populate each of the {len(sorted_typed_columns)} dataframe column(s)")
        for index, dataframe_column in enumerate(sorted_typed_columns):

            series = pd.Series(data=dataframe_column.create(self._number_of_records), name=dataframe_column._name)

            self._logger.info(f"Successfully populated column # {index} ("
                              f"name = '{dataframe_column.name}', "
                              f"description = '{dataframe_column.description}'")

            dict_of_series[dataframe_column._name] = series

        self._logger.info(f"Successfully populated each of the {len(sorted_typed_columns)} dataframe column(s)")
        pd_dataframe = pd.DataFrame.from_dict(dict_of_series)
        self._logger.info("Successfully turned the populated columns into a pd.DataFrame")
        return pd_dataframe
