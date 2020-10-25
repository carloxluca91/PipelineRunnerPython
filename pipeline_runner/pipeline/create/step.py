import logging
import pandas as pd
from typing import List, Dict

from pipeline_runner.pipeline.abstract import AbstractPipelineStep
from pipeline_runner.pipeline.create.column import TypedColumn, DateOrTimestampColumn, RandomColumn

DATAFRAME_COLUMN_SWICTH = {

    "timestamp": DateOrTimestampColumn,
    "date": DateOrTimestampColumn,
    "string": RandomColumn,
    "int": RandomColumn
}

PD_DATAFRAME_DTYPE = {

    "int": "int",
    "float": "float",
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
        dict_of_dtypes: Dict[str, str] = {}
        sorted_typed_columns: List[TypedColumn] = sorted(self._typed_columns, key=lambda x: x.column_number)

        self._logger.info(f"Starting to populate each of the {len(sorted_typed_columns)} dataframe column(s)")
        for index, typed_column in enumerate(sorted_typed_columns, start=1):

            series = pd.Series(data=typed_column.create(self._number_of_records), name=typed_column._name)

            self._logger.info(f"Successfully populated column # {index} ("
                              f"name = '{typed_column.name}', "
                              f"description = '{typed_column.description}'")

            dict_of_series[typed_column.name] = series
            dict_of_dtypes[typed_column.name] = PD_DATAFRAME_DTYPE.get(typed_column.column_type, "str")

        self._logger.info(f"Successfully populated each of the {len(sorted_typed_columns)} dataframe column(s)")
        pd_dataframe = pd.DataFrame.from_dict(dict_of_series).astype(dtype=dict_of_dtypes)
        self._logger.info("Successfully turned the populated columns into a pd.DataFrame")
        return pd_dataframe
