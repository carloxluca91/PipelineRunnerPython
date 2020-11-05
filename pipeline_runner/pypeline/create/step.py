import logging
import pandas as pd
from typing import List, Dict

from pyspark.sql import DataFrame, SparkSession

from pypeline.abstract import AbstractStep
from pypeline.create.column import TypedColumn, DateOrTimestampColumn, RandomColumn
from utils.spark import df_schema_tree_string

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


class CreateStep(AbstractStep):

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

        logger = self._logger

        list_of_typed_columns: List[TypedColumn] = []
        logger.info(f"Starting to process metadata for each of the {len(dataframe_columns)} column(s) "
                    f"of create step '{self._name}' "
                    f"('{self._description}')")

        for index, dataframe_column in enumerate(dataframe_columns, start=1):

            column_type_lc: str = dataframe_column["columnType"]
            column_name: str = dataframe_column["name"]
            column_description: str = dataframe_column["description"]

            typed_column: TypedColumn = DATAFRAME_COLUMN_SWICTH[column_type_lc].from_dict(dataframe_column)
            logger.info(f"Successfully parsed metadata for column # {index} (columnName = '{column_name}', description = '{column_description}')")
            list_of_typed_columns.append(typed_column)

        logger.info(f"Successfully processed metadata for each of the {len(dataframe_columns)} column(s) "
                    f"of create step '{self._name}' "
                    f"('{self._description}')")

        return list_of_typed_columns

    def create(self, spark_session: SparkSession) -> DataFrame:

        logger = self._logger

        dict_of_series: Dict[str, pd.Series] = {}
        dict_of_dtypes: Dict[str, str] = {}
        sorted_typed_columns: List[TypedColumn] = sorted(self._typed_columns, key=lambda x: x.column_number)

        logger.info(f"Starting to populate each of the {len(sorted_typed_columns)} dataframe column(s)")
        for index, typed_column in enumerate(sorted_typed_columns, start=1):

            series = pd.Series(data=typed_column.create(self._number_of_records), name=typed_column._name)
            dict_of_series[typed_column.name] = series
            dict_of_dtypes[typed_column.name] = PD_DATAFRAME_DTYPE.get(typed_column.column_type, "str")
            logger.info(f"Successfully populated column # {index} (name = '{typed_column.name}', description = '{typed_column.description}'")

        logger.info(f"Successfully populated each of the {len(sorted_typed_columns)} dataframe column(s)")

        pd_dataframe = pd.DataFrame.from_dict(dict_of_series).astype(dtype=dict_of_dtypes)

        logger.info("Successfully created pandas.DataFrame")

        spark_dataframe = spark_session.createDataFrame(pd_dataframe)

        logger.info(f"Successfully created pyspark DataFrame '{self.dataframe_id}'. Schema {df_schema_tree_string(spark_dataframe)}")
        logger.info(f"Successfully executed create step '{self.name}'")
        return spark_dataframe
