import logging
from typing import List

from pyspark.sql import DataFrame, SparkSession
# noinspection PyProtectedMember
from pyspark.sql.types import StructField, StructType, Row

from pypeline.abstract import AbstractStep
from pypeline.create.column import TypedColumn, DateOrTimestampColumn, RandomColumn
from utils.spark import SparkUtils

_COLUMN_DTYPE = {

    "timestamp": DateOrTimestampColumn,
    "date": DateOrTimestampColumn,
    "string": RandomColumn,
    "int": RandomColumn
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

        list_of_typed_columns: List[TypedColumn] = []
        self._logger.info(f"Starting to process metadata for each of the {len(dataframe_columns)} column(s) of create step '{self.name}'")

        for index, dataframe_column in enumerate(dataframe_columns, start=1):

            column_type_lc: str = dataframe_column["columnType"]
            column_name: str = dataframe_column["name"]

            typed_column: TypedColumn = _COLUMN_DTYPE[column_type_lc].from_dict(dataframe_column)
            self._logger.info(f"Successfully parsed metadata for column # {index} ('{column_name}')")
            list_of_typed_columns.append(typed_column)

        self._logger.info(f"Successfully processed metadata for each of the {len(dataframe_columns)} column(s) of create step '{self._name}'")

        return list_of_typed_columns

    def create(self, spark_session: SparkSession) -> DataFrame:

        column_data: List[List] = []
        struct_field_list: List[StructField] = []

        sorted_typed_columns: List[TypedColumn] = sorted(self._typed_columns, key=lambda x: x.column_number)
        self._logger.info(f"Starting to populate each of the {len(sorted_typed_columns)} dataframe column(s)")
        for index, typed_column in enumerate(sorted_typed_columns, start=1):

            column_data.append(typed_column.create(self._number_of_records))
            struct_field = StructField(typed_column.name, SparkUtils.get_spark_datatype(typed_column.column_type), nullable=True)
            struct_field_list.append(struct_field)
            self._logger.info(f"Successfully populated column # {index} (name = '{typed_column.name}', description = '{typed_column.description}'")

        self._logger.info(f"Successfully populated each of the {len(sorted_typed_columns)} dataframe column(s)")

        data: List[Row] = [Row(*t) for t in zip(*column_data)]
        schema = StructType(struct_field_list)
        spark_dataframe = spark_session.createDataFrame(data, schema)

        self._logger.info(f"Successfully created pyspark DataFrame '{self.dataframe_id}'. Schema {SparkUtils.df_schema_tree_string(spark_dataframe)}")
        self._logger.info(f"Successfully executed create step '{self.name}'")
        return spark_dataframe
