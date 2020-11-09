import logging
from typing import List, Dict

from pyspark.sql import DataFrame, Row, SparkSession
from pyspark.sql.types import StructField, StructType
from pypeline.abstract import AbstractStep
from pypeline.create.column import TypedColumn
from utils.spark import SparkUtils


class CreateStep(AbstractStep):

    def __init__(self,
                 name: str,
                 description: str,
                 step_type: str,
                 dataframe_id: str,
                 number_of_records: int,
                 dataframe_columns: List[Dict]):

        super().__init__(name, description, step_type, dataframe_id)

        self._logger = logging.getLogger(__name__)
        self._number_of_records = number_of_records
        self._dataframe_columns = dataframe_columns

    @property
    def number_of_records(self) -> int:
        return self._number_of_records

    @property
    def number_of_columns(self) -> int:
        return len(self._dataframe_columns)

    def create(self, spark_session: SparkSession) -> DataFrame:

        column_data: List[List] = []
        struct_fields: List[StructField] = []

        self._logger.info(f"Starting to populate each of the {self.number_of_columns} dataframe column(s)")
        for index, dict_ in enumerate(sorted(self._dataframe_columns, key=lambda x: x["columnNumber"]), start=1):

            typed_column = TypedColumn.from_dict(dict_)
            column_data.append(typed_column.create(self._number_of_records))
            column_type: str = "int" if typed_column.column_type == "rowId".lower() else \
                ("string" if typed_column.is_date_or_timestamp_as_string else typed_column.column_type)

            struct_field = StructField(typed_column.name, SparkUtils.get_spark_datatype(column_type), nullable=True)
            struct_fields.append(struct_field)
            self._logger.info(f"Successfully populated column # {index} ('{typed_column.name}')")

        self._logger.info(f"Successfully populated each of the {self.number_of_columns} dataframe column(s)")

        data: List[Row] = [Row(*t) for t in zip(*column_data)]
        spark_dataframe = spark_session.createDataFrame(data, StructType(struct_fields))

        self._logger.info(f"Successfully created pyspark DataFrame '{self.dataframe_id}'. Schema {SparkUtils.df_schema_tree_string(spark_dataframe)}")
        self._logger.info(f"Successfully executed create step '{self.name}'")
        return spark_dataframe
