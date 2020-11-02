import logging
from abc import ABC, abstractmethod
from typing import Dict

from pyspark.sql import DataFrame, Column

from pipeline_runner.pipeline.abstract import AbstractStep
from pipeline_runner.pipeline.transform.option import \
    WithColumnTransformationOptions, \
    DropColumnTransformationOptions, \
    SelectTransformationOptions

from pipeline_runner.pipeline.transform.parser import ColumnExpressionParser


class AbstractTransformation(AbstractStep, ABC):

    def __init__(self,
                 name: str,
                 description: str,
                 step_type: str,
                 dataframe_id: str,
                 transformation_options):

        super().__init__(name, description, step_type, dataframe_id)

        self._logger = logging.getLogger(__name__)
        self._transformation_options = transformation_options

    @property
    def transformation_options(self):

        return self._transformation_options

    @abstractmethod
    def transform(self, df_dict: Dict[str, DataFrame]) -> DataFrame:
        pass


class WithColumnTransformation(AbstractTransformation):

    def __init__(self,
                 name: str,
                 description: str,
                 step_type: str,
                 dataframe_id: str,
                 transformation_options: dict):

        super().__init__(name, description, step_type, dataframe_id, WithColumnTransformationOptions.from_dict(transformation_options))

    def transform(self, df_dict: Dict[str, DataFrame]):

        logger = self._logger
        transformation_options = self.transformation_options
        input_df = df_dict[transformation_options.input_source_id]
        columns_to_add = transformation_options.columns

        for index, column_to_add in enumerate(columns_to_add, start=1):

            column: Column = ColumnExpressionParser.parse_expression(column_to_add.expression)
            logger.info(f"Successfully parsed column expression # {index} ('{column_to_add.expression}') of transformationStep '{self.name}'")
            input_df = input_df.withColumn(column_to_add.alias, column)

        logger.info(f"Successfully parsed and added each column expression of transformationStep '{self.name}'")
        return input_df


class DropTransformation(AbstractTransformation):

    def __init__(self,
                 name: str,
                 description: str,
                 step_type: str,
                 dataframe_id: str,
                 transformation_options: dict):

        super().__init__(name, description, step_type, dataframe_id, DropColumnTransformationOptions.from_dict(transformation_options))

    def transform(self, df_dict: Dict[str, DataFrame]) -> DataFrame:

        logger = self._logger
        transformation_options = self.transformation_options
        input_df = df_dict[transformation_options.input_source_id]
        columns_to_drop = transformation_options.columns

        for column_to_drop in columns_to_drop:

            logger.info(f"Dropping column '{column_to_drop}'")
            input_df = input_df.drop(column_to_drop)

        logger.info(f"Successfully dropped each column of transformationStep {self.name}")
        return input_df


class SelectTransformation(AbstractTransformation):

    def __init__(self,
                 name: str,
                 description: str,
                 step_type: str,
                 dataframe_id: str,
                 transformation_options: dict):

        super().__init__(name, description, step_type, dataframe_id, SelectTransformationOptions.from_dict(transformation_options))

    def transform(self, df_dict: Dict[str, DataFrame]) -> DataFrame:

        logger = self._logger
        transformation_options = self.transformation_options
        input_df = df_dict[transformation_options.input_source_id]

        columns_to_select = []
        for index, column_to_select in enumerate(transformation_options.columns, start=1):

            column: Column = ColumnExpressionParser.parse_expression(column_to_select.expression)
            column_with_alias = column if column_to_select.alias is None else column.alias(column_to_select.alias)
            logger.info(f"Successfully parsed column expression # {index} ('{column_to_select.expression}') of transformationStep '{self.name}'")
            columns_to_select.append(column_with_alias)

        output_df = input_df.select(*columns_to_select)
        logger.info(f"Successfully parsed and selected each column of transformationStep '{self.name}'")
        return output_df
