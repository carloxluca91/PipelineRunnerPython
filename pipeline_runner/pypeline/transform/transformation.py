import logging
from abc import abstractmethod
from typing import Dict, Union

from pyspark.sql import DataFrame, Column

from pypeline.transform.option import DropTransformationOptions, SelectTransformationOptions, WithColumnTransformationOptions
from pypeline.transform.parser import ColumnExpressionParser

T = Union[WithColumnTransformationOptions,
          DropTransformationOptions,
          SelectTransformationOptions]


class AbstractTransformation:

    def __init__(self,
                 step_name: str,
                 transformation_options: T):

        self._logger = logging.getLogger(__name__)
        self._step_name = step_name
        self._transformation_options = transformation_options

    @property
    def step_name(self) -> str:

        return self._step_name

    @property
    def transformation_options(self) -> T:

        return self._transformation_options

    @abstractmethod
    def transform(self, df_dict: Dict[str, DataFrame]):
        pass


class WithColumnTransformation(AbstractTransformation):

    def __init__(self,
                 step_name: str,
                 transformation_options: Dict[str, str]):

        super().__init__(step_name, WithColumnTransformationOptions.from_dict(transformation_options))

    def transform(self, df_dict: Dict[str, DataFrame]):

        logger = self._logger
        transformation_options = self._transformation_options

        input_df = df_dict[transformation_options.input_source_id]
        columns_to_add = transformation_options.columns

        for index, column_to_add in enumerate(columns_to_add, start=1):

            column: Column = ColumnExpressionParser.parse_expression(column_to_add.expression)
            logger.info(f"Successfully parsed column expression # {index} ('{column_to_add.expression}') of transformationStep '{self.step_name}'")
            input_df = input_df.withColumn(column_to_add.alias, column)

        logger.info(f"Successfully parsed and added each column expression of transformationStep '{self.step_name}'")
        return input_df


class DropTransformation(AbstractTransformation):

    def __init__(self,
                 step_name: str,
                 transformation_options: Dict[str, str]):

        super().__init__(step_name, DropTransformationOptions.from_dict(transformation_options))

    def transform(self, df_dict: Dict[str, DataFrame]) -> DataFrame:

        logger = self._logger
        transformation_options = self.transformation_options
        input_df = df_dict[transformation_options.input_source_id]
        columns_to_drop = transformation_options.columns

        for index, column_to_drop in enumerate(columns_to_drop, start=1):

            logger.info(f"Dropping column # {index} ('{column_to_drop}')")
            input_df = input_df.drop(column_to_drop)

        logger.info(f"Successfully dropped each column of transformationStep '{self._step_name}'")
        return input_df


class SelectTransformation(AbstractTransformation):

    def __init__(self,
                 step_name: str,
                 transformation_options: Dict[str, str]):

        super().__init__(step_name, SelectTransformationOptions.from_dict(transformation_options))

    def transform(self, df_dict: Dict[str, DataFrame]) -> DataFrame:

        logger = self._logger
        transformation_options = self.transformation_options
        input_df = df_dict[transformation_options.input_source_id]

        columns_to_select = []
        for index, column_to_select in enumerate(transformation_options.columns, start=1):

            column: Column = ColumnExpressionParser.parse_expression(column_to_select.expression)
            column_with_alias = column if column_to_select.alias is None else column.alias(column_to_select.alias)
            logger.info(f"Successfully parsed column expression # {index} ('{column_to_select.expression}') of transformationStep '{self.step_name}'")
            columns_to_select.append(column_with_alias)

        output_df = input_df.select(*columns_to_select)
        logger.info(f"Successfully parsed and selected each column of transformationStep '{self.step_name}'")
        return output_df
