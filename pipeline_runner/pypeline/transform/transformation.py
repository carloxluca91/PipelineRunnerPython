import logging
from abc import abstractmethod
from typing import Dict, Union

from pyspark.sql import DataFrame, Column

from pypeline.transform.parser import ColumnExpressionParser
from pypeline.transform.option import WithColumnTransformationOptions,\
    DropTransformationOptions,\
    SelectTransformationOptions,\
    FilterTransformationOptions\

T = Union[WithColumnTransformationOptions,
          DropTransformationOptions,
          SelectTransformationOptions,
          FilterTransformationOptions]


class AbstractTransformation:

    def __init__(self,
                 transformation_options: T):

        self._logger = logging.getLogger(__name__)
        self._transformation_options = transformation_options

    @property
    def transformation_options(self) -> T:

        return self._transformation_options

    @abstractmethod
    def transform(self, df_dict: Dict[str, DataFrame]):
        pass


class WithColumnTransformation(AbstractTransformation):

    def __init__(self, transformation_options: Dict[str, str]):

        super().__init__(WithColumnTransformationOptions.from_dict(transformation_options))

    def transform(self, df_dict: Dict[str, DataFrame]):

        input_df = df_dict[self._transformation_options.input_source_id]
        original_df_columns = list(map(lambda x: x.lower(), input_df.columns))
        columns_to_add = self._transformation_options.columns

        for index, column_to_add in enumerate(columns_to_add, start=1):

            column: Column = ColumnExpressionParser.parse_expression(column_to_add.expression)
            self._logger.info(f"Successfully parsed column expression # {index} ('{column_to_add.expression}')")
            if column_to_add.alias.lower() in original_df_columns:

                self._logger.warning(f"Column '{column_to_add.alias}' already exists. "
                                     f"It will be overriden with the column expression just parsed")

            input_df = input_df.withColumn(column_to_add.alias.lower(), column)

        self._logger.info(f"Successfully parsed and added each column expression")
        return input_df


class DropTransformation(AbstractTransformation):

    def __init__(self, transformation_options: Dict[str, str]):

        super().__init__(DropTransformationOptions.from_dict(transformation_options))

    def transform(self, df_dict: Dict[str, DataFrame]) -> DataFrame:

        with self._logger as logger, self._transformation_options as options:

            input_df = df_dict[options.input_source_id]
            columns_to_drop = options.columns

            for index, column_to_drop in enumerate(columns_to_drop, start=1):

                logger.info(f"Dropping column # {index} ('{column_to_drop}')")
                input_df = input_df.drop(column_to_drop)

            logger.info(f"Successfully dropped each column")
            return input_df


class SelectTransformation(AbstractTransformation):

    def __init__(self, transformation_options: Dict[str, str]):

        super().__init__(SelectTransformationOptions.from_dict(transformation_options))

    def transform(self, df_dict: Dict[str, DataFrame]) -> DataFrame:

        with self._logger as logger, self._transformation_options as options:

            input_df = df_dict[options.input_source_id]

            columns_to_select = []
            for index, column_to_select in enumerate(options.columns, start=1):

                column: Column = ColumnExpressionParser.parse_expression(column_to_select.expression)
                column_with_alias = column if column_to_select.alias is None else column.alias(column_to_select.alias)
                logger.info(f"Successfully parsed column expression # {index} ('{column_to_select.expression}')")
                columns_to_select.append(column_with_alias)

            output_df = input_df.select(*columns_to_select)
            logger.info(f"Successfully parsed and selected each column expression")
            return output_df


class FilterTransformation(AbstractTransformation):

    def __init__(self, transformation_options: Dict[str, str]):

        super().__init__(FilterTransformationOptions.from_dict(transformation_options))

    def transform(self, df_dict: Dict[str, DataFrame]):

        with self._logger as logger, self.transformation_options as options, ColumnExpressionParser.parse_expression as parse_expression:

            input_df = df_dict[options.input_source_id]
            main_condition: Column = parse_expression(options.main_condition)
            if options.and_condition:

                and_condition: Column = parse_expression(options.and_condition)
                logger.info(f"Successfully parsed AND condition '{options.and_condition}'")
                main_condition = main_condition & and_condition

            if options.or_condition:

                or_condition: Column = parse_expression(options.or_condition)
                logger.info(f"Successfully parsed OR condition '{options.or_condition}'")
                main_condition = main_condition | or_condition

            return input_df.filter(main_condition)
