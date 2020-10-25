import logging
import re
from typing import List

from pyspark.sql import functions
from pyspark.sql import Column

from pipeline_runner.pipeline.column.expression import COLUMN_EXPRESSION_DICT, ColumnExpressionRegex

logger = logging.getLogger(__name__)


class ColumnExpressionParser:

    @staticmethod
    def parse_expression(column_expression: str) -> Column:

        list_of_column_expression_regex: List[ColumnExpressionRegex] = list(COLUMN_EXPRESSION_DICT.keys())
        matching_column_expression_regex: List[ColumnExpressionRegex] = list(filter(lambda x: re.match(x.regex_pattern, column_expression),
                                                                        list_of_column_expression_regex))

        if len(matching_column_expression_regex) == 0:

            logger.error(f"Unable to match such column expression '{column_expression}'")
            raise ValueError(f"Unable to match column expression '{column_expression}'")

        matching_column_expression: ColumnExpressionRegex = matching_column_expression_regex[0]
        matching_column_transformation = COLUMN_EXPRESSION_DICT[matching_column_expression](column_expression)
        logger.info(f"Matched function with name '{matching_column_transformation.function_name}'")

        nested_function_str: str = matching_column_transformation.nested_function_str
        df_col_match = re.match(ColumnExpressionRegex.DF_COL.regex_pattern, nested_function_str)
        lit_col_match = re.match(ColumnExpressionRegex.LIT_COL.regex_pattern, nested_function_str)
        if df_col_match or lit_col_match:

            logger.info(f"No further nested function detected ('{nested_function_str}')")
            input_column: Column = functions.col(df_col_match.group(2)) if df_col_match else functions.lit(lit_col_match.group(2))
            return matching_column_transformation.transform(input_column)

        else:

            logger.info(f"Nested function detected ('{nested_function_str}'. Trying to resolve it)")
            return matching_column_transformation\
                .transform(ColumnExpressionParser.parse_expression(nested_function_str))
