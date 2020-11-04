import logging

from pyspark.sql import Column
from pyspark.sql import functions

from pypeline.transform.expression import COLUMN_EXPRESSION_DICT, ColumnExpression

logger = logging.getLogger(__name__)


class ColumnExpressionParser:

    @staticmethod
    def _parse_static_expression(column_expression: str) -> Column:

        matching_static_expressions = list(filter(lambda x: x.is_static and x.match(column_expression), ColumnExpression))
        matching_static_expression = matching_static_expressions[0]
        match = matching_static_expression.match(column_expression)
        if matching_static_expression == ColumnExpression.CURRENT_DATE_OR_TIMESTAMP:

            is_date = match.group(1).lower() == "current_date"
            logger.info(f"Detected static function '{match.group(1)}'")
            return functions.current_date() if is_date else functions.current_timestamp()

        else:

            df_col_match = ColumnExpression.DF_COL.match(column_expression)
            lit_col_match = ColumnExpression.LIT_COL.match(column_expression)
            function_name = df_col_match.group(1) if df_col_match else lit_col_match.group(1)
            function_argument = df_col_match.group(2) if df_col_match else lit_col_match.group(2)

            logger.info(f"Detected static function '{function_name}({function_argument})")

            return functions.col(function_argument) if df_col_match else functions.lit(function_argument)

    @staticmethod
    def parse_expression(column_expression: str) -> Column:

        all_column_expressions = [r for r in ColumnExpression]
        matching_column_expressions = list(filter(lambda x: x.match(column_expression), all_column_expressions))
        if len(matching_column_expressions) == 0:

            logger.error(f"Unable to match such column expression '{column_expression}'")
            raise ValueError(f"Unable to match column expression '{column_expression}'")

        else:

            matching_column_expression: ColumnExpression = matching_column_expressions[0]
            if matching_column_expression.is_static:

                logger.info(f"No further nested function detected")
                return ColumnExpressionParser._parse_static_expression(column_expression)

            else:

                matching_column_transformation = COLUMN_EXPRESSION_DICT[matching_column_expression](column_expression)
                logger.info(f"Matched expression with string representation '{matching_column_transformation.to_string}'")

                nested_function: str = matching_column_transformation.nested_function
                logger.info(f"Nested function detected ('{nested_function}'). Trying to resolve it recursively")
                return matching_column_transformation\
                    .transform(ColumnExpressionParser.parse_expression(nested_function))
