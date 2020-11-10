import logging
import re
from datetime import date, datetime
from typing import Union, List

from pyspark.sql import Column
from pyspark.sql import functions

from pypeline.transform.expression import COLUMN_EXPRESSION_DICT, ColumnExpressions
from utils.time import TimeUtils


class ColumnExpressionParser:

    _logger = logging.getLogger(__name__)

    @classmethod
    def _parse_static_expression(cls, column_expression: str) -> Column:

        logger = cls._logger

        matching_static_expressions = list(filter(lambda x: x.is_static and x.match(column_expression), ColumnExpressions))
        matching_static_expression = matching_static_expressions[0]
        regex_match = matching_static_expression.match(column_expression)
        if matching_static_expression == ColumnExpressions.CURRENT_DATE_OR_TIMESTAMP:

            is_date = regex_match.group(1).lower() == "current_date"
            logger.info(f"Detected static function '{regex_match.group(1)}()'")
            return functions.current_date() if is_date else functions.current_timestamp()

        else:

            lit_match = ColumnExpressions.LIT.match(column_expression)
            if lit_match:

                # Inspect the literal value in order to detect its type
                lit_argument = lit_match.group(2)
                if lit_argument.startswith("'") and lit_argument.endswith("'"):

                    # If the literal value is in quotes, it can be a date, a timestamp or a string
                    lit_argument_without_quotes: str = lit_argument[1: - 1]
                    is_date = TimeUtils.has_date_format(lit_argument_without_quotes)
                    is_timestamp = TimeUtils.has_datetime_format(lit_argument_without_quotes)

                    literal_info_str = lit_argument_without_quotes
                    literal_type: str = "date" if is_date else ("timestamp" if is_timestamp else "string")
                    literal_argument: Union[date, datetime, str] = TimeUtils.to_date(lit_argument_without_quotes) if is_date else \
                        (TimeUtils.to_datetime(lit_argument_without_quotes) if is_timestamp else
                         lit_argument_without_quotes)

                else:

                    # Otherwise, the literal value can be a double or a integer
                    is_double = re.match(r"^\d+\.\d+$", lit_argument)
                    literal_type = "double" if is_double else "int"
                    literal_info_str = lit_argument
                    literal_argument: float = float(lit_argument) if is_double else int(lit_argument)

                logger.info(f"Detected literal value '{literal_info_str}' of type {literal_type}")
                return functions.lit(literal_argument)

            else:

                col_match = ColumnExpressions.COL.match(column_expression)
                column_name = col_match.group(2)
                logger.info(f"Detected dataframe column '{column_name}'")
                return functions.col(column_name)

    @classmethod
    def parse_expression(cls, column_expression: str) -> Column:

        logger = cls._logger

        # Filter list of defined ColumnExpression
        matching_column_expressions = list(filter(lambda x: x.match(column_expression), [r for r in ColumnExpressions]))
        if len(matching_column_expressions) == 0:

            value_error_msg: str = f"Unable to match column expression '{column_expression}'. " \
                                   f"Suggest to look for the nearest regex within {ColumnExpressions.__name__} " \
                                   f"and compare it with the unmatching expression using https://regex101.com/ ;)"
            raise ValueError(value_error_msg)

        else:

            # If one matches the provided column_expression, check wheter it is static or not
            matching_column_expression: ColumnExpressions = matching_column_expressions[0]
            if matching_column_expression.is_static:

                logger.info(f"Detected a static expression (i.e. an expression that does not depend on other columns)")
                return cls._parse_static_expression(column_expression)

            else:

                matching_column_transformation = COLUMN_EXPRESSION_DICT[matching_column_expression](column_expression)
                logger.info(f"Matched non-static expression with string representation '{matching_column_transformation.to_string}'")
                if matching_column_expression.is_multi_column:

                    input_columns: List[Column] = []
                    for index, expression in matching_column_transformation.expressions:

                        input_column: Column = ColumnExpressionParser.parse_expression(expression)
                        input_columns.append(input_column)
                        logger.info(f"Successfully parsed input expression # {index} ('{expression}')")

                    logger.info(f"Successfully parsed each of the {len(matching_column_transformation.expressions)} input expression")
                    return matching_column_transformation.combine(*input_columns)

                else:

                    nested_function: str = matching_column_transformation.nested_function
                    logger.info(f"Nested function detected ('{nested_function}'). Trying to resolve it recursively")
                    return matching_column_transformation\
                        .combine(ColumnExpressionParser.parse_expression(nested_function))
