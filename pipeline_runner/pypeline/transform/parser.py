import logging
from typing import List

from pyspark.sql import Column

from pypeline.transform.expression import COLUMN_EXPRESSION_DICT, ColumnExpression


class ColumnExpressionParser:

    _logger = logging.getLogger(__name__)

    @classmethod
    def parse_expression(cls, expression: str) -> Column:

        # Filter list of defined ColumnExpression
        matching_expressions = list(filter(lambda x: x.match(expression), [r for r in ColumnExpression]))
        if len(matching_expressions) == 0:

            value_error_msg: str = f"Unable to match column expression '{expression}'. " \
                                   f"Suggest to look for the nearest regex within {ColumnExpression.__name__} " \
                                   f"and compare it with the unmatching expression using https://regex101.com/ ;)"
            raise ValueError(value_error_msg)

        else:

            # If one matches the provided column_expression, get it and check its type in order to understand how to proceed
            matching_expression = COLUMN_EXPRESSION_DICT[matching_expressions[0]](expression)
            to_string_value: str = matching_expression.to_string

            # If it static
            if matching_expression.is_static:

                cls._logger.info(f"Detected a static expression: '{to_string_value}'")
                return matching_expression.get_static_column()

            # If it is multicolumn
            elif matching_expression.is_multi_column:

                cls._logger.info(f"Detected a multi-column expression: '{to_string_value}'")
                input_columns: List[Column] = []
                for index, sub_expression in enumerate(matching_expression.sub_expressions, start=1):

                    input_column: Column = ColumnExpressionParser.parse_expression(sub_expression)
                    input_columns.append(input_column)
                    cls._logger.info(f"Successfully parsed subexpression # {index} ('{sub_expression}')")

                cls._logger.info(f"Successfully parsed each of the {len(matching_expression.sub_expressions)} subexpression(s)")
                return matching_expression.combine(*input_columns)

            # Otherwise, as a single column transformation
            else:

                cls._logger.info(f"Detected a standard single-column expression: '{to_string_value}'")
                cls._logger.info(f"Detected a nested function: '{matching_expression.nested_function}'. Trying to resolve it recursively")
                nested_function: str = matching_expression.nested_function
                return matching_expression\
                    .combine(ColumnExpressionParser.parse_expression(nested_function))
