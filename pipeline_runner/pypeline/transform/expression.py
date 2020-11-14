import logging
import re

from abc import abstractmethod, ABC
from datetime import datetime, date
from enum import Enum, unique
from functools import reduce
from typing import List, Union, Tuple, Callable, Any

from pyspark.sql import Column
from pyspark.sql import functions

from utils.spark import SparkUtils
from utils.time import TimeUtils


@unique
class ColumnExpression(Enum):

    AND_OR_OR = r"^(and)\((.+\))\)$", False, True
    BROUND = r"^(bround)\((.+\)), (\d+)\)$", False, False
    CAST = r"^(cast)\((\w+\(.*\)), '(\w+)'\)$", False, False
    COL = r"^(col)\('(\w+)'\)$", True, False
    COMPARE = r"^(equal|not_equal|gt|geq|lt|leq)\((.+)\)$", False, True
    CONCAT = r"^(concat)\((.+\))\)$", False, True
    CONCAT_WS = r"^(concat_ws)\((.+\)), '(.+)'\)$", False, True
    CURRENT_DATE_OR_TIMESTAMP = r"^(current_date|current_timestamp)\(\)$", True, False
    IS_NULL_OR_NOT = r"^(is_null|is_not_null)\((\w+\(.*\))\)$", False, False
    LEFT_OR_RIGHT_PAD = r"^([l|r]pad)\((\w+\(.*\)), (\d+), '(.+)'\)$", False, False
    LIT = r"^(lit)\(('?.+'?)\)$", True, False
    LOWER_OR_UPPER = r"^(lower|upper)\((\w+\(.*\))\)$", False, False
    REPLACE = r"^(replace)\((.+\)), '(.+)', '(.+)'\)$", False, False
    SUBSTRING = r"^(substring)\((\w+\(.*\)), (\d+), (\d+)\)$", False, False
    TO_DATE_OR_TIMESTAMP = r"^(to_date|to_timestamp)\((\w+\(.*\)), '(.+)'\)$", False, False
    TRIM = r"^(trim)\((\w+\(.*\))\)$", False, False

    def __init__(self, regex: str, is_static: bool, is_multi_column: bool):

        self._regex = regex
        self._is_static = is_static
        self._is_multi_column = is_multi_column

    @property
    def regex(self) -> str:
        return self._regex

    @property
    def is_static(self) -> bool:
        return self._is_static

    @property
    def is_multi_column(self) -> bool:
        return self._is_multi_column

    def match(self, column_expression: str):
        return re.match(self.regex, column_expression)


class AbstractExpression(ABC):

    def __init__(self, string: str, column_expression: ColumnExpression):

        self._logger = logging.getLogger(__name__)
        self._column_expression = column_expression
        self._match = re.match(column_expression.regex, string)
        self._function_name: str = self.group(1)

    def group(self, i: int):

        return None if self._match is None \
            else self._match.group(i)

    @property
    def is_static(self) -> bool:
        return self._column_expression.is_static

    @property
    def is_multi_column(self) -> bool:
        return self._column_expression.is_multi_column

    @property
    def function_name(self):
        return self._function_name

    @property
    @abstractmethod
    def to_string(self) -> str:
        pass


class StaticColumnExpression(AbstractExpression, ABC):

    def __init__(self, string: str, column_expression: ColumnExpression):

        super().__init__(string, column_expression)

    @abstractmethod
    def get_static_column(self) -> Column:
        pass


class SingleColumnExpression(AbstractExpression, ABC):

    def __init__(self, string: str, column_expression: ColumnExpression):

        super().__init__(string, column_expression)

        self._nested_function: str = self.group(2)

    @property
    def nested_function(self) -> str:
        return self._nested_function

    @abstractmethod
    def transform(self, input_column: Column) -> Column:
        pass


class MultipleColumnExpression(AbstractExpression, ABC):

    def __init__(self, string: str, column_expression: ColumnExpression):

        super().__init__(string, column_expression)

        self._multiple_expressions_str: str = self.group(2)

        def split_into_list_of_expressions(s: str) -> List[str]:

            expressions = []
            open_parenthesis = 0
            start_index = 0
            for i, c in enumerate(s, start=1):

                open_parenthesis = open_parenthesis + 1 if c == "(" else (open_parenthesis - 1 if c == ")" else open_parenthesis)
                if open_parenthesis == 0 and c == ")":
                    expressions.append(s[start_index: i])
                    start_index = i + 2

            return expressions

        self._sub_expressions: List[str] = split_into_list_of_expressions(self._multiple_expressions_str)

    @property
    def sub_expressions(self) -> List[str]:
        return self._sub_expressions

    @abstractmethod
    def combine(self, *columns: Column) -> Column:
        pass


class TwoColumnExpression(MultipleColumnExpression, ABC):

    def __init__(self, string: str, column_expression: ColumnExpression):

        super().__init__(string, column_expression)
        if len(self.sub_expressions) != 2:

            join_exprs: str = ", ".join(list(map(lambda x: f"'{x}'", self.sub_expressions)))
            raise ValueError(f"Expected 2 column expressions, found {len(self.sub_expressions)} ({join_exprs})")


class AndOrOrExpression(MultipleColumnExpression):

    def __init__(self, string: str):

        super().__init__(string, ColumnExpression.AND_OR_OR)

    @property
    def to_string(self) -> str:
        return f" {self.function_name.upper()} ".join(self.sub_expressions)

    def combine(self, *input_columns: Column) -> Column:

        is_and = self.function_name.lower() == "and"
        lambda_reduce_function = (lambda x, y: x & y) if is_and else (lambda x, y: x | y)
        return reduce(lambda_reduce_function, input_columns)


class BroundExpression(SingleColumnExpression):

    def __init__(self, string: str):

        super().__init__(string, ColumnExpression.BROUND)

        self._decimal_digits = int(self.group(3))

    @property
    def decimal_digits(self) -> int:
        return self._decimal_digits

    @property
    def to_string(self) -> str:
        return f"{self.function_name.upper()}({self.nested_function}, decimal_digits = '{self.decimal_digits}')"

    def transform(self, input_column: Column) -> Column:

        return functions.bround(input_column, self.decimal_digits)


class CastExpression(SingleColumnExpression):

    def __init__(self, string: str):

        super().__init__(string, ColumnExpression.CAST)

        self._casting_type: str = self.group(3)

    @property
    def casting_type(self) -> str:
        return self._casting_type

    @property
    def to_string(self) -> str:
        return f"{self.function_name.upper()}({self.nested_function}, '{self._casting_type}')"

    def transform(self, input_column: Column) -> Column:

        return input_column.cast(SparkUtils.get_spark_datatype(self.casting_type))


class ColExpression(StaticColumnExpression):

    def __init__(self, string: str):

        super().__init__(string, ColumnExpression.COL)
        self._column_name = self.group(2)

    @property
    def column_name(self) -> str:
        return self._column_name

    @property
    def to_string(self) -> str:
        return f"{self.function_name.upper()}({self.column_name})"

    def get_static_column(self) -> Column:
        return functions.col(self.column_name)


class CompareExpression(TwoColumnExpression):

    _comparator_dict = {

        "equal": ("equal", lambda x, y: x == y),
        "not_equal": ("not_equal", lambda x, y: x != y),
        "gt": ("greater_than", lambda x, y: x > y),
        "geq": ("greater_or_equal_than", lambda x, y: x >= y),
        "lt": ("less_than", lambda x, y: x < y),
        "leq": ("less_or_equal_than", lambda x, y: x <= y)
    }

    def __init__(self, string: str):

        super().__init__(string, ColumnExpression.COMPARE)

        self._comparator: Tuple[str, Callable[[Any, Any], Any]] = self._comparator_dict[self.function_name.lower()]

    @property
    def to_string(self) -> str:
        return f"{self.sub_expressions[0]} {self._comparator[0].upper()} {self.sub_expressions[1]}"

    def combine(self, *columns: Column) -> Column:

        lambda_function = self._comparator[1]
        return lambda_function(columns[0], columns[1])


class ConcatExpression(MultipleColumnExpression):

    def __init__(self, string: str):

        super().__init__(string, ColumnExpression.CONCAT)

    @property
    def to_string(self) -> str:
        return f" {self.function_name.upper()} ".join(self.sub_expressions)

    def combine(self, *columns: Column) -> Column:
        return functions.concat(*columns)


class ConcatWsExpression(MultipleColumnExpression):

    def __init__(self, string: str):

        super().__init__(string, ColumnExpression.CONCAT_WS)

        self._separator: str = self.group(3)

    @property
    def separator(self) -> str:
        return self._separator

    @property
    def to_string(self) -> str:
        return f" {self.function_name.upper()}({self.separator}) ".join(self.sub_expressions)

    def combine(self, *columns: Column) -> Column:
        return functions.concat_ws(self.separator, *columns)


class CurrentDateOrTimestampExpression(StaticColumnExpression):

    def __init__(self, string: str):

        super().__init__(string, ColumnExpression.CURRENT_DATE_OR_TIMESTAMP)

    @property
    def to_string(self) -> str:
        return f"{self.function_name.upper()}()"

    def get_static_column(self) -> Column:

        is_date = self.function_name.lower() == "current_date"
        return functions.current_date() if is_date else functions.current_timestamp()


class IsNullOrNotExpression(SingleColumnExpression):

    def __init__(self, string: str):

        super().__init__(string, ColumnExpression.IS_NULL_OR_NOT)

        self._is_null = self.function_name.lower() == "is_null"

    @property
    def to_string(self) -> str:
        return f"{self.function_name.upper()}({self.nested_function})"

    def transform(self, input_column: Column) -> Column:

        return input_column.isNull() if self._is_null else input_column.isNotNull


class LeftOrRightPadExpression(SingleColumnExpression):

    def __init__(self, string: str):

        super().__init__(string, ColumnExpression.LEFT_OR_RIGHT_PAD)

        self._padding_length: int = int(self.group(3))
        self._padding_str: str = self.group(4)

    @property
    def padding_length(self) -> int:
        return self._padding_length

    @property
    def padding_str(self) -> str:
        return self._padding_str

    @property
    def to_string(self) -> str:
        return f"{self.function_name.upper()}({self.nested_function}, padding_length = {self.padding_length}, padding_str = '{self.padding_str}')"

    def transform(self, input_column: Column) -> Column:

        padding_function = functions.lpad if self.function_name == "lpad" else functions.rpad
        return padding_function(input_column, len=self.padding_length, pad=self.padding_str)


class LitExpression(StaticColumnExpression):

    def __init__(self, string: str):

        super().__init__(string, ColumnExpression.LIT)

        self._lit_argument: str = self.group(2)

    @property
    def lit_argument(self) -> str:
        return self._lit_argument

    @property
    def to_string(self) -> str:
        return f"{self.function_name.upper()}({self.lit_argument})"

    def get_static_column(self) -> Column:

        lit_argument = self.lit_argument

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

        self._logger.info(f"Detected literal value '{literal_info_str}' of type {literal_type}")
        return functions.lit(literal_argument)


class LowerOrUpperExpression(SingleColumnExpression):

    def __init__(self, string: str):

        super().__init__(string, ColumnExpression.LOWER_OR_UPPER)

    @property
    def to_string(self) -> str:
        return f"{self.function_name.upper()}({self.nested_function})"

    def transform(self, input_column: Column) -> Column:

        str_function = functions.lower if self.function_name == "lower" else functions.upper
        return str_function(input_column)


class ReplaceExpression(SingleColumnExpression):

    def __init__(self, string: str):

        super().__init__(string, ColumnExpression.REPLACE)

        self._pattern = self.group(3)
        self._replacement = self.group(4)

    @property
    def pattern(self) -> str:
        return self._pattern

    @property
    def replacement(self) -> str:
        return self._replacement

    @property
    def to_string(self) -> str:
        return f"{self.function_name.upper()}({self.nested_function}, pattern = '{self.pattern}', replacement = '{self.replacement}')"

    def transform(self, input_column: Column) -> Column:

        return functions.regexp_replace(input_column, self.pattern, self._replacement)


class SubstringExpression(SingleColumnExpression):

    def __init__(self, string: str):

        super().__init__(string, ColumnExpression.SUBSTRING)

        self._substring_start_index: int = int(self.group(3))
        self._substring_length: int = int(self.group(4))

    @property
    def pos(self):
        return self._substring_start_index

    @property
    def length(self):
        return self._substring_length

    @property
    def to_string(self) -> str:
        return f"${self.function_name.upper()}({self.nested_function}, pos = '{self.pos}', length = '{self.length}')"

    def transform(self, input_column: Column) -> Column:

        return functions.substring(input_column, pos=self.pos, len=self.length)


class ToDateOrTimestampExpression(SingleColumnExpression):

    def __init__(self, string: str):

        super().__init__(string, ColumnExpression.TO_DATE_OR_TIMESTAMP)

        self._format = self.group(3)

    @property
    def format(self) -> str:
        return self._format

    @property
    def to_string(self) -> str:
        return f"{self.function_name.upper()}({self.nested_function}, format = '{self.format}')"

    def transform(self, input_column: Column) -> Column:

        time_format_function = functions.to_date if self.function_name == "to_date" else functions.to_timestamp
        return time_format_function(input_column, self.format)


class TrimExpression(SingleColumnExpression):

    def __init__(self, string: str):

        super().__init__(string, ColumnExpression.TRIM)

    @property
    def to_string(self) -> str:
        return f"{self.function_name.upper()}({self.nested_function})"

    def transform(self, input_column: Column) -> Column:

        return functions.trim(input_column)


COLUMN_EXPRESSION_DICT = {

    ColumnExpression.AND_OR_OR: AndOrOrExpression,
    ColumnExpression.CAST: CastExpression,
    ColumnExpression.COL: ColExpression,
    ColumnExpression.COMPARE: CompareExpression,
    ColumnExpression.CONCAT: ConcatExpression,
    ColumnExpression.CONCAT_WS: ConcatWsExpression,
    ColumnExpression.CURRENT_DATE_OR_TIMESTAMP: CurrentDateOrTimestampExpression,
    ColumnExpression.IS_NULL_OR_NOT: IsNullOrNotExpression,
    ColumnExpression.LEFT_OR_RIGHT_PAD: LeftOrRightPadExpression,
    ColumnExpression.LIT: LitExpression,
    ColumnExpression.LOWER_OR_UPPER: LowerOrUpperExpression,
    ColumnExpression.REPLACE: ReplaceExpression,
    ColumnExpression.SUBSTRING: SubstringExpression,
    ColumnExpression.TO_DATE_OR_TIMESTAMP: ToDateOrTimestampExpression,
    ColumnExpression.TRIM: TrimExpression

}
