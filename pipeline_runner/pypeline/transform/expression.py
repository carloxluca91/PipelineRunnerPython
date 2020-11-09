import logging
import re
from abc import abstractmethod, ABC
from enum import Enum, unique

from pyspark.sql import Column
from pyspark.sql import functions

from pypeline.transform.parser import ColumnExpressionParser
from utils.spark import SparkUtils


@unique
class ColumnExpressions(Enum):

    CAST = r"^(cast)\((\w+\(.*\)), '(\w+)'\)$", False, False
    CURRENT_DATE_OR_TIMESTAMP = r"^(current_date|current_timestamp)\(\)$", True, False
    COL = r"^(col)\('(\w+)'\)$", True, False
    EQUAL_OR_NOT = r"^(equal|not_equal)\((\w+\(.*\)), (\w+\(.*\))\)$", False, True
    IS_NULL_OR_NOT = r"^(is_null|is_not_null)\((\w+\(.*\))\)$", False, False
    LEFT_OR_RIGHT_PAD = r"^([l|r]pad)\((\w+\(.*\)), (\d+), '(.+)'\)$", False, False
    LIT = r"^(lit)\(('?.+'?)\)$", True, False
    LOWER_OR_UPPER = r"^(lower|upper)\((\w+\(.*\)))\)$", False, False
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


class AbstractColumnExpression(ABC):

    def __init__(self,
                 string: str,
                 column_expression_regex: ColumnExpressions):

        self._logger = logging.getLogger(__name__)
        self._match = re.match(column_expression_regex.regex, string)

        self._function_name: str = self.group(1)
        self._nested_function: str = self.group(2)

    @property
    def function_name(self):
        return self._function_name

    @property
    def nested_function(self) -> str:
        return self._nested_function

    def group(self, i: int):

        return None if self._match is None \
            else self._match.group(i)

    @property
    @abstractmethod
    def to_string(self) -> str:
        pass

    @abstractmethod
    def transform(self, input_column: Column) -> Column:
        pass


class CastExpression(AbstractColumnExpression):

    def __init__(self, string: str):

        super().__init__(string, ColumnExpressions.CAST)

        self._casting_type: str = self.group(3)

    @property
    def casting_type(self) -> str:
        return self._casting_type

    @property
    def to_string(self) -> str:
        return f"{self.nested_function}.cast('{self._casting_type}')"

    def transform(self, input_column: Column) -> Column:

        return input_column.cast(SparkUtils.get_spark_datatype(self.casting_type))


class EqualOrNotExpression(AbstractColumnExpression):

    def __init__(self, string: str):

        super().__init__(string, ColumnExpressions.EQUAL_OR_NOT)

        self._is_equal = self.function_name == "equal"
        self._comparison_obj: str = self.group(3)
        self._comparison_column: Column = ColumnExpressionParser.parse_expression(self._comparison_obj)

    @property
    def to_string(self) -> str:
        return f"{self.nested_function} {'=' if self._is_equal else '<>'} {self._comparison_obj})"

    # noinspection PyTypeChecker
    def transform(self, input_column: Column) -> Column:

        return (input_column == self._comparison_column) if self._is_equal else (input_column != self._comparison_column)


class IsNullOrNotExpression(AbstractColumnExpression):

    def __init__(self, string: str):

        super().__init__(string, ColumnExpressions.IS_NULL_OR_NOT)

        self._is_null = self.function_name.lower() == "is_null"

    @property
    def to_string(self) -> str:
        return f"{self.nested_function}.{'isNull' if self._is_null else 'isNotNull'}()"

    def transform(self, input_column: Column) -> Column:

        return input_column.isNull() if self._is_null else input_column.isNotNull


class LeftOrRightPadExpression(AbstractColumnExpression):

    def __init__(self, string: str):

        super().__init__(string, ColumnExpressions.LEFT_OR_RIGHT_PAD)

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
        return f"{self.function_name}({self.nested_function}, padding_length = {self.padding_length}, padding_str = '{self.padding_str}')"

    def transform(self, input_column: Column) -> Column:

        padding_function = functions.lpad if self.function_name == "lpad" else functions.rpad
        return padding_function(input_column, len=self.padding_length, pad=self.padding_str)


class LowerOrUpperExpression(AbstractColumnExpression):

    def __init__(self, string: str):

        super().__init__(string, ColumnExpressions.LOWER_OR_UPPER)

    @property
    def to_string(self) -> str:
        return f"{self.function_name}({self.nested_function})"

    def transform(self, input_column: Column) -> Column:

        str_function = functions.lower if self.function_name == "lower" else functions.upper
        return str_function(input_column)


class SubstringExpression(AbstractColumnExpression):

    def __init__(self, string: str):

        super().__init__(string, ColumnExpressions.SUBSTRING)

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
        return f"${self.function_name}({self.nested_function}, pos = '{self.pos}', length = '{self.length}')"

    def transform(self, input_column: Column) -> Column:

        return functions.substring(input_column, pos=self.pos, len=self.length)


class ToDateOrTimestampExpression(AbstractColumnExpression):

    def __init__(self, string: str):

        super().__init__(string, ColumnExpressions.TO_DATE_OR_TIMESTAMP)

        self._format = self.group(3)

    @property
    def format(self) -> str:
        return self._format

    @property
    def to_string(self) -> str:
        return f"{self.function_name}({self.nested_function}, format = '{self.format}')"

    def transform(self, input_column: Column) -> Column:

        time_format_function = functions.to_date if self.function_name == "to_date" else functions.to_timestamp
        return time_format_function(input_column, self.format)


class TrimExpression(AbstractColumnExpression):

    def __init__(self, string: str):

        super().__init__(string, ColumnExpressions.TRIM)

    @property
    def to_string(self) -> str:
        return f"{self.function_name}({self.nested_function})"

    def transform(self, input_column: Column) -> Column:

        return functions.trim(input_column)


COLUMN_EXPRESSION_DICT = {

    ColumnExpressions.CAST: CastExpression,
    ColumnExpressions.EQUAL_OR_NOT: EqualOrNotExpression,
    ColumnExpressions.IS_NULL_OR_NOT: IsNullOrNotExpression,
    ColumnExpressions.LEFT_OR_RIGHT_PAD: LeftOrRightPadExpression,
    ColumnExpressions.LOWER_OR_UPPER: LowerOrUpperExpression,
    ColumnExpressions.SUBSTRING: SubstringExpression,
    ColumnExpressions.TO_DATE_OR_TIMESTAMP: ToDateOrTimestampExpression,
    ColumnExpressions.TRIM: TrimExpression

}
