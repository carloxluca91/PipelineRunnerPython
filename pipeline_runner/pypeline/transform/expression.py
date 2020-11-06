import logging
import re
from abc import abstractmethod, ABC
from enum import Enum, unique

from pyspark.sql import Column
from pyspark.sql import functions


@unique
class ColumnExpressions(Enum):

    CURRENT_DATE_OR_TIMESTAMP = r"^(current_date|current_timestamp)\(\)$", True
    DF_COL = r"^(col)\('(\w+)'\)$", True
    LEFT_OR_RIGHT_PAD = r"^([l|r]pad)\((.+\)),\s(\d+),\s'(.+)'\)$", False
    LIT_COL = r"^(lit)\('(.+)'\)$", True
    LOWER_OR_UPPER = r"^(lower|upper)\((.+\))\)$", False
    SUBSTRING = r"^(substring)\((.+\)),\s(\d+),\s(\d+)\)$", False
    TO_DATE_OR_TIMESTAMP = r"^(to_date|to_timestamp)\((.+\)), '(.+)'\)$", False
    TRIM = r"^(trim)\((.+\))\)$", False

    def __init__(self, regex: str, is_static: bool):

        self._regex = regex
        self._is_static = is_static

    @property
    def regex(self) -> str:

        return self._regex

    @property
    def is_static(self) -> bool:

        return self._is_static

    def match(self, column_expression: str):

        return re.match(self.regex, column_expression)


class AbstractColumnExpression(ABC):

    def __init__(self,
                 string: str,
                 column_expression_regex: ColumnExpressions):

        self._logger = logging.getLogger(__name__)
        self._match = re.match(column_expression_regex.regex, string)

        self._function_name: str = self._group(1)
        self._nested_function: str = self._group(2)

    @property
    def function_name(self):
        return self._function_name

    @property
    def nested_function(self) -> str:
        return self._nested_function

    @property
    @abstractmethod
    def to_string(self) -> str:
        pass

    @abstractmethod
    def transform(self, input_column: Column) -> Column:
        pass

    def _group(self, i: int):

        return None if self._match is None \
            else self._match.group(i)


class LeftOrRightPadExpression(AbstractColumnExpression):

    def __init__(self, string: str):

        super().__init__(string, ColumnExpressions.LEFT_OR_RIGHT_PAD)

        self._padding_length: int = int(self._group(3))
        self._padding_str: str = self._group(4)

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

        self._substring_start_index: int = int(self._group(3))
        self._substring_length: int = int(self._group(4))

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

        self._format = self._group(3)

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

    ColumnExpressions.LEFT_OR_RIGHT_PAD: LeftOrRightPadExpression,
    ColumnExpressions.LOWER_OR_UPPER: LowerOrUpperExpression,
    ColumnExpressions.SUBSTRING: SubstringExpression,
    ColumnExpressions.TO_DATE_OR_TIMESTAMP: ToDateOrTimestampExpression,
    ColumnExpressions.TRIM: TrimExpression

}
