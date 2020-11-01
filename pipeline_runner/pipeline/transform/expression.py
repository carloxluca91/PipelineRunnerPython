import logging
import re
from abc import abstractmethod, ABC
from enum import Enum, unique

from pyspark.sql import Column
from pyspark.sql import functions


@unique
class ColumnExpression(Enum):

    CURRENT_DATE_OR_TIMESTAMP = r"^(current_date|current_timestamp)\(\)$", True
    DF_COL = r"^(col)\('(\w+)'\)$", True
    LIT_COL = r"^(lit)\('([\w|\-|:]+)'\)$", True
    SUBSTRING = r"^(substring)\(([\w|\(|'|,|\)|\s]+\)),\s(\d+),\s(\d+)\)$", False
    TO_DATE_OR_TIMESTAMP = r"^(to_date|to_timestamp)\((.+\)), '(.+)'\)$", False

    def __init__(self, regex: str, is_static: bool):

        self._regex = regex
        self._is_static = is_static

    @property
    def regex(self) -> str:

        return self._regex

    @property
    def is_static(self) -> bool:

        return self._is_static

    def match(self, column_expression: str) -> re.Match:

        return re.match(self.regex, column_expression)


class AbstractColumnExpression(ABC):

    def __init__(self,
                 string: str,
                 column_expression_regex: ColumnExpression):

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


class SubstringExpression(AbstractColumnExpression):

    def __init__(self,
                 string: str):

        super().__init__(string, ColumnExpression.SUBSTRING)

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

        super().__init__(string, ColumnExpression.TO_DATE_OR_TIMESTAMP)

        self._format = self._group(3)

    @property
    def format(self) -> str:

        return self._format

    @property
    def to_string(self) -> str:

        return f"{self.function_name}({self.nested_function}, format = '{self.format}')"

    def transform(self, input_column: Column) -> Column:

        is_to_date = self.function_name.lower() == "to_date"
        return functions.to_date(input_column, self.format) if is_to_date else functions.to_timestamp(input_column, self.format)


COLUMN_EXPRESSION_DICT = {

    ColumnExpression.SUBSTRING: SubstringExpression,
    ColumnExpression.TO_DATE_OR_TIMESTAMP: ToDateOrTimestampExpression

}
