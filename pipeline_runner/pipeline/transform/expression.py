import logging
import re
from abc import abstractmethod, ABC
from enum import Enum, unique
from functools import partial
from typing import Callable

from pyspark.sql import Column
from pyspark.sql import functions


@unique
class ColumnExpressionRegex(Enum):

    DF_COL = r"^(col)\('(\w+)'\)$"
    LIT_COL = r"^(lit)\('([\w|\-|:]+)'\)$"
    SUBSTRING = r"^(substring)\(([\w|\(|'|,|\)|\s]+\)),\s(\d+),\s(\d+)\)$"

    def __init__(self, regex_pattern: str):

        self.regex_pattern = regex_pattern


class AbstractColumnTransformation(ABC):

    def __init__(self,
                 string: str,
                 column_expression_regex: ColumnExpressionRegex):

        self._logger = logging.getLogger(__name__)
        self._regex_match = re.search(column_expression_regex.regex_pattern, string)

        self._function_name: str = self._get_group(1)
        self._nested_function_str: str = self._get_group(2)

    @property
    def function_name(self):
        return self._function_name

    @property
    def nested_function_str(self) -> str:
        return self._nested_function_str

    @abstractmethod
    def _transformation_function(self) -> Callable[[Column], Column]:
        pass

    @abstractmethod
    def _transformation_description(self) -> str:
        pass

    def transform(self, input_column: Column) -> Column:

        self._logger.info(self._transformation_description)
        return self._transformation_function()(input_column)

    def _get_group(self, i: int):

        return None if self._regex_match is None \
            else self._regex_match.group(i)


class SubstringTransformation(AbstractColumnTransformation):

    def __init__(self,
                 string: str):

        super().__init__(string, ColumnExpressionRegex.SUBSTRING)

        self._substring_start_index: int = int(self._regex_match.group(3))
        self._substring_length: int = int(self._regex_match.group(4))

    @property
    def pos(self):

        return self._substring_start_index

    @property
    def length(self):

        return self._substring_length

    def _transformation_description(self) -> str:

        return f"substring({self.nested_function_str}, " \
               f"start_index = '{self._substring_start_index}', " \
               f"length = '{self._substring_length}')"

    def _transformation_function(self) -> Callable[[Column], Column]:

        return partial(functions.substring,
                       self._substring_start_index,
                       self._substring_length).func


COLUMN_EXPRESSION_DICT = {

    ColumnExpressionRegex.SUBSTRING: SubstringTransformation

}
