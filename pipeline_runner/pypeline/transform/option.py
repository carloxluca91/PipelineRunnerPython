from abc import ABC
from typing import List

from pypeline.abstract import AbstractJsonElement


class TransformationOptions(AbstractJsonElement, ABC):

    def __init__(self,
                 transformation_type: str,
                 input_source_id: str):

        super().__init__()

        self._transformation_type = transformation_type
        self._input_source_id = input_source_id

    @property
    def transformation_type(self) -> str:

        return self._transformation_type

    @property
    def input_source_id(self) -> str:

        return self._input_source_id


class WithColumnExpression(AbstractJsonElement):

    def __init__(self,
                 expression: str,
                 alias: str):

        super().__init__()

        self._expression = expression
        self._alias = alias

    @property
    def expression(self) -> str:

        return self._expression

    @property
    def alias(self) -> str:

        return self._alias


class WithColumnTransformationOptions(TransformationOptions):

    def __init__(self,
                 transformation_type: str,
                 input_source_id: str,
                 columns: List[dict]):

        super().__init__(transformation_type, input_source_id)

        self._columns = [WithColumnExpression.from_dict(d) for d in columns]

    @property
    def columns(self) -> List[WithColumnExpression]:

        return self._columns


class DropTransformationOptions(TransformationOptions):

    def __init__(self,
                 transformation_type: str,
                 input_source_id: str,
                 columns: List[str]):

        super().__init__(transformation_type, input_source_id)

        self._columns = columns

    @property
    def columns(self) -> List[str]:

        return self._columns


class SelectColumnExpression(WithColumnExpression):

    def __init__(self,
                 expression: str,
                 alias: str = None):

        super().__init__(expression, alias)


class SelectTransformationOptions(TransformationOptions):

    def __init__(self,
                 transformation_type: str,
                 input_source_id: str,
                 columns: List[dict]):

        super().__init__(transformation_type, input_source_id)

        self._columns = [SelectColumnExpression.from_dict(d) for d in columns]

    @property
    def columns(self):

        return self._columns
