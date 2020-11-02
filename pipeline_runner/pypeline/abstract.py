from abc import ABC, abstractmethod

from utils.json import from_java_to_python_convention


class AbstractJsonElement(ABC):

    @abstractmethod
    def __init__(self, **kwars):
        pass

    @classmethod
    def from_dict(cls, dict_: dict):
        return cls(**from_java_to_python_convention(dict_))


class AbstractPipelineElement(AbstractJsonElement, ABC):

    def __init__(self,
                 name: str,
                 description: str):

        super().__init__()
        self._name = name
        self._description = description

    @property
    def name(self) -> str:
        return self._name

    @property
    def description(self) -> str:
        return self._description


class AbstractStep(AbstractPipelineElement, ABC):

    def __init__(self,
                 name: str,
                 description: str,
                 step_type: str,
                 dataframe_id: str):

        super().__init__(name, description)

        self._step_type = step_type
        self._dataframe_id = dataframe_id

    @property
    def step_type(self) -> str:
        return self._step_type

    @property
    def dataframe_id(self) -> str:
        return self._dataframe_id
