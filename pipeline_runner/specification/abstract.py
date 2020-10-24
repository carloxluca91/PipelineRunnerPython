from abc import ABC


class AbstractPipelineElement(ABC):

    def __init__(self,
                 name: str,
                 description: str):

        self._name = name
        self._description = description

    @property
    def name(self) -> str:
        return self._name

    @property
    def description(self) -> str:
        return self._description

    @classmethod
    def from_dict(cls, input_dict: dict):

        return cls(**input_dict)

    @classmethod
    def from_json(cls, json_file_path: str):

        from pipeline_runner.specification.utils import load_json
        return cls(**load_json(json_file_path))


class AbstractPipelineStep(AbstractPipelineElement, ABC):

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
