from abc import ABC


class AbstractPipelineElement(ABC):

    def __init__(self,
                 name: str,
                 description: str):

        self.name = name
        self.description = description

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

        self.step_type = step_type
        self.dataframe_id = dataframe_id
