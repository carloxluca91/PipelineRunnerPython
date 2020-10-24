from typing import List, Union


class TimestampColumnMetadata:

    def __init__(self,
                 lower_bound: str,
                 output_format: str,
                 upper_bound: str = None,
                 tamper: bool = False,
                 tamper_probability: float = None,
                 tamper_format: str = None):

        from datetime import datetime
        from pipeline_runner.time.format import to_datetime, DEFAULT_TIMESTAMP_FORMAT

        self.lower_bound_dtt = to_datetime(lower_bound, DEFAULT_TIMESTAMP_FORMAT)
        self.upper_bound_dtt = datetime.now() if upper_bound is None else to_datetime(upper_bound, DEFAULT_TIMESTAMP_FORMAT)
        self.output_format = output_format
        self.tamper = tamper
        self.tamper_probability = tamper_probability
        self.tamper_format = tamper_format

    @classmethod
    def from_dict(cls, input_dict: dict):
        return cls(**input_dict)

    @classmethod
    def from_json(cls, json_file_path: str):
        from pipeline_runner.specification.utils import load_json
        return cls(**load_json(json_file_path))


class RandomColumnMetadata:

    def __init__(self,
                 values: List[Union[str, float, int]],
                 p: List[float]):

        self.values = values
        self.p = p if p is not None else [1/len(values)] * len(values)

    @classmethod
    def from_dict(cls, input_dict: dict):
        return cls(**input_dict)

    @classmethod
    def from_json(cls, json_file_path: str):
        from pipeline_runner.specification.utils import load_json
        return cls(**load_json(json_file_path))


COLUMN_METADATA_DICT: dict = {

    "timestamp": TimestampColumnMetadata

}
