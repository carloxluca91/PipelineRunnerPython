from abc import ABC
from typing import List, Union

from pipeline_runner.utils.time import JAVA_PYTHON_DT_FORMAT_CONVERTER, JAVA_PYTHON_TS_FORMAT_CONVERTER


class DateOrTimestampMetadata(ABC):

    def __init__(self,
                 is_date: bool,
                 lower_bound: str,
                 output_format: str,
                 upper_bound: str = None,
                 corrupt_flag: bool = False,
                 corrupt_probability: float = None,
                 corrupt_format: str = None):

        from datetime import date, datetime
        from pipeline_runner.utils.time import to_datetime, to_date
        from pipeline_runner.utils.time import DEFAULT_TIMESTAMP_FORMAT, DEFAULT_DATE_FORMAT

        self.is_date = is_date
        self.lower_bound_dtt = to_date(lower_bound, DEFAULT_DATE_FORMAT) if is_date \
            else to_datetime(lower_bound, DEFAULT_TIMESTAMP_FORMAT)

        default_upper_bound: Union[date, datetime] = datetime.now().date() if is_date else datetime.now()
        self.upper_bound_dtt = default_upper_bound if upper_bound is None else \
            to_date(upper_bound, DEFAULT_DATE_FORMAT) if is_date \
                else to_datetime(upper_bound, DEFAULT_TIMESTAMP_FORMAT)

        self.java_output_format = output_format
        self.python_output_format = JAVA_PYTHON_DT_FORMAT_CONVERTER[output_format] if is_date \
            else JAVA_PYTHON_TS_FORMAT_CONVERTER[output_format]

        self.corrupt_flag = corrupt_flag
        self.corrupt_probability = corrupt_probability
        self.java_corrupt_format = corrupt_format
        self.python_corrupt_format = corrupt_format if corrupt_format is None \
            else JAVA_PYTHON_DT_FORMAT_CONVERTER[corrupt_format] if is_date \
            else JAVA_PYTHON_TS_FORMAT_CONVERTER[corrupt_format]


class DateColumnMetadata(DateOrTimestampMetadata):

    def __init__(self,
                 lower_bound: str,
                 output_format: str,
                 upper_bound: str = None,
                 corrupt_flag: bool = False,
                 corrupt_probability: float = None,
                 corrupt_format: str = None,
                 is_date: bool = True):

        super().__init__(is_date,
                         lower_bound,
                         output_format,
                         upper_bound,
                         corrupt_flag,
                         corrupt_probability,
                         corrupt_format)

    @classmethod
    def from_dict(cls, input_dict: dict):
        return cls(**input_dict)

    @classmethod
    def from_json(cls, json_file_path: str):
        from pipeline_runner.specification.utils import load_json
        return cls(**load_json(json_file_path))


class TimestampColumnMetadata(DateOrTimestampMetadata):

    def __init__(self,
                 lower_bound: str,
                 output_format: str,
                 upper_bound: str = None,
                 corrupt_flag: bool = False,
                 corrupt_probability: float = None,
                 corrupt_format: str = None,
                 is_date: bool = False):

        super().__init__(is_date,
                         lower_bound,
                         output_format,
                         upper_bound,
                         corrupt_flag,
                         corrupt_probability,
                         corrupt_format)

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
                 p: List[float] = None):

        self.values = values
        self.p = [1/len(values)] * len(values) if p is None else p

    @classmethod
    def from_dict(cls, input_dict: dict):
        return cls(**input_dict)

    @classmethod
    def from_json(cls, json_file_path: str):
        from pipeline_runner.specification.utils import load_json
        return cls(**load_json(json_file_path))
