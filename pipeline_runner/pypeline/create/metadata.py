from datetime import date, datetime
from typing import List, Union

from pypeline.abstract import AbstractJsonElement
from utils.time import TimeUtils


class DateOrTimestampMetadata(AbstractJsonElement):

    def __init__(self,
                 is_date: bool,
                 lower_bound: str,
                 output_format: str,
                 upper_bound: str = None,
                 corrupt_flag: bool = False,
                 corrupt_probability: float = None,
                 corrupt_format: str = None):

        super().__init__()

        default_date_format = TimeUtils.default_date_format()
        default_timestamp_format = TimeUtils.default_timestamp_format()

        self.is_date = is_date
        self.lower_bound_dtt = TimeUtils.to_date(lower_bound, default_date_format) if is_date \
            else TimeUtils.to_datetime(lower_bound, default_timestamp_format)

        default_upper_bound: Union[date, datetime] = datetime.now().date() if is_date else datetime.now()
        self.upper_bound_dtt = default_upper_bound if upper_bound is None else \
            TimeUtils.to_date(upper_bound, default_date_format) if is_date \
                else TimeUtils.to_datetime(upper_bound, default_timestamp_format)

        self.java_output_format = output_format
        self.python_output_format = TimeUtils.to_python_format(output_format)

        self.corrupt_flag = corrupt_flag
        self.corrupt_probability = corrupt_probability
        self.java_corrupt_format = corrupt_format
        self.python_corrupt_format = TimeUtils.to_python_format(self.java_corrupt_format)


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


class RandomColumnMetadata(AbstractJsonElement):

    def __init__(self,
                 values: List[Union[str, float, int]],
                 p: List[float] = None):

        super().__init__()

        self.values = values
        self.p = [1/len(values)] * len(values) if p is None else p
