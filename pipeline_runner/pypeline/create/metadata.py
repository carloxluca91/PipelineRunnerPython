from datetime import datetime, timedelta
from typing import Dict

from pypeline.abstract import AbstractJsonElement
from utils.time import TimeUtils


class DateOrTimestampMetadata(AbstractJsonElement):

    def __init__(self,
                 lower_bound: str,
                 upper_bound: str = None,
                 as_string: bool = False,
                 as_string_info: Dict = None):

        super().__init__()

        try:
            TimeUtils.to_date(lower_bound)
        except ValueError:
            is_date = False
        else:
            is_date = True

        self._is_date: bool = is_date
        default_format = TimeUtils.java_default_dt_format() if is_date else TimeUtils.java_default_ts_format()
        self._lower_bound_dtt: datetime = TimeUtils.to_datetime(lower_bound, default_format)
        self._upper_bound_dtt: datetime = datetime.now() if upper_bound is None else TimeUtils.to_datetime(upper_bound, default_format)
        self._time_delta: timedelta = self._upper_bound_dtt - self._lower_bound_dtt

        self._as_string = as_string

        def get_or_else(dict_: Dict, key: str, default):

            return dict_.get(key, default) if dict_ else default

        self._java_output_format = get_or_else(as_string_info, "outputFormat", default_format)
        self._corrupt_flag = get_or_else(as_string_info, "corruptFlag", False)
        self._corrupt_probability = get_or_else(as_string_info, "corruptProb", 0)
        self._java_corrupt_format = get_or_else(as_string_info, "corruptFormat", TimeUtils.java_default_corrupt_format())

    @property
    def lower_bound(self) -> datetime:
        return self._lower_bound_dtt

    @property
    def upper_bound(self) -> datetime:
        return self._upper_bound_dtt

    @property
    def time_delta(self) -> timedelta:
        return self._time_delta

    @property
    def is_date(self) -> bool:
        return self._is_date

    @property
    def as_string(self) -> bool:
        return self._as_string

    @property
    def java_output_format(self) -> str:
        return self._java_output_format

    @property
    def corrupt_flag(self) -> bool:
        return self._corrupt_flag

    @property
    def corrupt_prob(self) -> float:
        return self._corrupt_probability

    @property
    def java_corrupt_format(self) -> str:
        return self._java_corrupt_format


class RandomColumnMetadata(AbstractJsonElement):

    def __init__(self,
                 data_origin: str,
                 data_info: Dict):

        super().__init__()

        self._has_embedded_data = data_origin.lower() == "embedded"

        def get_or_else(dict_: Dict, key: str, default):

            return dict_.get(key, default) if dict_ else default

        self._embedded_values = get_or_else(data_info, "values", [])
        len_embedded_values = len(self._embedded_values)
        default_prob = 1 if len_embedded_values == 0 else [1/len_embedded_values] * len_embedded_values
        self._embedded_probs = get_or_else(data_info, "probs", default_prob)

    @property
    def has_embedded_data(self) -> bool:
        return self._has_embedded_data

    @property
    def embedded_values(self):
        return self._embedded_values

    @property
    def embedded_probs(self):
        return self._embedded_probs
