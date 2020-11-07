import logging
import numpy as np

from abc import abstractmethod, ABC
from typing import List, Iterable, Union

from pypeline.abstract import AbstractPipelineElement
from pypeline.create.metadata import DateColumnMetadata, RandomColumnMetadata, TimestampColumnMetadata

_DEFAULT_NULLABLE = True
_DEFAULT_NULLABLE_PROBABILITY = 0.005


class TypedColumn(AbstractPipelineElement, ABC):

    def __init__(self,
                 name: str,
                 description: str,
                 column_type: str,
                 column_number: int,
                 nullable: bool = _DEFAULT_NULLABLE,
                 nullable_probability: float = _DEFAULT_NULLABLE_PROBABILITY):

        super().__init__(name, description)

        self._logger = logging.getLogger(__name__)
        self._rng = np.random.RandomState()

        self._column_type = column_type
        self._nullable = nullable
        self._nullable_probability = nullable_probability
        self._column_number = column_number

    @property
    def column_type(self) -> str:
        return self._column_type

    @property
    def nullable(self) -> bool:
        return self._nullable

    @property
    def nullable_probability(self) -> float:
        return self._nullable_probability

    @property
    def column_number(self) -> int:
        return self._column_number

    def _corrupt_with_none(self, original_data):

        nullable_probability: float = self._nullable_probability
        self._logger.info(f"Corrupting original data of column '{self.name}' using probability = {nullable_probability}")
        none_probabilities: Iterable[int] = self._rng.choice([0, 1], len(original_data), p=[1 - nullable_probability, nullable_probability])
        return [original_datum if prob == 0 else None for original_datum, prob in zip(original_data, none_probabilities)]

    @abstractmethod
    def create(self, number_of_records: int):
        pass


class DateOrTimestampColumn(TypedColumn):

    def __init__(self,
                 name: str,
                 description: str,
                 column_type: str,
                 column_number: int,
                 metadata: dict,
                 nullable: bool = _DEFAULT_NULLABLE,
                 nullable_probability: float = _DEFAULT_NULLABLE_PROBABILITY):

        super().__init__(name,
                         description,
                         column_type,
                         column_number,
                         nullable,
                         nullable_probability)

        self._metadata = TimestampColumnMetadata.from_dict(metadata) if column_type.lower() == "timestamp" \
            else DateColumnMetadata.from_dict(metadata)

    @property
    def metadata(self):
        return self._metadata

    def create(self, number_of_records: int) -> List[str]:

        from datetime import date, datetime, timedelta

        # CONVERT BOTH lower_bound AND upper_bound AS datetime.datetime OBJECTS AND COMPUTE RELATED TIMEDELTA
        lower_bound_dtt: Union[date, datetime] = self._metadata.lower_bound_dtt
        upper_bound_dtt: Union[date, datetime] = self._metadata.upper_bound_dtt
        time_delta: timedelta = upper_bound_dtt - lower_bound_dtt

        # EXTRACT INFORMATION ABOUT DATA CORRUPTION
        data_corruption_flag: bool = self._metadata.corrupt_flag
        data_corruption_prob: float = self._metadata.corrupt_probability
        java_corrupting_format: str = self._metadata.java_corrupt_format
        python_corrupting_format: str = self._metadata.python_corrupt_format

        python_output_format: str = self._metadata.python_output_format
        random_samples = self._rng.random_sample(number_of_records)
        random_ts_or_dates: List[str] = [(lower_bound_dtt + (time_delta * rn)).strftime(python_output_format) for rn in random_samples]

        # CORRUPT DATA, IF NECESSARY
        if data_corruption_flag and data_corruption_prob and python_corrupting_format:

            self._logger.info(f"Corrupting operation details (probability = {data_corruption_prob}, format = '{java_corrupting_format}')")
            corruption_probabilities = self._rng.choice([0, 1], number_of_records, p=[1 - data_corruption_prob, data_corruption_prob])
            random_ts_or_dates = [datetime.strptime(random_ts_or_date, python_output_format).strftime(python_corrupting_format) if p == 1
                                  else random_ts_or_date
                                  for random_ts_or_date, p in zip(random_ts_or_dates, corruption_probabilities)]

        return self._corrupt_with_none(random_ts_or_dates) if self.nullable else random_ts_or_dates


class RandomColumn(TypedColumn):

    def __init__(self,
                 name: str,
                 description: str,
                 column_type: str,
                 column_number: int,
                 metadata: dict,
                 nullable: bool = _DEFAULT_NULLABLE,
                 nullable_probability: float = _DEFAULT_NULLABLE_PROBABILITY):

        super().__init__(name,
                         description,
                         column_type,
                         column_number,
                         nullable,
                         nullable_probability)

        self._metadata = RandomColumnMetadata.from_dict(metadata)

    @property
    def metadata(self):
        return self._metadata

    def create(self, number_of_records: int):

        metadata = self._metadata
        values_length: int = len(metadata.values)
        p_length: int = len(metadata.p)
        p_sum: float = sum(metadata.p)

        if (values_length == p_length) and p_sum == 1:

            original_data = self._rng.choice(metadata.values, size=number_of_records, p=metadata.p)
            return self._corrupt_with_none(original_data) if self.nullable else original_data

        else:

            raise ValueError(f"The number of provided values ({values_length}) does not match "
                             f"the number of provided probabilities ({p_length}), "
                             f"or the probailities do not sum up to 1 ({p_sum})")
