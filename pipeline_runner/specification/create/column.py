import logging
from abc import abstractmethod, ABC
from typing import List, Iterable

import numpy as np

from pipeline_runner.specification.abstract import AbstractPipelineElement
from pipeline_runner.specification.metadata import TimestampColumnMetadata, RandomColumnMetadata

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

        self.column_type = column_type
        self.nullable = nullable
        self.nullable_probability = nullable_probability
        self.column_number = column_number

    def _maybe_some_to_none(self, original_data):

        if self.nullable:

            nullable_prob: float = self.nullable_probability
            none_probabilities: Iterable[int] = self._rng.choice([0, 1], len(original_data), p=[1 - nullable_prob, nullable_prob])
            return [original_datum if p == 0 else None for original_datum, p in zip(original_data, none_probabilities)]

        else:
            return original_data

    @abstractmethod
    def create(self, number_of_records: int):
        pass


class TimestampColumn(TypedColumn):

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

        self._metadata = TimestampColumnMetadata.from_dict(metadata)

    def create(self, number_of_records: int) -> List[str]:

        from datetime import datetime, timedelta
        from pipeline_runner.time.format import JAVA_TO_PYTHON_FORMAT

        lower_bound_dtt: datetime = self._metadata.lower_bound_dtt
        upper_bound_dtt: datetime = self._metadata.upper_bound_dtt
        time_delta: timedelta = upper_bound_dtt - lower_bound_dtt
        python_output_format: str = JAVA_TO_PYTHON_FORMAT[self._metadata.output_format]

        tamper_flag: bool = self._metadata.tamper
        tamper_p: float = self._metadata.tamper_probability
        tamper_dt_format: str = self._metadata.tamper_format

        random_numbers = self._rng.random_sample(number_of_records)
        random_dts: List[str]
        if tamper_flag and tamper_p and tamper_dt_format:

            python_tamper_format: str = JAVA_TO_PYTHON_FORMAT[tamper_dt_format]
            self._logger.info(f"Tampering operation details (probability = {tamper_p}, format = '{tamper_dt_format}')")
            tamper_probs = self._rng.choice([0, 1], number_of_records, p=[1 - tamper_p, tamper_p])
            random_dts = [(lower_bound_dtt + (time_delta * rn)).strftime(python_output_format) if p == 0
                          else (lower_bound_dtt + (time_delta * rn)).strftime(python_tamper_format)
                          for rn, p in zip(random_numbers, tamper_probs)]

        else:

            random_dts = [(lower_bound_dtt + (time_delta * rn)).strftime(python_output_format)
                          for rn in self._rng.random_sample(number_of_records)]

        return self._maybe_some_to_none(random_dts)


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

    def create(self, number_of_records: int):

        return self._maybe_some_to_none([self._rng.choice(self._metadata.values,
                                                          size=number_of_records,
                                                          p=self._metadata.p)])
