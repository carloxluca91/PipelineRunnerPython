import logging
from datetime import datetime, date
from typing import List, Union, Dict, Any, Tuple

import numpy as np
from pyspark.sql import SparkSession

from pypeline.abstract import AbstractPipelineElement
from pypeline.create.metadata import RandomColumnMetadata, DateOrTimestampMetadata
from utils.time import TimeUtils

T = Union[DateOrTimestampMetadata, RandomColumnMetadata]

_METADATA_TYPE = {

    "date": DateOrTimestampMetadata,
    "timestamp": DateOrTimestampMetadata
}


class TypedColumn(AbstractPipelineElement):

    def __init__(self,
                 name: str,
                 description: str,
                 column_type: str,
                 column_number: int,
                 metadata: Dict[str, str],
                 nullable: bool = True,
                 nullable_probability: float = 0.005):

        super().__init__(name, description)

        self._logger = logging.getLogger(__name__)
        self._rng = np.random.RandomState()

        self._column_type = column_type.lower()
        self._nullable = nullable
        self._nullable_probability = nullable_probability
        self._column_number = column_number
        self._typed_metadata: T = _METADATA_TYPE.get(self._column_type, RandomColumnMetadata).from_dict(metadata)

    @property
    def column_type(self) -> str:
        return self._column_type

    @property
    def nullable(self) -> bool:
        return self._nullable

    @property
    def column_number(self) -> int:
        return self._column_number

    def create(self, number_of_records: int, spark_session: SparkSession) -> List[Any]:

        typed_metadata = self._typed_metadata
        if isinstance(typed_metadata, DateOrTimestampMetadata):

            is_date = typed_metadata.is_date
            lower_bound = typed_metadata.lower_bound
            time_delta = typed_metadata.time_delta

            # Define a lambda expression depending on column type
            date_or_datetime_lambda = (lambda x: (lower_bound + time_delta * x).date()) if is_date else (lambda x: (lower_bound + time_delta * x))
            random_data: List[Union[date, datetime]] = list(map(date_or_datetime_lambda, self._rng.random_sample(number_of_records)))

            # If dates (or timestamps) must be converted to strings
            if typed_metadata.as_string:

                java_output_format: str = typed_metadata.java_output_format
                self._logger.info(f"Converting {self._column_type}s within column '{self.name}' to string with format '{java_output_format}'")
                random_data: List[str] = list(map(lambda x: TimeUtils.format(x, java_output_format), random_data))

                # Check if data have to be corrupted with wrong time formats
                corrupt_flag = typed_metadata.corrupt_flag
                corrupt_prob = typed_metadata.corrupt_prob
                java_corrupt_format = typed_metadata.java_corrupt_format

                if corrupt_flag and corrupt_prob and java_corrupt_format:

                    # If so, some dates (or timestamps), now as strings, must be converted back and forth in order to modify their string format
                    self._logger.info(f"Corrupting {self._column_type}s within column '{self.name}' "
                                      f"with prob {typed_metadata.corrupt_prob}, "
                                      f"format = '{typed_metadata.java_corrupt_format}')")

                    corruption_probabilities = self._rng.choice([0, 1], number_of_records, p=[1 - corrupt_prob, corrupt_prob])

                    # Function for modifying date (or timestamp) string format
                    def corrupt_lambda(t: Tuple[str, int]) -> str:

                        dt_or_ts, prob = t[0], t[1]
                        return TimeUtils.format(TimeUtils.to_datetime(dt_or_ts, java_output_format), java_corrupt_format) if prob == 1 \
                            else dt_or_ts

                    random_data: List[str] = list(map(corrupt_lambda, zip(random_data, corruption_probabilities)))

        else:

            # If values to be picked (and their probabilities) are embedded within column infos
            if typed_metadata.has_embedded_data:

                random_data = self._rng.choice(typed_metadata.embedded_values, size=number_of_records, p=typed_metadata.embedded_probs)

            else:

                # Retrieve values to be picked (and their probabilities) from Hive table
                # TODO
                random_data = []

        if self._nullable:

            nullable_probability = self._nullable_probability
            self._logger.info(f"Corrupting data of column '{self.name}' with some None values using probability = {nullable_probability}")
            none_probabilities = self._rng.choice([0, 1], len(random_data), p=[1 - nullable_probability, nullable_probability])
            return [original_datum if prob == 0 else None for original_datum, prob in zip(random_data, none_probabilities)]

        else:

            return random_data
