import logging
from typing import List, Dict, Any

import numpy as np
from pyspark.sql import SparkSession

from pypeline.abstract import AbstractPipelineElement
from pypeline.create.metadata import RandomColumnMetadata, DateOrTimestampMetadata


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
        self._metadata = metadata

    @property
    def column_type(self) -> str:
        return self._column_type

    @property
    def nullable(self) -> bool:
        return self._nullable

    @property
    def column_number(self) -> int:
        return self._column_number

    @property
    def is_date_or_timestamp_as_string(self) -> bool:

        return True if self.column_type in ["date", "timestamp"] and "asString" in self._metadata \
            else False

    def create(self, number_of_records: int, spark_session: SparkSession) -> List[Any]:

        if self.column_type in ["date", "timestamp"]:

            typed_metadata = DateOrTimestampMetadata.from_dict(self._metadata)
            random_data = typed_metadata.create_data(number_of_records)

        elif self.column_type == "rowId".lower():

            random_data = list(range(1, number_of_records + 1))

        else:

            typed_metadata = RandomColumnMetadata.from_dict(self._metadata)
            random_data = typed_metadata.create_data(number_of_records, spark_session)

        if self._nullable:

            nullable_probability = self._nullable_probability
            self._logger.info(f"Corrupting data of column '{self.name}' with (approximately) 1 None value every {1/nullable_probability} sample(s)")
            none_probabilities = self._rng.choice([0, 1], len(random_data), p=[1 - nullable_probability, nullable_probability])
            return [None if prob else datum for datum, prob in zip(random_data, none_probabilities)]

        else:

            return random_data
