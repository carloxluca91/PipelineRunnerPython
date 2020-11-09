import logging
from abc import abstractmethod, ABC
from datetime import datetime, timedelta, date
from typing import Dict, List, Union, Tuple, Any

import numpy as np
from pyspark.sql import functions, Row, SparkSession

from pypeline.abstract import AbstractJsonElement
from utils.time import TimeUtils

T = Union[date, datetime, str, int]


class MetadataUtils:

    @staticmethod
    def get_or_else(dict_: Dict, key: str, default):

        return dict_.get(key, default) if dict_ else default


class BaseMetadata(AbstractJsonElement, ABC):

    def __init__(self):

        super().__init__()

        self._logger = logging.getLogger(__name__)
        self._rng = np.random.RandomState()


class AbstractMetadata(BaseMetadata, ABC):

    def __init__(self):

        super().__init__()

    @abstractmethod
    def create_data(self, number_of_records: int) -> List[T]:
        pass


class AbstractMetadataPlusSparkSession(BaseMetadata, ABC):

    def __init__(self):

        super().__init__()

    @abstractmethod
    def create_data(self, number_of_records: int, spark_session: SparkSession) -> List[T]:
        pass


class DateOrTimestampMetadata(AbstractMetadata):

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
        with MetadataUtils.get_or_else as get_or_else:

            self._java_output_format = get_or_else(as_string_info, "outputFormat", default_format)
            self._corrupt_flag = get_or_else(as_string_info, "corruptFlag", False)
            self._corrupt_probability = get_or_else(as_string_info, "corruptProb", 0)
            self._java_corrupt_format = get_or_else(as_string_info, "corruptFormat", TimeUtils.java_default_corrupt_format())

    def create_data(self, number_of_records: int) -> List[T]:

        is_date = self._is_date
        lower_bound = self._lower_bound_dtt
        time_delta = self._time_delta

        # Define a lambda expression depending on column type
        date_or_datetime_lambda = (lambda x: (lower_bound + time_delta * x).date()) if is_date else (lambda x: (lower_bound + time_delta * x))
        random_data: List[Union[date, datetime]] = list(map(date_or_datetime_lambda, self._rng.random_sample(number_of_records)))

        # If dates (or timestamps) must be converted to strings
        type_ = "date" if is_date else "timestamp"
        if self._as_string:

            java_output_format: str = self._java_output_format
            random_data: List[str] = list(map(lambda x: TimeUtils.format(x, java_output_format), random_data))

            # Check if data must be corrupted with wrong time formats
            corrupt_flag = self._corrupt_flag
            corrupt_prob = self._corrupt_probability
            java_corrupt_format = self._java_corrupt_format

            if corrupt_flag and corrupt_prob and java_corrupt_format:

                # If so, some dates (or timestamps), now as strings, must be converted back and forth in order to modify their time format
                self._logger.info(f"Corrupting {type_}(s) with prob {corrupt_prob}, alternative format = '{java_corrupt_format}')")

                corruption_probabilities = self._rng.choice([0, 1], number_of_records, p=[1 - corrupt_prob, corrupt_prob])

                # Function for modifying date (or timestamp) string format
                def corrupt_lambda(t: Tuple[str, int]) -> str:
                    dt_or_ts, prob = t[0], t[1]
                    return TimeUtils.format(TimeUtils.to_datetime(dt_or_ts, java_output_format), java_corrupt_format) if prob == 1 \
                        else dt_or_ts

                random_data: List[str] = list(map(corrupt_lambda, zip(random_data, corruption_probabilities)))
                self._logger.info(f"Returning data as list of strings with format {java_output_format}")

        else:

            self._logger.info(f"Returning data as a list of {type_}(s)")

        return random_data


class RandomColumnMetadata(AbstractMetadataPlusSparkSession):

    def __init__(self,
                 data_origin: str,
                 data_info: Dict):

        super().__init__()

        self._has_embedded_data = data_origin.lower() == "embedded"
        with MetadataUtils.get_or_else as get_or_else:

            self._embedded_values = get_or_else(data_info, "values", [])
            len_embedded_values = len(self._embedded_values)
            default_prob = 1 if len_embedded_values == 0 else [1/len_embedded_values] * len_embedded_values
            self._embedded_probs = get_or_else(data_info, "probs", default_prob)
            self._db_name = get_or_else(data_info, "dbName", None)
            self._table_name = get_or_else(data_info, "tableName", None)
            self._pipeline_name = get_or_else(data_info, "pipelineName", None)
            self._dataframe_id = get_or_else(data_info, "dataframeId", None)
            self._column_name = get_or_else(data_info, "columnName", None)

    def create_data(self, number_of_records: int, spark_session: SparkSession) -> List[Any]:

        values: List[Any]
        probs: List[Any]

        if self._has_embedded_data:

            # If values to be picked (and their probabilities) are embedded within column infos
            values = self._embedded_values
            probs = self._embedded_probs

        else:

            # Otherwise, retrieve values to be picked (and their probabilities) from Hive table
            pipeline_name_column = "pipeline_name"
            dataframe_id_column = "dataframe_id"
            column_name_column = "column_name"

            db_name: str = self._db_name
            table_name: str = self._table_name
            pipeline_name: str = self._pipeline_name
            dataframe_id: str = self._dataframe_id
            column_name: str = self._column_name

            related_to_info = f"({pipeline_name_column} = '{pipeline_name}', " \
                              f"{dataframe_id_column} = '{dataframe_id}', " \
                              f"{column_name_column} = '{column_name}')"

            full_table_name: str = f"{db_name}.{table_name}"
            self._logger.info(f"Trying to scan table '{full_table_name}' in order to detect some records related to {related_to_info}")

            values_and_probs: List[Row] = spark_session.table(full_table_name) \
                .filter((functions.col(pipeline_name_column) == pipeline_name) &
                        (functions.col(dataframe_id_column) == dataframe_id) &
                        (functions.col(column_name_column) == column_name)) \
                .select("value", "probability") \
                .collect()

            if len(values_and_probs) == 0:

                value_error_msg: str = f"Unable to detect records from table '{full_table_name}' related to {related_to_info}"
                raise ValueError(value_error_msg)

            self._logger.info(f"Identified {len(values_and_probs)} row(s) within table '{full_table_name}' related to {related_to_info}")

            values = [r["value"] for r in values_and_probs]
            probs = [r["probability"] for r in values_and_probs]

        return self._rng.choice(values, size=number_of_records, p=probs)
