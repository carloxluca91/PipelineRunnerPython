import logging
from abc import abstractmethod
from typing import Union, Callable, Tuple

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.types import StructType

from pipeline_runner.specification.abstract import AbstractPipelineStep
from pipeline_runner.specification.read.options import ReadCsvOptions, ReadParquetOptions


class AbstractReadStage(AbstractPipelineStep):

    def __init__(self,
                 name: str,
                 description: str,
                 step_type: str,
                 dataframe_id: str,
                 source_type: str,
                 source_options: Union[ReadCsvOptions, ReadParquetOptions]):

        super().__init__(name, description, step_type, dataframe_id)

        self._logger = logging.getLogger(__name__)

        self.source_type = source_type
        self.source_options = source_options

    @property
    @abstractmethod
    def _start_to_load_repr(self) -> str:
        pass

    @property
    @abstractmethod
    def _successfully_loaded_data_repr(self) -> str:
        pass

    @abstractmethod
    def _setup_df_reader_method(self, spark_session: SparkSession) -> Callable[[str], DataFrame]:
        pass

    def load(self, spark_session: SparkSession) -> Tuple[str, DataFrame]:

        self._logger.info(self._start_to_load_repr)

        path: str = self.source_options.path
        df_reader_method: Callable[[str], DataFrame] = self._setup_df_reader_method(spark_session)
        df: DataFrame = df_reader_method(path)

        self._logger.info(self._successfully_loaded_data_repr)

        return self._dataframe_id.lower(), df


class ReadCsvStage(AbstractReadStage):

    def __init__(self,
                 name: str,
                 description: str,
                 step_type: str,
                 dataframe_id: str,
                 source_type: str,
                 source_options: dict):

        super().__init__(name, description, step_type, source_type, dataframe_id, ReadCsvOptions(**source_options))

    @property
    def _start_to_load_repr(self) -> str:

        path: str = self.source_options.path
        header: bool = self.source_options.header
        separator: str = self.source_options.separator

        return f"Starting to load .csv file at path '{path}' using options (header = '{header}', sep = '{separator}')"

    @property
    def _successfully_loaded_data_repr(self) -> str:

        path: str = self.source_options.path
        header: bool = self.source_options.header
        separator: str = self.source_options.separator

        return f"Successfully loaded .csv file at path '{path}' using options (header = '{header}', sep = '{separator}')"

    def _setup_df_reader_method(self, spark_session: SparkSession) -> Callable[[str], DataFrame]:

        csv_schema: StructType = self.source_options.df_schema
        header: bool = self.source_options.header
        separator: str = self.source_options.separator

        return spark_session\
            .read\
            .schema(csv_schema)\
            .option("header", header)\
            .option("sep", separator)\
            .csv


class ReadParquetStage(AbstractReadStage):

    def __init__(self,
                 name: str,
                 description: str,
                 step_type: str,
                 dataframe_id: str,
                 source_type: str,
                 source_options: dict):

        super().__init__(name, description, step_type, source_type, dataframe_id, ReadParquetOptions(**source_options))

    @property
    def _start_to_load_repr(self) -> str:

        path: str = self.source_options.path
        return f"Starting to load .parquet file at path '{path}'"

    @property
    def _successfully_loaded_data_repr(self) -> str:

        path: str = self.source_options.path
        return f"Successfullt loaded .parquet file at path '{path}'"

    def _setup_df_reader_method(self, spark_session: SparkSession) -> Callable[[str], DataFrame]:

        return spark_session\
            .read\
            .parquet
