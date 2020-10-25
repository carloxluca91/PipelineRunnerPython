import logging
from abc import abstractmethod
from typing import Union, List

from pyspark.sql import DataFrame, DataFrameWriter

from pipeline_runner.specification.abstract import AbstractPipelineStep
from pipeline_runner.specification.write.options import WriteCsvOptions, WriteParquetOptions


class AbstractWriteStage(AbstractPipelineStep):

    def __init__(self,
                 name: str,
                 description: str,
                 step_type: str,
                 dataframe_id: str,
                 destination_type: str,
                 destination_options: Union[WriteCsvOptions, WriteParquetOptions]):

        super().__init__(name, description, step_type, dataframe_id)
        self._logger = logging.getLogger(__name__)
        self.destination_type = destination_type
        self.destination_options = destination_options

    def _get_base_df_writer(self, df: DataFrame) -> DataFrameWriter:

        savemode: str = self.destination_options.savemode
        coalesce: int = self.destination_options.coalesce
        partition_by: Union[List[str], str] = self.destination_options.partition_by

        df_maybe_coalesced = df if coalesce is None else df.coalesce(coalesce)
        partition_by_columns: List[str] = None if partition_by is None \
            else (list(partition_by) if isinstance(partition_by, str)
                  else partition_by)

        df_writer_maybe_partitioned: DataFrameWriter = df_maybe_coalesced.write if partition_by_columns is None \
            else df_maybe_coalesced\
            .write\
            .partitionBy(*partition_by_columns)

        self._logger.info(f"Setting up {type(DataFrameWriter).__name__} with following settings: "
                          f"format = '{self.destination_type}', "
                          f"savemode = '{savemode}', "
                          f"coalesce = '{coalesce}', "
                          f"partitionBy = '{partition_by_columns}'")

        return df_writer_maybe_partitioned\
            .format(self.destination_type)\
            .mode(savemode)

    @property
    @abstractmethod
    def _start_to_save_repr(self) -> str:
        pass

    @property
    @abstractmethod
    def _successfully_saved_repr(self) -> str:
        pass

    @abstractmethod
    def _extend_df_writer(self, df_writer: DataFrameWriter) -> DataFrameWriter:
        pass

    def write(self, df: DataFrame) -> None:

        self._logger.info(self._start_to_save_repr)

        base_df_writer: DataFrameWriter = self._get_base_df_writer(df)
        final_df_writer: DataFrameWriter = self._extend_df_writer(base_df_writer)
        final_df_writer.save(self.destination_options.path)

        self._logger.info(self._successfully_saved_repr)


class WriteCsvStage(AbstractWriteStage):

    def __init__(self,
                 name: str,
                 description: str,
                 step_type: str,
                 dataframe_id: str,
                 destination_type: str,
                 destination_options: dict):

        super().__init__(name, description, step_type, dataframe_id, destination_type, WriteCsvOptions(**destination_options))

    def _extend_df_writer(self, df_writer: DataFrameWriter) -> DataFrameWriter:

        delimiter: str = self.destination_options.delimiter
        header: bool = self.destination_options.header

        return df_writer\
            .option("sep", delimiter)\
            .option("header", header)

    @property
    def _start_to_save_repr(self) -> str:

        path: str = self.destination_options.path
        savemode: str = self.destination_options.savemode
        header: bool = self.destination_options.header
        delimiter: str = self.destination_options.delimiter

        return f"Starting to save data as .csv at path '{path}' with savemode '{savemode}' " \
               f"(header = '{header}', " \
               f"sep = '{delimiter}')"

    @property
    def _successfully_saved_repr(self) -> str:

        path: str = self.destination_options.path
        savemode: str = self.destination_options.savemode
        header: bool = self.destination_options.header
        delimiter: str = self.destination_options.delimiter

        return f"Successfully saved data as .csv at path '{path}' with savemode '{savemode}' " \
               f"(header = '{header}', " \
               f"sep = '{delimiter}')"


class WriteParquetStage(AbstractWriteStage):

    def __init__(self,
                 name: str,
                 description: str,
                 step_type: str,
                 dataframe_id: str,
                 destination_type: str,
                 destination_options: dict):

        super().__init__(name, description, step_type, dataframe_id, destination_type, WriteParquetOptions(**destination_options))

    @property
    def _start_to_save_repr(self) -> str:

        path: str = self.destination_options.path
        savemode: str = self.destination_options.savemode
        return f"Starting to save data as .parquet at path '{path}' with savemode '{savemode}'"

    @property
    def _successfully_saved_repr(self) -> str:

        path: str = self.destination_options.path
        savemode: str = self.destination_options.savemode
        return f"Successfully saved data as .parquet at path '{path}' with savemode '{savemode}'"

    def _extend_df_writer(self, df_writer: DataFrameWriter) -> DataFrameWriter:
        return df_writer
