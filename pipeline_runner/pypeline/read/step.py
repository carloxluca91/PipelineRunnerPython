import logging
from configparser import ConfigParser

from pyspark.sql import DataFrame, SparkSession

from pypeline.abstract import AbstractStep
from pypeline.read.option import HiveTableSrcOptions, CsvSrcOptions
from pypeline.read.reader import CsvReader, HiveTableReader
from utils.spark import df_schema_tree_string

SRC_OPTIONS_TYPE = {

    "csv": CsvSrcOptions,
    "hive": HiveTableSrcOptions
}


class ReadStep(AbstractStep):

    def __init__(self,
                 name: str,
                 description: str,
                 step_type: str,
                 dataframe_id: str,
                 src_options: dict):

        super().__init__(name, description, step_type, dataframe_id)

        self._logger = logging.getLogger(__name__)
        self._src_type = src_options["srcType"]
        self._dst_options = SRC_OPTIONS_TYPE[self._src_type].from_dict(src_options)
        self._src_options = src_options

    @property
    def src_options(self):
        return self._src_options

    def read(self, job_properties: ConfigParser, spark_session: SparkSession) -> DataFrame:

        logger = self._logger
        src_options = self._src_options

        df: DataFrame
        if isinstance(src_options, CsvSrcOptions):

            df = CsvReader(job_properties, spark_session, src_options).read()

        elif isinstance(src_options, HiveTableSrcOptions):

            df = HiveTableReader(job_properties, spark_session, src_options).read()

        logger.info(f"Dataset read during read step '{self.name}' has schema " + df_schema_tree_string(df))
        return df