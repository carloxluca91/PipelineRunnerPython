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
        self._src_options = SRC_OPTIONS_TYPE[self._src_type].from_dict(src_options)

    @property
    def src_options(self):
        return self._src_options

    def read(self, job_properties: ConfigParser, spark_session: SparkSession) -> DataFrame:

        logger = self._logger
        src_options = self._src_options

        if isinstance(src_options, CsvSrcOptions):

            df: DataFrame = CsvReader(job_properties, spark_session, src_options).read()

        else:

            df: DataFrame = HiveTableReader(job_properties, spark_session, HiveTableSrcOptions.from_dict(src_options)).read()

        logger.info(f"Successfully read dataframe '{self.dataframe_id}'. Schema {df_schema_tree_string(df)}")
        logger.info(f"Successfully executed read step '{self.name}'")
        return df
