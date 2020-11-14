import logging

from pyspark.sql import DataFrame, SparkSession

from pypeline.abstract import AbstractStep
from pypeline.read.option import HiveTableSrcOptions, CsvSrcOptions
from pypeline.read.reader import CsvReader, HiveTableReader
from utils.properties import CustomConfigParser
from utils.spark import SparkUtils


class ReadStep(AbstractStep):

    _SRC_OPTIONS_TYPE = {

        "csv": CsvSrcOptions,
        "hive": HiveTableSrcOptions
    }

    def __init__(self,
                 name: str,
                 description: str,
                 step_type: str,
                 dataframe_id: str,
                 src_options: dict):

        super().__init__(name, description, step_type, dataframe_id)

        self._logger = logging.getLogger(__name__)
        self._src_type = src_options["srcType"]
        self._src_options = self._SRC_OPTIONS_TYPE[self._src_type].from_dict(src_options)

    @property
    def src_options(self):
        return self._src_options

    def read(self, job_properties: CustomConfigParser, spark_session: SparkSession) -> DataFrame:

        if isinstance(self._src_options, CsvSrcOptions):

            df: DataFrame = CsvReader(job_properties, spark_session, self._src_options).read()

        else:

            df: DataFrame = HiveTableReader(job_properties, spark_session, self._src_options).read()

        self._logger.info(f"Successfully read dataframe '{self.dataframe_id}'. Schema {SparkUtils.df_schema_tree_string(df)}")
        self._logger.info(f"Successfully executed read step '{self.name}'")
        return df
