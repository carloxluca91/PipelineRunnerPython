import logging

from pyspark.sql import DataFrame, SparkSession

from pypeline.abstract import AbstractStep
from pypeline.write.option import CsvDstOptions, HiveTableDstOptions, JDBCTableDstOptions
from pypeline.write.writer import HiveTableWriter, JDBCTableWriter
from utils.properties import CustomConfigParser
from utils.spark import SparkUtils


class WriteStep(AbstractStep):

    _DST_OPTIONS_TYPE = {

        "csv": CsvDstOptions,
        "hive": HiveTableDstOptions,
        "jdbc": JDBCTableDstOptions
    }

    def __init__(self,
                 name: str,
                 description: str,
                 step_type: str,
                 dataframe_id: str,
                 dst_options: dict):

        super().__init__(name, description, step_type, dataframe_id)

        self._logger = logging.getLogger(__name__)
        self._dst_type = dst_options["destinationType"]
        self._dst_options = self._DST_OPTIONS_TYPE[self._dst_type].from_dict(dst_options)

    @property
    def dst_type(self) -> str:
        return self._dst_type

    def write(self, df: DataFrame, job_properties: CustomConfigParser, spark_session: SparkSession) -> None:

        self._logger.info(f"Starting to write dataframe '{self.dataframe_id}'. Schema {SparkUtils.df_schema_tree_string(df)}")
        if isinstance(self._dst_options, HiveTableDstOptions):

            HiveTableWriter(job_properties, self._dst_options, spark_session).write(df)

        elif isinstance(self._dst_options, JDBCTableDstOptions):

            JDBCTableWriter(job_properties, self._dst_options).write(df)

        self._logger.info(f"Successfully written dataframe '{self.dataframe_id}'")
        self._logger.info(f"Successfully executed write step '{self.name}'")
