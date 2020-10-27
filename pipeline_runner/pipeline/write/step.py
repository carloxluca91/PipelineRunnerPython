import configparser
import logging
from typing import Dict

from pyspark.sql import DataFrame, SparkSession

from pipeline_runner.pipeline.abstract import AbstractStep
from pipeline_runner.pipeline.write.option import CsvDstOptions, HiveTableDstOptions, JDBCTableDstOptions
from pipeline_runner.pipeline.write.writer import HiveTableWriter, JDBCTableWriter
from pipeline_runner.utils.spark import df_schema_tree_string

DST_OPTIONS_TYPE = {

    "csv": CsvDstOptions,
    "hive": HiveTableDstOptions,
    "jdbc": JDBCTableDstOptions
}


class WriteStep(AbstractStep):

    def __init__(self,
                 spark_session: SparkSession,
                 name: str,
                 description: str,
                 step_type: str,
                 dataframe_id: str,
                 dst_options: dict):

        super().__init__(name, description, step_type, dataframe_id)

        self._logger = logging.getLogger(__name__)
        self._spark_session = spark_session
        self._dst_type = dst_options["destinationType"]
        self._dst_options = DST_OPTIONS_TYPE[self._dst_type].from_dict(dst_options)

    @property
    def dst_type(self) -> str:
        return self._dst_type

    def write(self, df_dict: Dict[str, DataFrame], job_properties: configparser.ConfigParser) -> None:

        dst_options = self._dst_options
        df = df_dict[self.dataframe_id]

        self._logger.info(f"Dataframe to be written during write step '{self.name}' has schema:\n" + df_schema_tree_string(df))
        if isinstance(dst_options, HiveTableDstOptions):

            HiveTableWriter(job_properties, dst_options, self._spark_session).write(df)

        elif isinstance(dst_options, JDBCTableDstOptions):

            JDBCTableWriter(job_properties, dst_options).write(df)
