import configparser
import logging
from typing import Dict

from pyspark.sql import DataFrame, SparkSession

from pipeline_runner.pipeline.abstract import AbstractPipelineStep
from pipeline_runner.pipeline.write.option import CsvDstOptions, HiveTableDstOptions, JDBCTableDstOptions
from pipeline_runner.pipeline.write.writer import HiveTableWriter

DST_OPTIONS_TYPE_SWITCH = {

    "csv": CsvDstOptions,
    "hive": HiveTableDstOptions,
    "jdbc": JDBCTableDstOptions
}


def _get(job_properties: configparser.ConfigParser, section: str, key: str):

    return job_properties[section][key]


def _get_or_else(job_properties: configparser.ConfigParser, section: str, key: str, default_value):

    return default_value if key is None else _get(job_properties, section, key)


class WriteStep(AbstractPipelineStep):

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
        self._dst_type = dst_options["destination_type"]
        self._dst_options = DST_OPTIONS_TYPE_SWITCH[self._dst_type].from_dict(dst_options)

    @classmethod
    def from_session_plus_dict(cls, spark_session: SparkSession, input_dict: dict):
        return cls(spark_session, **input_dict)

    @property
    def dst_type(self) -> str:
        return self._dst_type

    def write(self, df_dict: Dict[str, DataFrame], job_properties: configparser.ConfigParser) -> None:

        df: DataFrame = df_dict[self.dataframe_id]
        if isinstance(self._dst_options, HiveTableDstOptions):

            HiveTableWriter(job_properties, self._dst_options, self._spark_session).write(df)

