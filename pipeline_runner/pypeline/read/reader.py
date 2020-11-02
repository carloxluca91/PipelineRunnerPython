import logging
from abc import ABC, abstractmethod
from configparser import ConfigParser
from typing import Union

from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.types import StructType, StructField

from pypeline.read.option import CsvSrcOptions, HiveTableSrcOptions
from utils.json import load_json
from utils.spark import DATA_TYPE_DICT


class AbstractReader(ABC):

    def __init__(self,
                 job_properties: ConfigParser,
                 spark_session: SparkSession,
                 src_options: Union[CsvSrcOptions, HiveTableSrcOptions]):

        self._logger = logging.getLogger(__name__)
        self._job_properties = job_properties
        self._spark_session = spark_session
        self._src_options = src_options

    def _get(self, section: str, key: str):
        return self._job_properties[section][key]

    def _get_or_else(self, section: str, key: str, default_value):
        return default_value if key is None else self._get(section, key)

    @abstractmethod
    def read(self) -> DataFrame: pass


class CsvReader(AbstractReader):

    def __init__(self,
                 job_properties: ConfigParser,
                 spark_session: SparkSession,
                 src_options: CsvSrcOptions):

        super().__init__(job_properties, spark_session, src_options)

    def _from_json_to_structype(self, json_file_path: str) -> StructType:

        logger = self._logger

        json_dict: dict = load_json(json_file_path)

        def to_structfield(dict_: dict) -> StructField:

            return StructField(dict_["name"], DATA_TYPE_DICT[dict_["dataType"]], dict_["nullable"])

        structType_from_json = StructType(list(map(to_structfield, json_dict["columns"])))
        logger.info(f"Successfully retrieved StructType from file '{json_file_path}'")
        return structType_from_json

    def read(self) -> DataFrame:

        logger = self._logger
        src_options = self._src_options
        src_type = src_options.src_type
        spark_session = self._spark_session

        get = self._get
        get_or_else = self._get_or_else

        path = get(src_type, src_options.path)
        schema_file = get(src_type, src_options.schema_file)
        header = get_or_else(src_type, src_options.header, False)
        separator = get_or_else(src_type, src_options.separator, ",")

        logger.info(f"Starting to load .csv data from path '{path}', header = '{header}', separator = '{separator}'")
        df = spark_session\
            .read\
            .option("header", header)\
            .option("sep", separator)\
            .schema(self._from_json_to_structype(schema_file))\
            .csv(path)

        logger.info(f"Successfully loaded .csv data from path '{path}', header = '{header}', separator = '{separator}'")
        return df


class HiveTableReader(AbstractReader):

    def __init__(self,
                 job_properties: ConfigParser,
                 spark_session: SparkSession,
                 src_options: HiveTableSrcOptions):

        super().__init__(job_properties, spark_session, src_options)

    def read(self) -> DataFrame:

        logger = self._logger
        src_options = self._src_options
        src_type = src_options.src_type
        spark_session = self._spark_session

        get = self._get

        db_name = get(src_type, src_options.db_name)
        table_name = get(src_type, src_options.table_name)

        df = spark_session.table(f"{db_name}.{table_name}")
        logger.info(f"Successfully loaded data from Hive table '{db_name}.{table_name}'")
        return df
