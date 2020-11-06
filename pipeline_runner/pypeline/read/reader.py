import logging
from abc import ABC, abstractmethod
from configparser import ConfigParser
from typing import Union

from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.types import StructType, StructField

from pypeline.read.option import CsvSrcOptions, HiveTableSrcOptions
from utils.json import JsonUtils
from utils.spark import SparkUtils


class AbstractReader(ABC):

    def __init__(self,
                 job_properties: ConfigParser,
                 spark_session: SparkSession,
                 src_options: Union[CsvSrcOptions, HiveTableSrcOptions]):

        self._logger = logging.getLogger(__name__)
        self._job_properties = job_properties
        self._spark_session = spark_session
        self._src_options = src_options

    def get(self, section: str, key: str):
        return self._job_properties[section][key]

    def get_or_else(self, section: str, key: str, default_value):
        return default_value if key is None else self.get(section, key)

    @abstractmethod
    def read(self) -> DataFrame: pass


class CsvReader(AbstractReader):

    def __init__(self,
                 job_properties: ConfigParser,
                 spark_session: SparkSession,
                 src_options: CsvSrcOptions):

        super().__init__(job_properties, spark_session, src_options)

    def _from_json_to_structype(self, json_file_path: str) -> StructType:

        json_dict: dict = JsonUtils.load_json(json_file_path)

        def to_structfield(dict_: dict) -> StructField:

            return StructField(dict_["name"], SparkUtils.get_spark_datatype(dict_["dataType"]), dict_["nullable"])

        structtype_from_json = StructType(list(map(to_structfield, json_dict["columns"])))
        self._logger.info(f"Successfully retrieved {StructType.__name__} from file '{json_file_path}'")
        return structtype_from_json

    def read(self) -> DataFrame:

        src_type = self._src_options.src_type

        path = self.get(src_type, self._src_options.path)
        schema_file = self.get(src_type, self._src_options.schema_file)
        header = self.get_or_else(src_type, self._src_options.header, False)
        separator = self.get_or_else(src_type, self._src_options.separator, ",")

        self._logger.info(f"Starting to load .csv data from path '{path}', header = '{header}', separator = '{separator}'")
        df = self._spark_session\
            .read\
            .option("header", header)\
            .option("sep", separator)\
            .schema(self._from_json_to_structype(schema_file))\
            .csv(path)

        self._logger.info(f"Successfully loaded .csv data from path '{path}', header = '{header}', separator = '{separator}'")
        return df


class HiveTableReader(AbstractReader):

    def __init__(self,
                 job_properties: ConfigParser,
                 spark_session: SparkSession,
                 src_options: HiveTableSrcOptions):

        super().__init__(job_properties, spark_session, src_options)

    def read(self) -> DataFrame:

        src_type = self._src_options.src_type
        db_name = self.get(src_type, self._src_options.db_name)
        table_name = self.get(src_type, self._src_options.table_name)

        df = self._spark_session.table(f"{db_name}.{table_name}")
        self._logger.info(f"Successfully loaded data from Hive table '{db_name}.{table_name}'")
        return df
