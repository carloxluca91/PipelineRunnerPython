import logging
from abc import ABC

from pyspark.sql.types import StructType, StructField

from pipeline_runner.specification.utils import DATA_TYPE_DICT, load_json


class AbstractReadOptions(ABC):

    def __init__(self, path: str):

        self.path = path


class ReadCsvOptions(AbstractReadOptions):

    def __init__(self,
                 path: str,
                 schema_file: str,
                 separator: str,
                 header: bool):

        super().__init__(path)

        self._logger = logging.getLogger(__name__)
        self.schema_file = schema_file
        self.separator = separator
        self.header = header
        self.df_schema = self._from_json_to_structtype(schema_file)

    def _from_json_to_structtype(self, json_file_path: str):

        json_content = load_json(json_file_path)
        structType_from_json: StructType = StructType(list(map(lambda x: StructField(name=x["_name"],
                                                                                     dataType=DATA_TYPE_DICT[x["type"]],
                                                                                     nullable=True if x["_nullable"].lower() == "true" else False),
                                                               json_content["schema"])))

        self._logger.info(f"Successfully retrieved StructType from file '{json_file_path}'")
        return structType_from_json


class ReadParquetOptions(AbstractReadOptions):

    def __init__(self, path: str):
        super().__init__(path)
