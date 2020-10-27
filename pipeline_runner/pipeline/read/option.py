from abc import ABC

from pipeline_runner.pipeline.abstract import AbstractJsonElement


class SrcOptions(AbstractJsonElement, ABC):

    def __init__(self,
                 src_type: str):

        super().__init__()

        self._src_type = src_type

    @property
    def src_type(self) -> str:
        return self._src_type


class FileSrcOptions(SrcOptions, ABC):

    def __init__(self,
                 src_type: str,
                 path: str):

        super().__init__(src_type)

        self._path = path

    @property
    def path(self) -> str:
        return self._path


class TableSrcOptions(SrcOptions, ABC):

    def __init__(self,
                 src_type: str,
                 db_name: str,
                 table_name: str):

        super().__init__(src_type)

        self._db_name = db_name
        self._table_name = table_name

    @property
    def db_name(self) -> str:
        return self._db_name

    @property
    def table_name(self) -> str:
        return self._table_name


class CsvSrcOptions(FileSrcOptions, ABC):

    def __init__(self,
                 src_type: str,
                 path: str,
                 schema_file: str,
                 separator: str = None,
                 header: str = None):

        super().__init__(src_type, path)

        self._schema_file = schema_file
        self._separator = separator
        self._header = header

    @property
    def schema_file(self) -> str:
        return self._schema_file

    @property
    def separator(self) -> str:
        return self._separator

    @property
    def header(self) -> str:
        return self._header


class HiveTableSrcOptions(TableSrcOptions):

    def __init__(self,
                 src_type: str,
                 db_name: str,
                 table_name: str):

        super().__init__(src_type, db_name, table_name)
