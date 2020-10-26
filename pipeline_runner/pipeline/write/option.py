from abc import ABC
from typing import List


class DstOptions(ABC):

    def __init__(self,
                 destination_type: str,
                 savemode: str,
                 partition_by: List[str],
                 coalesce: int):

        self._destination_type = destination_type
        self._savemode = savemode
        self._partition_by = partition_by
        self._coalesce = coalesce

    @property
    def destination_type(self) -> str:
        return self._destination_type

    @property
    def savemode(self) -> str:
        return self._savemode

    @property
    def partition_by(self) -> List[str]:
        return self._partition_by

    @property
    def coalesce(self) -> int:
        return self._coalesce

    @classmethod
    def from_dict(cls, input_dict: dict):
        return cls(**input_dict)


class FileDstOptions(DstOptions, ABC):

    def __init__(self,
                 destination_type: str,
                 savemode: str,
                 path: str,
                 partition_by: List[str],
                 coalesce: int):

        super().__init__(destination_type, savemode, partition_by, coalesce)

        self._path = path

    @property
    def path(self) -> str:
        return self._path


class TableDstOptions(DstOptions, ABC):

    def __init__(self,
                 destination_type: str,
                 savemode: str,
                 db_name: str,
                 table_name: str,
                 create_database_if_not_exists: str,
                 partition_by: List[str],
                 coalesce: int):

        super().__init__(destination_type, savemode, partition_by, coalesce)

        self._db_name = db_name
        self._table_name = table_name
        self._create_database_if_not_exists = create_database_if_not_exists

    @property
    def db_name(self) -> str:
        return self._db_name

    @property
    def table_name(self) -> str:
        return self._table_name

    @property
    def create_database_if_not_exists(self) -> str:
        return self._create_database_if_not_exists


class HiveTableDstOptions(TableDstOptions):

    def __init__(self,
                 destination_type: str,
                 savemode: str,
                 db_name: str,
                 table_name: str,
                 create_database_if_not_exists: str = None,
                 table_location: str = None,
                 db_location: str = None,
                 partition_by: List[str] = None,
                 coalesce: int = None):

        super().__init__(destination_type, savemode, db_name, table_name, create_database_if_not_exists, partition_by, coalesce)

        self._table_location = table_location
        self._db_location = db_location

    @property
    def table_location(self) -> str:
        return self._table_location

    @property
    def db_location(self) -> str:
        return self._db_location


class JDBCTableDstOptions(TableDstOptions):

    def __init__(self,
                 destination_type: str,
                 savemode: str,
                 db_name: str,
                 table_name: str,
                 jdbc_url: str,
                 jdbc_driver: str,
                 jdbc_username: str,
                 jdbc_password: str,
                 jdbc_use_ssl: str = None,
                 create_database_if_not_exists: str = None,
                 partition_by: List[str] = None,
                 coalesce: int = None):

        super().__init__(destination_type, savemode, db_name, table_name, create_database_if_not_exists, partition_by, coalesce)

        self._jdbc_url = jdbc_url
        self._jdbc_driver = jdbc_driver
        self._jdbc_username = jdbc_username
        self._jdbc_password = jdbc_password
        self._jdbc_use_ssl = jdbc_use_ssl

    @property
    def url(self) -> str:
        return self._jdbc_url

    @property
    def driver(self) -> str:
        return self._jdbc_driver

    @property
    def username(self) -> str:
        return self._jdbc_username

    @property
    def password(self) -> str:
        return self._jdbc_password

    @property
    def use_ssl(self) -> str:
        return self._jdbc_use_ssl


class CsvDstOptions(FileDstOptions):

    def __init__(self,
                 destination_type: str,
                 savemode: str,
                 path: str,
                 header: str,
                 sep: str,
                 partition_by: List[str] = None,
                 coalesce: int = None):

        super().__init__(destination_type, savemode, path, partition_by, coalesce)

        self._header = header
        self._sep = sep

    @property
    def header(self) -> str:
        return self._header

    @property
    def sep(self) -> str:
        return self._sep
