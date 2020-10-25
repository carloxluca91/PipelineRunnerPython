from abc import ABC
from typing import Union, List


class AbstractWriteOptions(ABC):

    def __init__(self,
                 path: str,
                 savemode: str,
                 coalesce: int,
                 partition_by: Union[str, List[str]]):

        self.path = path
        self.savemode = savemode
        self.coalesce = coalesce
        self.partition_by = partition_by


class WriteCsvOptions(AbstractWriteOptions):

    def __init__(self,
                 path: str,
                 savemode: str,
                 coalesce: int,
                 partition_by: Union[str, List[str]],
                 delimiter: str,
                 header: bool):

        super().__init__(path, savemode, coalesce, partition_by)
        self.delimiter = delimiter
        self.header = header


class WriteParquetOptions(AbstractWriteOptions):

    def __init__(self,
                 path: str,
                 savemode: str,
                 coalesce: int,
                 partition_by: Union[str, List[str]]):

        super().__init__(path, savemode, coalesce, partition_by)
