import logging
from abc import abstractmethod

from pyspark.sql import DataFrame

from pipeline_runner.specification.abstract import AbstractPipelineElement


class AbstractTransformation(AbstractPipelineElement):

    def __init__(self,
                 name: str,
                 description: str,
                 source_id: str,
                 transformation_type: str):

        super().__init__(name, description)

        self._logger = logging.getLogger(__name__)

        self.source_id = source_id
        self.transformation_type = transformation_type

    @abstractmethod
    def _transform_input_df(self, input_df: DataFrame) -> DataFrame:
        pass

    @property
    @abstractmethod
    def transform_details(self) -> str:
        pass

    def transform(self, input_df: DataFrame) -> DataFrame:

        self._logger.info(f"Defining transformation of type '{self.transformation_type}' on source_id '{self.source_id}' ({self.transform_details})")
        return self._transform_input_df(input_df)
