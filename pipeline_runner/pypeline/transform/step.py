import logging
from typing import Dict, Type, Union

from pyspark.sql import DataFrame

from pypeline.abstract import AbstractStep
from pypeline.transform.transformation import WithColumnTransformation, DropTransformation, SelectTransformation
from utils.spark import SparkUtils

_T = Union[Type[WithColumnTransformation],
           Type[DropTransformation],
           Type[SelectTransformation]]

_TRANSFORMATION_DICT: Dict[str, _T] = {

    "withColumn": WithColumnTransformation,
    "drop": DropTransformation,
    "select": SelectTransformation
}


class TransformStep(AbstractStep):

    def __init__(self,
                 name: str,
                 description: str,
                 step_type: str,
                 dataframe_id: str,
                 transformation_options: dict):

        super().__init__(name, description, step_type, dataframe_id)

        self._logger = logging.getLogger(__name__)
        self._transformation_type: str = transformation_options["transformationType"]
        self._transformation_options = transformation_options

    @property
    def transformation_type(self) -> str:

        return self._transformation_type

    @property
    def transformation_options(self):

        return self._transformation_options

    def transform(self, df_dict: Dict[str, DataFrame]) -> DataFrame:

        transformation = _TRANSFORMATION_DICT[self.transformation_type](self.name, self.transformation_options)
        self._logger.info(f"Successfully initialized transform step '{self.name}'")

        df: DataFrame = transformation.transform(df_dict)
        input_source_id = transformation.transformation_options.input_source_id

        self._logger.info(f"Successfully transformed input dataframe '{input_source_id}' to dataframe '{self.dataframe_id}'. "
                          f"Dataframe schema {SparkUtils.df_schema_tree_string(df)}")
        self._logger.info(f"Successfully executed transform step '{self.name}'")
        return df
