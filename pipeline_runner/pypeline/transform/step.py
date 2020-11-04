import logging
from typing import Dict, Type, Union

from pyspark.sql import DataFrame

from pypeline.abstract import AbstractStep
from pypeline.transform.transformation import WithColumnTransformation, DropTransformation, SelectTransformation

T = Union[Type[WithColumnTransformation],
          Type[DropTransformation],
          Type[SelectTransformation]]

TRANSFORM_DICT: Dict[str, T] = {

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

        logger = self._logger

        transformation = TRANSFORM_DICT[self.transformation_type](self.name, self.transformation_options)
        logger.info(f"Successfully initialized transformation operation for transformStep '{self.name}'")

        input_source_id = transformation.transformation_options.input_source_id
        df: DataFrame = transformation.transform(df_dict)
        logger.info(f"Successfully transformed input dataframe with id '{input_source_id}' according to transformStep '{self.name}'")
        return df
