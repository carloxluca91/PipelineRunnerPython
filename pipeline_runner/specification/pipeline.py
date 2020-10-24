import logging
from typing import List, Dict

from pyspark.sql import DataFrame, SparkSession

from pipeline_runner.specification.abstract import AbstractPipelineElement
from pipeline_runner.specification.read.stages import AbstractReadStage, ReadCsvStage, ReadParquetStage
from specification.create.step import CreateStep

SOURCE_TYPE_DICT = {

    "csv": ReadCsvStage,
    "parquet": ReadParquetStage
}


class Pipeline(AbstractPipelineElement):

    def __init__(self,
                 name: str,
                 description: str,
                 pipeline_id: str,
                 pipeline_steps: dict,
                 stages: dict):

        super().__init__(name, description)

        self._logger = logging.getLogger(__name__)

        self._pipeline_id = pipeline_id
        self._pipeline_steps = pipeline_steps
        self._spark_session: SparkSession = SparkSession.builder\
            .enableHiveSupport()\
            .config("hive.exec.dynamic.partition", "true")\
            .config("hive.exec.dynamic.partition.mode", "nonstrict")\
            .getOrCreate()

        self._logger.info(f"Successfully got or created SparkSession for application '{self._spark_session.sparkContext.appName}'. "
                           f"Application Id: '{self._spark_session.sparkContext.applicationId}', "
                           f"UI url: {self._spark_session.sparkContext.uiWebUrl}")

    def _get_loading_stages(self) -> List[AbstractReadStage]:

        # RETRIEVE LOADING STAGES AS A SIMPLE LIST OF DICTs

        original_loading_stages: List[dict] = self._pipeline_steps["read_steps"]

        # LIST OF LOADING STAGES PARSED AS PYTHON OBJECTS
        loading_stages_parsed: List[AbstractReadStage] = []
        self._logger.info(f"Identified {len(original_loading_stages)} reading steps within pipeline {self._name} ({self._description})")
        for stage_index, loading_stage in enumerate(original_loading_stages):

            # PARSE EACH LOADING STAGE FROM A SIMPLE DICT TO A PYTHON OBJECT DEPENDING ON "source_type"
            source_type_lc: str = loading_stage["src_options"]["source_type"]
            self._logger.info(f"Starting to parse loading stage # {stage_index} (source_type = '{source_type_lc}')")
            loading_stage_parsed: AbstractReadStage = SOURCE_TYPE_DICT[source_type_lc].from_dict(loading_stage)
            self._logger.info(f"Successfully parsed loading stage # {stage_index} (source_type = '{source_type_lc}', "
                              f"name = '{loading_stage_parsed._name}', "
                              f"description = '{loading_stage_parsed._description}', "
                              f"source_id = '{loading_stage_parsed._dataframe_id}')")

            # ADD TO LIST OF LOADING STAGES
            loading_stages_parsed.append(loading_stage_parsed)

        self._logger.info(f"Successfully parsed each loading stage")
        return loading_stages_parsed

    def run(self):

        sources_dict: Dict[str, DataFrame] = {}
        self._logger.info(f"Kicking off pipeline '{self._name}' ('{self._description}')")

        # PARSE LOADING_STAGES (IF PRESENT)
        if "read_steps" in list(self._pipeline_steps.keys()):

            loading_stages: List[AbstractReadStage] = self._get_loading_stages()
            for loading_stage_index, loading_stage in enumerate(loading_stages):

                self._logger.info(f"Starting loading stage # {loading_stage_index} (source_type = '{loading_stage.source_type}', "
                                  f"name = '{loading_stage._name}', "
                                  f"description = '{loading_stage._description}', "
                                  f"source_id = '{loading_stage._dataframe_id}')")

                # RUN EACH STAGE
                df_id, loaded_df = loading_stage.load(self._spark_session)

                self._logger.info(f"Successfully executed loading stage # {loading_stage_index} (source_type = '{loading_stage.source_type}', "
                                  f"name = '{loading_stage._name}', "
                                  f"description = '{loading_stage._description}', "
                                  f"source_id = '{loading_stage._dataframe_id}')")

                # SAVE THE RESULT IN A dict
                sources_dict.update(df_id=loaded_df)

        else:

            self._logger.warning(f"No loading stages defined within pipeline '{self._name}' ('{self._description}')")

        if "create_steps" in (list(self._pipeline_steps.keys())):

            raw_create_steps: List[dict] = self._pipeline_steps["create_steps"]
            self._logger.info(f"Identified {len(raw_create_steps)} create step(s) within pipeline '{self._name}' ({self._description})")

            for index, raw_create_step in enumerate(raw_create_steps):

                create_step = CreateStep.from_dict(raw_create_step)
                self._logger.info(f"Successfully initialized create step # {index} "
                                  f"('{create_step._name}', "
                                  f"description = '{create_step._description}')")

                spark_df: DataFrame = self._spark_session.createDataFrame(create_step.create())

                self._logger.info(f"Successfully created pyspark.sql.DataFrame related to create step # {index} "
                                  f"('{create_step._name}', "
                                  f"description = '{create_step._description}'). Related dataFrameId = '{create_step._dataframe_id}'")

                sources_dict.update({create_step._dataframe_id})
