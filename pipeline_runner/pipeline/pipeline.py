import logging
from configparser import ConfigParser
from datetime import date, datetime
from typing import List, Dict

import pandas as pd
from pyspark import SparkContext
from pyspark.sql import DataFrame, SparkSession

from pipeline_runner.pipeline.abstract import AbstractPipelineElement, AbstractStep
from pipeline_runner.pipeline.create.step import CreateStep
from pipeline_runner.pipeline.read.step import AbstractReadStage, ReadCsvStage, ReadParquetStage
from pipeline_runner.pipeline.write.step import WriteStep
from pipeline_runner.utils.jdbc import get_spark_writer_jdbc_options
from pipeline_runner.utils.spark import JDBCLogRecord, df_print_schema

SOURCE_TYPE_DICT = {

    "csv": ReadCsvStage,
    "parquet": ReadParquetStage
}


def _extract_step_info(step: dict) -> (str, str, str, str):

    return step["name"], step["description"], step["dataframeId"], step["stepType"]


class Pipeline(AbstractPipelineElement):

    def __init__(self,
                 job_properties: ConfigParser,
                 name: str,
                 description: str,
                 pipeline_id: str,
                 pipeline_steps: dict):

        super().__init__(name, description)

        self._logger = logging.getLogger(__name__)

        self._job_properties = job_properties
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

        self._dataframe_dict: Dict[str, DataFrame] = {}
        self._jdbc_log_records: List[JDBCLogRecord] = []

    @property
    def pipeline_id(self):
        return self._pipeline_id

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
                              f"source_id = '{loading_stage_parsed.dataframe_id}')")

            # ADD TO LIST OF LOADING STAGES
            loading_stages_parsed.append(loading_stage_parsed)

        self._logger.info(f"Successfully parsed each loading stage")
        return loading_stages_parsed

    def run(self):

        self._logger.info(f"Kicking off pipeline '{self.name}' ('{self.description}')")

        if self._run_create_steps():

            if self._run_write_steps():

                self._logger.info(f"Successfully executed the whole pipeline '{self.name}' ('{self.description}')")

        '''
        # PARSE LOADING_STAGES (IF PRESENT)
        if "read_steps" in list(self._pipeline_steps.keys()):

            loading_stages: List[AbstractReadStage] = self._get_loading_stages()
            for loading_stage_index, loading_stage in enumerate(loading_stages):

                self._logger.info(f"Starting loading stage # {loading_stage_index} (source_type = '{loading_stage.source_type}', "
                                  f"name = '{loading_stage.name}', "
                                  f"description = '{loading_stage.description}', "
                                  f"source_id = '{loading_stage.dataframe_id}')")

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
                
        '''

    def _run_create_steps(self) -> bool:

        everything_ok = True
        if "createSteps" in self._pipeline_steps:

            create_steps: List[dict] = self._pipeline_steps["createSteps"]
            self._logger.info(f"Identified {len(create_steps)} create step(s) within pipeline "
                              f"'{self.name}' "
                              f"('{self.description}')")

            for index, raw_create_step in enumerate(create_steps, start=1):

                step_name, step_description, step_type, dataframe_id = _extract_step_info(raw_create_step)
                try:

                    create_step = CreateStep.from_dict(raw_create_step)
                    self._logger.info(f"Successfully initialized create step # {index} "
                                      f"('{create_step.name}', "
                                      f"description = '{create_step.description}')")

                    pd_dataframe: pd.DataFrame = create_step.create()
                    spark_dataframe: DataFrame = self._spark_session.createDataFrame(pd_dataframe)

                    self._logger.info(f"Successfully created pyspark.sql.DataFrame related to create step # {index} "
                                      f"('{create_step.name}', "
                                      f"description = '{create_step.description}'). Related dataFrameId = '{create_step.dataframe_id}'")

                    self._logger.info(f"Created pyspark.sql.Dataframe has schema:\n\n " + df_print_schema(spark_dataframe))
                    self._dataframe_dict[create_step.dataframe_id] = spark_dataframe

                except Exception as exception:

                    self._logger.exception(f"Caught exception while running create step # {index} "
                                       f"('{step_name}', "
                                       f"description = '{step_description}'", exception)

                    jdbc_log_record = self._log_record(step_name, step_description, step_type, dataframe_id, exception)
                    self._jdbc_log_records.append(jdbc_log_record)
                    self._write_jdbc_log_records()
                    everything_ok = False
                    break

                else:

                    self._jdbc_log_records.append(self._log_record_from_step(create_step))

            if everything_ok:

                self._logger.info(f"Successfully executed all of {len(create_steps)} create step(s) within pipeline "
                                  f"'{self.name}' "
                                  f"('{self.description}')")

        else:

            self._logger.warning(f"No create steps defined within pipeline '{self.name}' ('{self.description}')")

        return everything_ok

    def _run_write_steps(self) -> bool:

        everything_ok = True
        if "writeSteps" in self._pipeline_steps:

            write_steps: List[dict] = self._pipeline_steps["writeSteps"]
            self._logger.info(f"Identified {len(write_steps)} write step(s) within pipeline "
                              f"'{self.name}' "
                              f"('{self.description}')")

            for index, raw_write_step in enumerate(write_steps, start=1):

                step_name, step_description, step_type, dataframe_id = _extract_step_info(raw_write_step)

                try:

                    raw_write_step["spark_session"] = self._spark_session
                    write_step = WriteStep.from_dict(raw_write_step)
                    self._logger.info(f"Successfully initialized write step # {index} "
                                      f"('{write_step.name}', "
                                      f"description = '{write_step.description}')")

                    write_step.write(self._dataframe_dict, self._job_properties)
                    self._logger.info(f"Successfully executed write step # {index} "
                                      f"('{write_step.name}', "
                                      f"description = '{write_step.description}', "
                                      f"dataframeId = '{write_step.dataframe_id}')")

                except Exception as exception:

                    self._logger.exception(f"Caught exception while executing write step # {index} "
                                           f"('{step_name}', "
                                           f"description = '{step_description}', "
                                           f"dataframeId = '{dataframe_id}')", exception)

                    jdbc_log_record = self._log_record(step_name, step_description, step_type, dataframe_id, exception)
                    self._jdbc_log_records.append(jdbc_log_record)
                    self._write_jdbc_log_records()
                    everything_ok = False
                    break

                else:

                    self._jdbc_log_records.append(self._log_record_from_step(write_step))

            if everything_ok:

                self._logger.info(f"Successfully executed all of {len(write_steps)} write step(s) within pipeline "
                                  f"'{self.name}' "
                                  f"('{self.description}')")

                self._write_jdbc_log_records()

        else:

            self._logger.warning(f"No write step has been defined. Thus, no data will be stored")

        return everything_ok

    def _log_record(self,
                    step_name: str,
                    step_description: str,
                    step_type: str,
                    dataframe_id: str,
                    exception: Exception = None):

        spark_context: SparkContext = self._spark_session.sparkContext

        application_id: str = spark_context.applicationId
        application_name: str = spark_context.appName
        application_start_time: datetime = datetime.fromtimestamp(spark_context.startTime / 1000)
        application_start_date: date = application_start_time.date()

        return JDBCLogRecord(application_id,
                             application_name,
                             application_start_time,
                             application_start_date,
                             self.name,
                             self.description,
                             self.pipeline_id,
                             step_name,
                             step_description,
                             step_type,
                             dataframe_id,
                             datetime.now(),
                             datetime.now().date(),
                             0 if exception is None else -1,
                             "OK" if exception is None else "KO",
                             None if exception is None else repr(exception))

    def _log_record_from_step(self, step: AbstractStep, exception: Exception = None) -> JDBCLogRecord:

        return self._log_record(step.name, step.description, step.step_type, step.dataframe_id, exception)

    def _write_jdbc_log_records(self):

        logging_dataframe: DataFrame = self._spark_session.createDataFrame(self._jdbc_log_records, JDBCLogRecord.as_structype())

        self._logger.info(f"Successfully turned list of {len(self._jdbc_log_records)} {JDBCLogRecord.__name__}(s) into a {DataFrame.__name__}")
        self._logger.info(f"DataFrame to be written has schema:\n{df_print_schema(logging_dataframe)}")

        log_table_name_full: str = self._job_properties["jdbc"]["jdbc.default.logTable.full"]
        log_table_savemode: str = self._job_properties["jdbc"]["jdbc.default.logTable.saveMode"]

        self._logger.info(f"Starting to insert data into table '{log_table_name_full}' using save_mode '{log_table_savemode}'")

        logging_dataframe \
            .write \
            .format("jdbc") \
            .options(**get_spark_writer_jdbc_options(self._job_properties)) \
            .option("dbtable", log_table_name_full) \
            .mode(log_table_savemode) \
            .save()

        self._logger.info(f"Successfully inserted data into table '{log_table_name_full}' using save_mode '{log_table_savemode}'")
