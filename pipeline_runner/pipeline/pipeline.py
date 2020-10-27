import logging
from configparser import ConfigParser
from datetime import date, datetime
from typing import List, Dict, Tuple

from pyspark import SparkContext
from pyspark.sql import DataFrame, SparkSession

from pipeline_runner.pipeline.abstract import AbstractPipelineElement, AbstractStep
from pipeline_runner.pipeline.create.step import CreateStep
from pipeline_runner.pipeline.write.step import WriteStep
from pipeline_runner.utils.jdbc import get_spark_writer_jdbc_options
from pipeline_runner.utils.spark import JDBCLogRecord, df_schema_tree_string


def _extract_step_info(step: dict) -> (str, str, str, str):

    return step["name"], step["description"], step["stepType"], step["dataframeId"]


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

        def check_step_type(dict_: dict, key: str) -> Tuple[bool, int]:

            return (False, 0) if key not in dict_ else (True, len(dict_[key]))

        self._any_create_step, self._create_steps_number = check_step_type(self._pipeline_steps, "createSteps")
        self._any_read_step, self._read_steps_number = check_step_type(self._pipeline_steps, "readSteps")
        self._any_transform_step, self._transform_steps_number = check_step_type(self._pipeline_steps, "transformSteps")
        self._any_write_step, self._write_steps_number = check_step_type(self._pipeline_steps, "writeSteps")

        self._spark_session: SparkSession = SparkSession.builder\
            .enableHiveSupport()\
            .config("hive.exec.dynamic.partition", "true")\
            .config("hive.exec.dynamic.partition.mode", "nonstrict")\
            .getOrCreate()
            
        self._logger.info(f"Successfully got or created SparkSession for application '{self._spark_session.sparkContext.appName}'. "
                           f"Application Id: '{self._spark_session.sparkContext.applicationId}', "
                           f"UI url: {self._spark_session.sparkContext.uiWebUrl}")

        self._df_dict: Dict[str, DataFrame] = {}
        self._jdbc_log_records: List[JDBCLogRecord] = []

    @property
    def pipeline_id(self):
        return self._pipeline_id

    def run(self):

        self._logger.info(f"Kicking off pipeline '{self.name}' ('{self.description}')")

        if self._run_create_steps():

            if self._run_write_steps():

                self._logger.info(f"Successfully executed the whole pipeline '{self.name}'")

    def _run_create_steps(self) -> bool:

        logger = self._logger
        df_dict = self._df_dict

        everything_ok = True
        if self._any_create_step:

            create_steps: List[dict] = self._pipeline_steps["createSteps"]
            logger.info(f"Identified {self._create_steps_number} create step(s) within pipeline "
                              f"'{self.name}' "
                              f"('{self.description}')")

            for index, raw_create_step in enumerate(create_steps, start=1):

                step_name, step_description, step_type, dataframe_id = _extract_step_info(raw_create_step)
                try:

                    raw_create_step["spark_session"] = self._spark_session
                    create_step = CreateStep.from_dict(raw_create_step)
                    logger.info(f"Successfully initialized create step # {index} ('{create_step.name}')")
                    df_dict[create_step.dataframe_id] = create_step.create()
                    logger.info(f"Updated df dict with dataframeId of create step # {index} ('{create_step.name}'), i.e. '{create_step.dataframe_id}'")

                except Exception as exception:

                    logger.exception(f"Caught exception while running create step # {index} ('{step_name}')", exception)
                    jdbc_log_record = self._log_record(index, step_name, step_description, step_type, dataframe_id, exception)
                    self._jdbc_log_records.append(jdbc_log_record)
                    self._write_jdbc_log_records()
                    everything_ok = False
                    break

                else:

                    self._jdbc_log_records.append(self._log_record_from_step(index, create_step))

            if everything_ok:

                logger.info(f"Successfully executed all of {self._create_steps_number} create step(s) within pipeline '{self.name}'")

        else:

            logger.warning(f"No create steps defined within pipeline '{self.name}'")

        return everything_ok

    def _run_write_steps(self) -> bool:

        logger = self._logger
        df_dict = self._df_dict

        everything_ok = True
        if self._any_write_step:

            write_steps: List[dict] = self._pipeline_steps["writeSteps"]
            logger.info(f"Identified {self._write_steps_number} write step(s) within pipeline '{self.name}'")
            enumerate_start_index = sum([self._create_steps_number, self._read_steps_number, self._transform_steps_number]) + 1
            for index, raw_write_step in enumerate(write_steps, start=enumerate_start_index):

                step_name, step_description, step_type, dataframe_id = _extract_step_info(raw_write_step)

                try:

                    raw_write_step["spark_session"] = self._spark_session
                    write_step = WriteStep.from_dict(raw_write_step)
                    logger.info(f"Successfully initialized write step # {index} ('{write_step.name}')")
                    write_step.write(df_dict, self._job_properties)
                    logger.info(f"Successfully executed write step # {index} ('{write_step.name}')")

                except Exception as exception:

                    logger.exception(f"Caught exception while executing write step # {index} ('{step_name}')", exception)
                    jdbc_log_record = self._log_record(index, step_name, step_description, step_type, dataframe_id, exception)
                    self._jdbc_log_records.append(jdbc_log_record)
                    self._write_jdbc_log_records()
                    everything_ok = False
                    break

                else:

                    self._jdbc_log_records.append(self._log_record_from_step(index, write_step))

            if everything_ok:

                logger.info(f"Successfully executed all of {self._write_steps_number} write step(s) within pipeline '{self.name}'")
                self._write_jdbc_log_records()

        else:

            self._logger.warning(f"No write step has been defined. Thus, no data will be stored")

        return everything_ok

    def _log_record(self,
                    step_index: int,
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
                             step_index,
                             step_name,
                             step_description,
                             step_type,
                             dataframe_id,
                             datetime.now(),
                             datetime.now().date(),
                             0 if exception is None else -1,
                             "OK" if exception is None else "KO",
                             None if exception is None else repr(exception))

    def _log_record_from_step(self, step_index: int, step: AbstractStep, exception: Exception = None) -> JDBCLogRecord:

        return self._log_record(step_index, step.name, step.description, step.step_type, step.dataframe_id, exception)

    def _write_jdbc_log_records(self):

        logger = self._logger
        job_properties = self._job_properties
        jdbc_log_records = self._jdbc_log_records

        logging_dataframe = self._spark_session.createDataFrame(jdbc_log_records, JDBCLogRecord.as_structype())

        logger.info(f"Successfully turned list of {len(jdbc_log_records)} {JDBCLogRecord.__name__}(s) into a {DataFrame.__name__}")
        logger.info(f"DataFrame to be written has schema:\n{df_schema_tree_string(logging_dataframe)}")

        log_table_name_full = job_properties["jdbc"]["jdbc.default.logTable.full"]
        log_table_savemode = job_properties["jdbc"]["jdbc.default.logTable.saveMode"]

        logger.info(f"Starting to insert data into table '{log_table_name_full}' using save_mode '{log_table_savemode}'")

        logging_dataframe \
            .write \
            .format("jdbc") \
            .options(**get_spark_writer_jdbc_options(job_properties)) \
            .option("dbtable", log_table_name_full) \
            .mode(log_table_savemode) \
            .save()

        logger.info(f"Successfully inserted data into table '{log_table_name_full}' using save_mode '{log_table_savemode}'")
