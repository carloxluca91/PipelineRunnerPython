import logging
from configparser import ConfigParser
from datetime import date, datetime
from typing import List, Dict

import mysql
from mysql import connector
from pyspark import SparkContext
from pyspark.sql import DataFrame, SparkSession

from pypeline.abstract import AbstractPipelineElement, AbstractStep
from pypeline.create.step import CreateStep
from pypeline.read.step import ReadStep
from pypeline.transform.step import TransformStep
from pypeline.write.step import WriteStep
from utils.jdbc import create_db_if_not_exists, get_connector_options, get_spark_writer_jdbc_options
from utils.spark import JDBCLogRecord, df_schema_tree_string


class Pipeline(AbstractPipelineElement):

    def __init__(self,
                 job_properties: ConfigParser,
                 spark_session: SparkSession,
                 name: str,
                 description: str,
                 pipeline_steps: dict):

        super().__init__(name, description)

        self._logger = logging.getLogger(__name__)

        self._job_properties = job_properties
        self._pipeline_steps = pipeline_steps
        self._spark_session = spark_session

        self._df_dict: Dict[str, DataFrame] = {}
        self._jdbc_log_records: List[JDBCLogRecord] = []

    # noinspection PyBroadException
    def run(self):

        logger = self._logger
        df_dict = self._df_dict

        def get_steps_for_key(dict_: dict, key: str) -> List[Dict]:

            return [] if key not in dict_ else dict_[key]

        create_steps = get_steps_for_key(self._pipeline_steps, "createSteps")
        read_steps = get_steps_for_key(self._pipeline_steps, "readSteps")
        transform_steps = get_steps_for_key(self._pipeline_steps, "transformSteps")
        write_steps = get_steps_for_key(self._pipeline_steps, "writeSteps")

        def extract_step_info(step: dict) -> (str, str, str, str):

            return step["name"], step["description"], step["stepType"], step["dataframeId"]

        everything_ok = True
        full_step_list = create_steps + read_steps + transform_steps + write_steps
        for index, raw_step in enumerate(full_step_list, start=1):

            step_name, step_description, step_type, dataframe_id = extract_step_info(raw_step)
            try:

                if step_type == "create":

                    typed_step = CreateStep.from_dict(raw_step)
                    created_df: DataFrame = typed_step.create(self._spark_session)
                    self._update_df_dict(typed_step.dataframe_id, created_df)

                elif step_type == "read":

                    typed_step = ReadStep.from_dict(raw_step)
                    read_df: DataFrame = typed_step.read(self._job_properties, self._spark_session)
                    self._update_df_dict(typed_step.dataframe_id, read_df)

                elif step_type == "transform":

                    typed_step = TransformStep.from_dict(raw_step)
                    transformed_df: DataFrame = typed_step.transform(df_dict)
                    self._update_df_dict(typed_step.dataframe_id, transformed_df)

                else:

                    typed_step = WriteStep.from_dict(raw_step)
                    typed_step.write(df_dict, self._job_properties)

            except Exception as e:

                logger.exception(f"Caught exception while running step # {index} of type '{step_type}' and name '{step_name}", e)
                jdbc_log_record = self._log_record(index, step_name, step_description, step_type, dataframe_id, e)
                self._jdbc_log_records.append(jdbc_log_record)
                self._write_jdbc_log_records()
                everything_ok = False
                break

            else:

                self._jdbc_log_records.append(self._log_record_from_step(index, typed_step))
                logger.info(f"Successfully executed step # {index} of type '{step_type}' and name '{step_name}'")

        if everything_ok:

            self._write_jdbc_log_records()
            self._logger.info(f"Successfully executed whole pipeline '{self.name}'")

    def _update_df_dict(self, dataframe_id: str, df: DataFrame):

        logger = self._logger
        df_dict = self._df_dict

        if dataframe_id in df_dict:

            logger.warning(f"Dataframe '{dataframe_id}' already exists. "
                           f"It will be overriden by a DataFrame with schema {df_schema_tree_string(df)}")

        df_dict[dataframe_id] = df

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
                             step_index,
                             step_name,
                             step_description,
                             step_type,
                             dataframe_id,
                             datetime.now(),
                             datetime.now().date(),
                             -1 if exception else 0,
                             "KO" if exception else "OK",
                             repr(exception) if exception else None)

    def _log_record_from_step(self, step_index: int, step: AbstractStep, exception: Exception = None) -> JDBCLogRecord:

        return self._log_record(step_index, step.name, step.description, step.step_type, step.dataframe_id, exception)

    def _write_jdbc_log_records(self):

        logger = self._logger
        job_properties = self._job_properties
        jdbc_log_records = self._jdbc_log_records

        logging_dataframe = self._spark_session.createDataFrame(jdbc_log_records, JDBCLogRecord.as_structype())

        logger.info(f"Successfully turned list of {len(jdbc_log_records)} {JDBCLogRecord.__name__}(s) into a {DataFrame.__name__}")
        logger.info(f"Logging dataFrame schema {df_schema_tree_string(logging_dataframe)}")

        log_table_db_name = job_properties["jdbc"]["jdbc.default.dbName"]
        log_table_name_full = job_properties["jdbc"]["jdbc.default.logTable.full"]
        log_table_savemode = job_properties["jdbc"]["jdbc.default.logTable.saveMode"]

        mysql_connection = mysql.connector.connect(**get_connector_options(job_properties))
        logger.info(f"Successfully estabilished JDBC connection with default coordinates")
        create_db_if_not_exists(mysql_connection.cursor(), log_table_db_name)

        logger.info(f"Starting to insert data into table '{log_table_name_full}' using save_mode '{log_table_savemode}'")

        logging_dataframe \
            .write \
            .format("jdbc") \
            .options(**get_spark_writer_jdbc_options(job_properties)) \
            .option("dbtable", log_table_name_full) \
            .mode(log_table_savemode) \
            .save()

        logger.info(f"Successfully inserted data into table '{log_table_name_full}' using save_mode '{log_table_savemode}'")
