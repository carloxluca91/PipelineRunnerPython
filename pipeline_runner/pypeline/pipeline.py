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
from utils.jdbc import JDBCUtils
from utils.spark import LogRecord, SparkUtils


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
        self._log_records: List[LogRecord] = []

    # noinspection PyBroadException
    def run(self):

        def get_steps_for_key(dict_: dict, key: str) -> List[Dict[str, str]]:

            return [] if key not in dict_ else dict_[key]

        create_steps = get_steps_for_key(self._pipeline_steps, "createSteps")
        read_steps = get_steps_for_key(self._pipeline_steps, "readSteps")
        transform_steps = get_steps_for_key(self._pipeline_steps, "transformSteps")
        write_steps = get_steps_for_key(self._pipeline_steps, "writeSteps")

        def extract_step_info(step: Dict[str, str]) -> (str, str, str, str):

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
                    transformed_df: DataFrame = typed_step.combine(self._df_dict)
                    self._update_df_dict(typed_step.dataframe_id, transformed_df)

                else:

                    typed_step = WriteStep.from_dict(raw_step)
                    write_df: DataFrame = self._df_dict[typed_step.dataframe_id]
                    typed_step.write(write_df, self._job_properties, self._spark_session)

            except Exception as e:

                self._logger.exception(f"Caught exception while running step # {index}, name '{step_name}', type '{step_type}'")
                self._log_records.append(self._build_log_record(index, step_name, step_description, step_type, dataframe_id, e))
                self._write_log_records()
                everything_ok = False
                break

            else:

                self._log_records.append(self._build_log_record_for_step(index, typed_step))
                self._logger.info(f"Successfully executed step # {index}, name '{step_name}', type '{step_type}'")

        if everything_ok:

            self._write_log_records()
            self._logger.info(f"Successfully executed whole pipeline '{self.name}'")

        else:

            self._logger.warning(f"Unable to fully execute pipeline '{self.name}'")

    def _update_df_dict(self, dataframe_id: str, df: DataFrame):

        if dataframe_id in self._df_dict:

            old_df: DataFrame = self._df_dict[dataframe_id]
            self._logger.warning(f"Dataframe '{dataframe_id}' already exists. Schema {SparkUtils.df_schema_tree_string(old_df)}")
            self._logger.warning(f"It will be overriden by a new DataFrame with schema {SparkUtils.df_schema_tree_string(df)}")

        self._df_dict[dataframe_id] = df

    def _build_log_record(self,
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

        return LogRecord(application_id,
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

    def _build_log_record_for_step(self, step_index: int, step: AbstractStep, exception: Exception = None) -> LogRecord:

        return self._build_log_record(step_index, step.name, step.description, step.step_type, step.dataframe_id, exception)

    def _write_log_records(self):

        logging_dataframe = self._spark_session.createDataFrame(self._log_records, LogRecord.as_structype())

        self._logger.info(f"Successfully turned list of {len(self._log_records)} {LogRecord.__name__}(s) into a {DataFrame.__name__}")
        self._logger.info(f"Logging dataframe schema {SparkUtils.df_schema_tree_string(logging_dataframe)}")

        log_table_db_name = self._job_properties["jdbc"]["jdbc.default.dbName"]
        log_table_name_full = self._job_properties["jdbc"]["jdbc.default.logTable.full"]
        log_table_savemode = self._job_properties["jdbc"]["jdbc.default.logTable.saveMode"]

        mysql_connection = mysql.connector.connect(**JDBCUtils.get_connector_options(self._job_properties))
        self._logger.info(f"Successfully estabilished JDBC connection with default coordinates")
        JDBCUtils.create_db_if_not_exists(mysql_connection.cursor(), log_table_db_name)

        self._logger.info(f"Starting to insert data into table '{log_table_name_full}' using save_mode '{log_table_savemode}'")

        logging_dataframe \
            .coalesce(1) \
            .write \
            .format("jdbc") \
            .options(**JDBCUtils.get_spark_writer_jdbc_options(self._job_properties)) \
            .option("dbtable", log_table_name_full) \
            .mode(log_table_savemode) \
            .save()

        self._logger.info(f"Successfully inserted data into table '{log_table_name_full}' using save_mode '{log_table_savemode}'")
