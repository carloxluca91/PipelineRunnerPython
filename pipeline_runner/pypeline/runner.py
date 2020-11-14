import logging
from typing import List, Dict

from pyspark.sql import functions, Row, SparkSession

from pypeline.pipeline import Pipeline
from utils.json import JsonUtils
from utils.properties import CustomConfigParser


class PipelineRunner:

    def __init__(self,
                 pipeline_name: str,
                 job_properties: CustomConfigParser):

        self._logger = logging.getLogger(__name__)
        self._pipeline_name = pipeline_name
        self._job_properties = job_properties

        # Initialize SparkSession
        self._spark_session: SparkSession = SparkSession.builder \
            .enableHiveSupport() \
            .config("hive.exec.dynamic.partition", "true") \
            .config("hive.exec.dynamic.partition.mode", "nonstrict") \
            .getOrCreate()

        self._logger.info(f"Successfully got or created SparkSession for application '{self._spark_session.sparkContext.appName}'. "
                          f"Application Id: '{self._spark_session.sparkContext.applicationId}', "
                          f"UI url: {self._spark_session.sparkContext.uiWebUrl}")

    def run_pipeline(self):

        pypeline_runner_db_name = self._job_properties["hive.db.pypelineRunner.name"].lower()
        existing_databases: List[str] = [db.name.lower() for db in self._spark_session.catalog.listDatabases()]
        pypeline_runner_db_exists = pypeline_runner_db_name in existing_databases

        pipeline_info_table_name = self._job_properties["hive.table.pipelineInfo.name"].lower()
        pypeline_info_table_exists = pipeline_info_table_name in [
            table.name.lower() for table in self._spark_session.catalog.listTables(pypeline_runner_db_name)] if pypeline_runner_db_exists \
            else False

        pipeline_info_table_name_full = f"{pypeline_runner_db_name}.{pipeline_info_table_name}"
        pypeline_info_table_not_empty = self._spark_session\
            .table(pipeline_info_table_name_full)\
            .count() > 0

        if pypeline_runner_db_exists and pypeline_info_table_exists and pypeline_info_table_not_empty:

            # If Both Hive Db and table exists, it's not the first time the application is run
            # Thus, retrieve location of .json file describing the pipeline from Hive Table containing pipeline infos

            self._logger.info(f"Looking for info on pipeline '{self._pipeline_name}' on table '{pipeline_info_table_name_full}'")
            pipeline_row_list: List[Row] = self._spark_session\
                .table(pipeline_info_table_name_full)\
                .filter(functions.upper(functions.col("pipeline_name")) == self._pipeline_name.upper())\
                .select("file_name")\
                .collect()

            if len(pipeline_row_list) == 0:

                self._logger.warning(f"Unable to find any row related to pipeline '{self._pipeline_name}'. Thus, no processing will be triggered")
                return

            else:

                pipeline_file_name: str = pipeline_row_list[0]["file_name"]
                pipeline_file_dir_path = self._job_properties["pipeline.json.dir.path"]
                pipeline_file_path = f"{pipeline_file_dir_path}\\{pipeline_file_name}"

        else:

            # Otherwise, it's the first time the application or something is wrong with pipeline info.
            # Thus, run pipeline for INITIAL_LOAD independently on provided pipeline name

            initial_load = "INITIAL_LOAD"
            warning_message = f"Hive db '{pypeline_runner_db_name}' does not exist" if not pypeline_runner_db_exists \
                else (f"Table '{pipeline_info_table_name_full}' does not exist" if not pypeline_info_table_exists
                      else f"Table '{pipeline_info_table_name_full}' is empty")

            self._logger.warning(warning_message)
            if self._pipeline_name != initial_load:

                self._logger.warning(f"Thus, provided pipeline won't be run. Running '{initial_load}' instead")

            pipeline_file_path = self._job_properties["initialLoad.json.file.path"]

        pipeline_input_dict: Dict = JsonUtils.load_json(pipeline_file_path)
        pipeline_input_dict["jobProperties"] = self._job_properties
        pipeline_input_dict["sparkSession"] = self._spark_session

        pipeline = Pipeline.from_dict(pipeline_input_dict)
        self._logger.info(f"Successfully parsed pipeline specification file '{pipeline_file_path}' as a {Pipeline.__name__} object")
        self._logger.info(f"Kicking off pipeline '{self._pipeline_name}' right now")
        pipeline.run()
