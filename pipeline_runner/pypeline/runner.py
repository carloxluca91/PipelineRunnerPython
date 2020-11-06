import json
import logging
from configparser import ConfigParser
from typing import List

from pyspark.sql import Row
from pyspark.sql import functions, SparkSession

from pypeline.pipeline import Pipeline


class PipelineRunner:

    def __init__(self,
                 pipeline_name: str,
                 job_properties: ConfigParser):

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

        default_db_name = self._job_properties["hive"]["hive.default.dbName"].lower()
        existing_databases: List[str] = [db.name.lower() for db in self._spark_session.catalog.listDatabases()]
        if default_db_name in existing_databases:

            # If default Hive Db exists, it's not the first time the application is run
            # Thus, retrieve location of .json file describing the pipeline from Hive Table containing pipeline infos

            pipeline_info_table_name = self._job_properties["hive"]["hive.default.pipelineInfoTable.full"]
            self._logger.info(f"Hive db {default_db_name} exists. Looking for pipeline '{self._pipeline_name}' on table '{pipeline_info_table_name}'")

            pipeline_row_list: List[Row] = self._spark_session.table(pipeline_info_table_name)\
                .filter(functions.col("pipeline_name") == self._pipeline_name.upper())\
                .select("file_name")\
                .collect()

            if len(pipeline_row_list) == 0:

                self._logger.warning(f"Unable to find any row related to pipeline '{self._pipeline_name}'. Thus, no processing will be triggered")
                return

            else:

                pipeline_file_name: str = pipeline_row_list[0]["file_name"]
                pipeline_file_dir_path = self._job_properties["hdfs"]["pipeline.json.dir.path"]
                pipeline_file_path = f"{pipeline_file_dir_path}\\{pipeline_file_name}"

        else:

            # Otherwise, it's the first time the application is run. Thus, run pipeline for INITIAL_LOAD independently on provided pipeline name

            initial_load = "INITIAL_LOAD"
            pipeline_info_table_name = self._job_properties["hive"]["hive.default.pipelineInfoTable"]
            self._logger.warning(f"Default Hive db ('{default_db_name}') does not exist. Thus, neither table '{pipeline_info_table_name}' does")
            if self._pipeline_name != initial_load:

                self._logger.warning(f"Thus, provided pipeline won't be run. Running '{initial_load}' instead")

            pipeline_file_path = self._job_properties["hdfs"]["pipeline.initialLoad.json.file.path"]

        with open(pipeline_file_path, mode="r", encoding="UTF-8") as f:

            json_dict: dict = json.load(f)

        pipeline_input_dict = json_dict
        pipeline_input_dict["jobProperties"] = self._job_properties
        pipeline_input_dict["sparkSession"] = self._spark_session

        pipeline = Pipeline.from_dict(pipeline_input_dict)
        self._logger.info(f"Successfully parsed pipeline specification file '{pipeline_file_path}' as a {Pipeline.__name__} object")
        self._logger.info(f"Kicking off pipeline '{self._pipeline_name}' right now")
        pipeline.run()
