import json
import logging
from configparser import ConfigParser
from typing import List

from pyspark.sql import functions, SparkSession

from pypeline.pipeline import Pipeline


class PipelineRunner:

    def __init__(self,
                 pipeline_name: str,
                 job_properties: ConfigParser):

        self._logger = logging.getLogger(__name__)
        self._pipeline_name = pipeline_name
        self._job_properties = job_properties

        self._spark_session: SparkSession = SparkSession.builder \
            .enableHiveSupport() \
            .config("hive.exec.dynamic.partition", "true") \
            .config("hive.exec.dynamic.partition.mode", "nonstrict") \
            .getOrCreate()

        self._logger.info(f"Successfully got or created SparkSession for application '{self._spark_session.sparkContext.appName}'. "
                          f"Application Id: '{self._spark_session.sparkContext.applicationId}', "
                          f"UI url: {self._spark_session.sparkContext.uiWebUrl}")

    def run_pipeline(self):

        logger = self._logger
        pipeline_name = self._pipeline_name
        job_properties = self._job_properties
        spark_session = self._spark_session

        default_db_name = job_properties["hive"]["hive.default.dbName"].lower()
        pypeline_info_table_name = job_properties["hive"]["hive.default.pipelineInfoTable"].lower()
        existing_databases: List[str] = [db.name.lower() for db in spark_session.catalog.listDatabases()]
        if default_db_name in existing_databases:

            # IF DEFAULT Hive Db EXISTS, IT MEANS IT'S NOT THE FIRST TIME THE APPLICATION IS RUN. THUS, RETRIEVE LOCATION OF .json FILE FROM Hive
            pipeline_file_name: str = spark_session.table(f"{default_db_name}.{pypeline_info_table_name}")\
                .filter(functions.col("pipeline_name") == pipeline_name.upper())\
                .select("file_name")\
                .collect()[0]

            pipeline_file_dir_path = job_properties["hdfs"]["pipeline.json.dir.path"]
            pipeline_file_path = f"{pipeline_file_dir_path}\\{pipeline_file_name}"

        else:

            # OTHERWISE, IT MEANS THIS IS THE FIRST TIME THE APPLICATION IS RUN. THUS, RUN PIPELINE FOR INITIAL LOAD
            pipeline_file_path = job_properties["hdfs"]["pipeline.initialLoad.json.file.path"]
            logger.warning(f"As default Hive db ('{default_db_name}') does not exist, provided pipeline won't be run. Running 'INITIAL_LOAD' instead")

        with open(pipeline_file_path, mode="r", encoding="UTF-8") as f:

            json_dict: dict = json.load(f)

        pipeline_input_dict = json_dict
        pipeline_input_dict["jobProperties"] = job_properties
        pipeline_input_dict["sparkSession"] = spark_session

        pipeline = Pipeline.from_dict(pipeline_input_dict)
        logger.info(f"Successfully parsed pipeline specification file '{pipeline_file_path}' as a {Pipeline.__name__}")
        pipeline.run()
