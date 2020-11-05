import json
import logging
from configparser import ConfigParser
from typing import List

from pyspark.sql import functions, SparkSession

from pypeline.pipeline import Pipeline

logger = logging.getLogger(__name__)


class PipelineRunner:

    @staticmethod
    def run_pipeline(pipeline_name: str, job_properties: ConfigParser):

        spark_session: SparkSession = SparkSession.builder \
            .enableHiveSupport() \
            .config("hive.exec.dynamic.partition", "true") \
            .config("hive.exec.dynamic.partition.mode", "nonstrict") \
            .getOrCreate()

        logger.info(f"Successfully got or created SparkSession for application '{spark_session.sparkContext.appName}'. "
                    f"Application Id: '{spark_session.sparkContext.applicationId}', "
                    f"UI url: {spark_session.sparkContext.uiWebUrl}")

        default_db_name = job_properties["hive"]["hive.default.dbName"].lower()
        pypeline_info_table_name = job_properties["hive"]["hive.default.pipelineInfoTable"].lower()
        existing_databases: List[str] = [db.name.lower() for db in spark_session.catalog.listDatabases()]
        if default_db_name in existing_databases:

            # IF DEFAULT Hive Db EXISTS, IT MEANS IT'S NOT THE FIRST TIME THE APPLICATION IS RUN. THUS, RETRIEVE LOCATION OF .json FILE FROM Hive
            pipeline_file_name: str = spark_session.table(f"{default_db_name}.{pypeline_info_table_name}")\
                .filter(functions.col("pipeline_name") == pipeline_name.upper())\
                .select("file_name")\
                .collect()[0]

            pipeline_file_dir_path = job_properties["hdfs"]["pypeline.json.dir.path"]
            pipeline_file_path = f"{pipeline_file_dir_path}\\{pipeline_file_name}"

        else:

            # OTHERWISE, IT MEANS THIS IS THE FIRST TIME THE APPLICATION IS RUN. THUS, RUN PIPELINE FOR INITIAL LOAD
            pipeline_file_path = job_properties["hdfs"]["pypeline.loadTableInfo.json.file.path"]
            logger.warning(f"As default Hive db ('{default_db_name}') does not exist, provided pipeline won't be run. Running initial load instead")

        with open(pipeline_file_path, mode="r", encoding="UTF-8") as f:

            json_dict: dict = json.load(f)

        pipeline_input_dict = json_dict
        pipeline_input_dict["jobProperties"] = job_properties
        pipeline_input_dict["sparkSession"] = spark_session

        pipeline = Pipeline.from_dict(pipeline_input_dict)
        logger.info(f"Successfully parsed pipeline specification file '{pipeline_file_path}' as a {Pipeline.__name__}")
        pipeline.run()
