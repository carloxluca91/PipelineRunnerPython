import datetime

from pyspark.sql.types import StructType, StructField, StringType, TimestampType, DateType, IntegerType


class JDBCLogRecord:

    def __init__(self,
                 application_id: str,
                 application_name: str,
                 application_start_time: datetime.datetime,
                 application_start_date: datetime.date,
                 pipeline_name: str,
                 pipeline_description: str,
                 pipeline_id: str,
                 step_name: str,
                 step_description: str,
                 step_type: str,
                 dataframe_id: str,
                 step_finish_time: datetime.datetime,
                 step_finish_date: datetime.date,
                 step_finish_code: int,
                 step_finish_status: str,
                 exception_message: str):

        self.application_id = application_id
        self.application_name = application_name
        self.application_start_time = application_start_time
        self.application_start_date = application_start_date
        self.pipeline_name = pipeline_name
        self.pipeline_description = pipeline_description
        self.pipeline_id = pipeline_id
        self.step_name = step_name
        self.step_description = step_description
        self.step_type = step_type
        self.dataframe_id = dataframe_id
        self.step_finish_time = step_finish_time
        self.step_finish_date = step_finish_date
        self.step_finish_code = step_finish_code
        self.step_finish_status = step_finish_status
        self.exception_message = exception_message

    @classmethod
    def as_structype(cls) -> StructType:

        return StructType([StructField("application_id", StringType(), True),
                           StructField("application_name", StringType(), True),
                           StructField("application_start_time", TimestampType(), True),
                           StructField("application_start_date", DateType(), True),
                           StructField("pipeline_name", StringType(), True),
                           StructField("pipeline_description", StringType(), True),
                           StructField("pipeline_id", StringType(), True),
                           StructField("step_name", StringType(), True),
                           StructField("step_description", StringType(), True),
                           StructField("step_type", StringType(), True),
                           StructField("dataframe_id", StringType(), True),
                           StructField("step_finish_time", TimestampType(), True),
                           StructField("step_finish_date", DateType(), True),
                           StructField("step_finish_code", IntegerType(), True),
                           StructField("step_finish_status", StringType(), True),
                           StructField("exception_message",	StringType(), True)])
