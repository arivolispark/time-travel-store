from typing import NoReturn
from datetime import datetime
import constants
import os.path as path
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.types import StringType
from pyspark.sql.functions import lit
#from py4j.protocol import Py4JJavaError
import logging
import traceback


logger = logging.getLogger()
logger.setLevel(logging.DEBUG)
logger.debug("\n logger: ", logger)

global current_time, virtual_location, output_data_prefix


def create_spark_session(app_name: str) -> SparkSession:
    if app_name is None or len(app_name) == 0:
        raise Exception("Invalid app_name")

    return SparkSession.builder.appName(app_name).getOrCreate()


def display_df(df: DataFrame) -> NoReturn:
    if df is None or type(df) is not DataFrame:
        raise ValueError("Invalid dataframe")

    count = df.count()
    df.printSchema()
    logger.debug("\n count: ", count)
    df.show()


def get_current_time():
    return datetime.now()


def get_timestamp_year_month_day_hour(dt: datetime):
    if dt is None or type(dt) is not datetime:
        raise ValueError("Invalid datetime")

    return dt.timestamp(), dt.year, dt.month, dt.day, dt.hour


def get_s3_prefix(virtual_location: str, source: str, subset: str, dt: datetime) -> str:
    if virtual_location is None or type(virtual_location) is not str:
        raise ValueError("Invalid virtual_location")
    if source is None or type(source) is not str:
        raise ValueError("Invalid source")
    if subset is None or type(subset) is not str:
        raise ValueError("Invalid subset")
    if dt is None or type(dt) is not datetime:
        raise ValueError("Invalid datetime")

    global timestamp, year, month, day, hour
    timestamp, year, month, day, hour = get_timestamp_year_month_day_hour(dt)

    prefix = constants.S3_BASE_PREFIX_DATA +\
             path.sep + "parquet" +\
             virtual_location +\
             path.sep + constants.S3_PREFIX_SOURCE + constants.DELIMITER_EQUALS + source +\
             path.sep + constants.S3_PREFIX_SUBSET + constants.DELIMITER_EQUALS + subset +\
             path.sep + constants.S3_PREFIX_YEAR + constants.DELIMITER_EQUALS + str(year) +\
             path.sep + constants.S3_PREFIX_MONTH + constants.DELIMITER_EQUALS + str(month) +\
             path.sep + constants.S3_PREFIX_DAY + constants.DELIMITER_EQUALS + str(day) +\
             path.sep + constants.S3_PREFIX_HOUR + constants.DELIMITER_EQUALS + str(hour) +\
             path.sep + constants.S3_PREFIX_TIMESTAMP + constants.DELIMITER_EQUALS + str(timestamp)
    return prefix


def store_dataframe(df: DataFrame, config_args: dict) -> NoReturn:
    logger.debug("\n store_dataframe(df: DataFrame, config_args: dict)")
    display_df(df)
    logger.debug("\n config_args: ", config_args)

    virtual_location = config_args.get("virtual_location")
    if virtual_location is None or type(virtual_location) is not str:
        raise ValueError("Invalid virtual_location")

    source = config_args.get("source")
    if source is None or type(source) is not str:
        raise ValueError("Invalid source")

    subset = config_args.get("subset")
    if subset is None or type(subset) is not str:
        raise ValueError("Invalid subset")


    spark = create_spark_session(constants.VERSIONED_STORE_SPARK_APP_NAME)

    logger.debug("\n virtual_location: " + virtual_location)
    logger.debug("\n source: " + source)
    logger.debug("\n subset: " + subset)

    current_time = datetime.now()
    logger.debug("\n current_time: ", current_time)

    prefix = get_s3_prefix(virtual_location, source, subset, current_time)
    logger.debug("\n prefix: ", prefix)

    output_data_prefix = prefix
    logger.debug(" output_data_prefix: " + output_data_prefix)


    try:
        print("\n try block")
        df.write.csv(header="true", path=output_data_prefix)
    except Exception as e:
        print("\n except block")

        exc_obj = e
        tb_str = ''.join(traceback.format_exception(None, exc_obj, exc_obj.__traceback__))
        logger.debug("\n tb_str: ", tb_str)

        # Write to log
        # Emit event
        # Write to /metadata/failure prefix

        failure_df = spark.createDataFrame(["{job_run_id}"], StringType()).toDF("job_run_id")
        failure_df = failure_df.withColumn("timestamp", lit(current_time))
        failure_df = failure_df.withColumn("application_type", lit("Spark"))
        failure_df = failure_df.withColumn("virtual_location", lit("{virtual_location}"))
        failure_df = failure_df.withColumn("output_data_prefix", lit("{output_data_prefix}"))
        failure_df = failure_df.withColumn("output_data_format", lit("Parquet"))
        failure_df = failure_df.withColumn("schema_version", lit("{schema_version}"))
        failure_df = failure_df.withColumn("schema", lit("{schema}"))
        failure_df = failure_df.withColumn("status", lit("FAILURE"))
        failure_df = failure_df.withColumn("description", lit("{description}"))

        display_df(failure_df)

        failure_df.coalesce(1).write.format('json').save("operation.json", overwrite=True)


def load_dataframe(config_args: dict) -> DataFrame:
    logger.debug("\n load_dataframe(config_args: dict)")
    return None  # returns df


def load_latest_version_dataframe(config_args: dict) -> DataFrame:
    logger.debug("\n load_latest_version_dataframe(config_args: dict)")
    return None


def load_dataframe_by_time_range(config_args: dict) -> DataFrame:
    logger.debug("\n load_dataframe_by_time_range(config_args: dict)")
    return None
