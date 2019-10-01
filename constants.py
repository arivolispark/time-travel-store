import os.path as path

DELIMITER_EQUALS = "="
DELIMITER_UNDERSCORE = "_"

S3_BASE_PREFIX_DATA = path.sep + "data"
S3_BASE_PREFIX_METADATA = path.sep + "metadata"
S3_BASE_PREFIX_METADATA_SUCCESS = S3_BASE_PREFIX_METADATA + path.sep + "success"
S3_BASE_PREFIX_METADATA_FAILURE = S3_BASE_PREFIX_METADATA + path.sep + "failure"

S3_PREFIX_SOURCE = DELIMITER_UNDERSCORE + DELIMITER_UNDERSCORE + "source"
S3_PREFIX_SUBSET = DELIMITER_UNDERSCORE + DELIMITER_UNDERSCORE + "subset"
S3_PREFIX_YEAR = DELIMITER_UNDERSCORE + DELIMITER_UNDERSCORE + "year"
S3_PREFIX_MONTH = DELIMITER_UNDERSCORE + DELIMITER_UNDERSCORE + "month"
S3_PREFIX_DAY = DELIMITER_UNDERSCORE + DELIMITER_UNDERSCORE + "day"
S3_PREFIX_HOUR = DELIMITER_UNDERSCORE + DELIMITER_UNDERSCORE + "hour"
S3_PREFIX_TIMESTAMP = DELIMITER_UNDERSCORE + DELIMITER_UNDERSCORE + "timestamp"

DATA_FORMAT_PARQUET = "parquet"
SOURCE_SEER = "SEER"
SUBSET_ALL = "all"

VERSIONED_STORE_SPARK_APP_NAME = "VersionedStoreApp"