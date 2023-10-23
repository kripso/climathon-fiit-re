import os
import time
from datetime import datetime
from enum import Enum
from functools import wraps
from typing import (Any, Callable, Concatenate, Dict, Iterable, List, Optional,
                    ParamSpec, TypeVar, Union)

import more_itertools
from pyspark import SparkConf
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql import types as T
from pyspark.sql.dataframe import DataFrame

DEFAULT_SPARK_CONF = [

    ["spark.cores.max", "12"],

    ["spark.driver.cores", "8"],
    ["spark.driver.memory", "24g"],
    ["spark.driver.memoryOverhead", "8g"],
    ["spark.driver.maxResultSize", "3g"],
    ["spark.driver.bindAddress", "localhost"],
    ["spark.driver.host", "localhost"],

    ["spark.executor.memoryOverhead", "3"],
    ["spark.executor.instances", "2"],
    ["spark.executor.memory", "4g"],
    ["spark.executor.cores", "2"],

    ["spark.sql.shuffle.partitions", "100"],

    ["spark.eventLog.enabled", "true"],
    ["spark.eventLog.dir", "s3a://test/pyspark-logs"],

    ["spark.ui.port", "4050"],

    # ["spark.history.store.path", "s3a://test/pyspark-logs"],
    ["spark.history.fs.logDirectory", "s3a://test/pyspark-logs"],
    ['spark.history.retainedApplications', "50"],
    ['spark.history.ui.port', "18080"],

    # ["spark.jars", "./jars/postgresql-42.2.19.jar"],

    # delta
    ["spark.jars.packages", "org.apache.hadoop:hadoop-aws:3.3.4,io.delta:delta-spark_2.12:3.0.0"],
    # ["spark.jars.packages", "org.apache.hadoop:hadoop-aws:3.3.4,com.amazonaws:aws-java-sdk-bundle:1.12.262,org.apache.hadoop:hadoop-common:3.3.4,io.delta:delta-spark_2.12:3.0.0"],
    ["spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension"],
    ["spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog"],
    ["spark.delta.logStore.class", "org.apache.spark.sql.delta.storage.S3SingleDriverLogStore"],
    ["spark.hadoop.delta.enableFastS3AListFrom", "true"],
    # ["spark.databricks.delta.replaceWhere.constraintCheck.enabled", "false"],

    ["spark.hadoop.fs.s3a.aws.credentials.provider", "org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider"],
    ["spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem"],
    ["spark.hadoop.fs.s3a.access.key", "9sDoI9HqA4uNymT1Nwb8"],
    ["spark.hadoop.fs.s3a.secret.key", "oo5xezcbCdGLyCjd3dMl4NAmZF3iNYiajEYjGokq"],
    ["spark.hadoop.fs.s3a.endpoint", "192.168.0.199:9002"],
    # ["spark.hadoop.fs.s3a.endpoint", "https://s3minio.kripso-world.com"],
    ["spark.hadoop.fs.s3a.connection.ssl.enabled", "false"],
    ["spark.hadoop.fs.s3a.path.style.access", "true"],
    ["spark.hadoop.fs.s3a.committer.magic.enabled", "true"],
    ["spark.hadoop.fs.s3a.committer.staging.abort.pending.uploads", "true"],
    ["spark.hadoop.fs.s3a.committer.staging.unique-filenames", "true"],
    ["spark.hadoop.fs.s3a.committer.name", "partitioned"],
    ["spark.hadoop.fs.s3a.fast.upload", "true"],
    ["spark.hadoop.fs.s3a.downgrade.syncable.exceptions", "true"],
    ["spark.hadoop.fs.s3a.buffer.dir", "/tmp"],

    ["spark.hadoop.mapreduce.outputcommitter.factory.scheme.s3a", "org.apache.hadoop.fs.s3a.commit.S3ACommitterFactory"],
    ["spark.sql.parquet.int96RebaseModeInWrite", "CORRECTED"],
    ["spark.sql.parquet.int96RebaseModeInRead", "CORRECTED"],
    ["spark.sql.parquet.datetimeRebaseModeInWrite", "CORRECTED"],
    ["spark.sql.parquet.datetimeRebaseModeInRead", "CORRECTED"],
]
DIR = os.path.dirname(os.path.realpath(__file__))

P = ParamSpec("P")
A = TypeVar("A")
B = TypeVar("B")


#
# Enums
#
class DataType(str, Enum):
    CSV = ".csv"
    PARQUET = ".parquet"
    JSON = ".json"
    JDBC = "jdbc"
    DELTA = "delta"
    NONE = None

    def __str__(self):
        return self.value


class SaveMode(str, Enum):
    OVERWRITE = "overwrite"
    APPEND = "append"
    IGNORE = "ignore"

    def __str__(self):
        return self.value


#
# Classes
#
class Input:
    def __init__(
        self, path: str, filtered_columns: List[str] = ['*'], data_type=DataType.PARQUET, schema: Optional[T.StructType] = None, merge_schema=True, spark_session=None, **kwargs
    ) -> "Input":
        self.merge_schema = merge_schema
        self.data_type = data_type
        self.spark = spark_session
        self.filtered_columns = filtered_columns
        self.schema = schema
        self.path = path
        self.header = kwargs.get('header', True)
        self.infer_schema = kwargs.get('delimeter', True)
        self.delimeter = kwargs.get('delimeter', ',')
        self.encoding = kwargs.get('encoding', 'utf-8')

    def get_path(self) -> str:
        return self.path

    def add_spark_session(self, spark_session: SparkSession) -> None:
        self.spark = spark_session

    def read_dataframe(self) -> DataFrame:
        if DataType.CSV == self.data_type:
            df = self.spark.read.csv(self.path, inferSchema=self.infer_schema, header=self.header, sep=self.delimeter, encoding=self.encoding)
            df = df if self.schema is None else self.spark.createDataFrame(data=df.rdd, schema=self.schema)
        if DataType.JSON == self.data_type:
            df = self.spark.read.json(self.path, schema=self.schema)
        if DataType.PARQUET == self.data_type:
            df = self.spark.read.parquet(self.path, mergeSchema=self.merge_schema)
        if DataType.DELTA == self.data_type:
            df = self.spark.read.format('delta').load(self.path, mergeSchema=self.merge_schema)
        if DataType.JDBC == self.data_type:
            df = (
                self.spark.read
                .jdbc(
                    url='jdbc:postgresql://192.168.0.199:5432/postgres',
                    table=self.path,
                    properties={"driver": "org.postgresql.Driver", "user": "postgres", "password": "291122"}
                )
            )

        return df.select(*self.filtered_columns)


class Output(Input):
    def __init__(
        self, path: str, partition_by: Optional[str | List[str]] = None, build_datetime=True, save_mode=SaveMode.APPEND, **kwargs
    ) -> "Output":
        super().__init__(path, **kwargs)
        self.build_datetime = build_datetime
        self.partition_by = partition_by
        self.save_mode = save_mode
        self.overwriteSchema = kwargs.get('overwriteSchema', True if self.save_mode == SaveMode.OVERWRITE else False)

    def _datetime_now(self) -> str:
        return datetime.now().strftime("%Y-%m-%d %H:%M:%S")

    def _new_partition_keys(self, keys: Optional[str | List[str]], add_key: str) -> str | List[str]:
        if keys is None:
            return add_key
        if isinstance(keys, str):
            return list({keys, add_key})
        return list({*keys, add_key})

    def write_dataframe(self, df: DataFrame) -> None:
        if self.build_datetime:
            df = df.withColumn("build_datetime", F.lit(self._datetime_now()).cast(T.TimestampType()))
            self.partition_by = self._new_partition_keys(self.partition_by, "build_datetime")

        if DataType.CSV == self.data_type:
            df.write.mode(self.save_mode).csv(self.path, header=True)
        if DataType.JSON == self.data_type:
            df.write.mode(self.save_mode).json(self.path)
        if DataType.PARQUET == self.data_type:
            df.write.mode(self.save_mode).parquet(self.path, partitionBy=self.partition_by)
        if DataType.DELTA == self.data_type:
            df.write.format('delta').save(self.path, mode=self.save_mode, overwriteSchema=self.overwriteSchema, partitionBy=self.partition_by)

    def read_df(self) -> DataFrame:
        try:
            return super().read_df()
        except BaseException:
            return self.spark.createDataFrame(data=self.spark.sparkContext.emptyRDD(), schema=T.StructType([]))


#
# Decorators
#
def path_check(**dataframes: Dict[str, Input | Output]) -> None:
    paths = {}
    for key in dataframes:
        _df_type = "Input" if type(dataframes[key]) is Input else "Output"
        _path = dataframes[key].get_path()
        if _path in paths:
            if _df_type == paths[_path]:
                raise BaseException(f"Multiple Inputs Collision: {_path}")
            else:
                raise BaseException(f"Cicling Dependency Error: {_path}")

        paths[_path] = _df_type


def path_check_2(**dataframes: Dict[str, Input | Output]) -> None:
    _paths = set()
    for key in dataframes:
        _path = dataframes[key].get_path()
        if _path in _paths:
            raise BaseException(f"Dependency Error On Path: {_path}")

        _paths.add(_path)


# for use with docker set -> master = "spark://spark-master:7077"
def configure(conf="DEFAULT_SPARK_CONF", master="local[*]", auto=True):
    def configure_func(func):
        @wraps(func)
        def wrapped() -> SparkSession:
            os.makedirs('/tmp/spark-events', exist_ok=True)

            spark_session = (
                SparkSession.builder.config(conf=(SparkConf().setAppName("My-Spark-Application").setMaster(master).setAll(DEFAULT_SPARK_CONF)))
            ).getOrCreate()
            spark_session.sparkContext.addFile(DIR, recursive=True)
            return func(spark_session)
        if auto:
            return wrapped()
        return wrapped
    return configure_func


def transform(**dataframes: Dict[str, Input | Output]):
    def transform_func(func: Callable[[Optional[SparkSession], Dict[str, Input | Output]], None]):
        @wraps(func)
        def wrapped(spark_session: Optional[SparkSession] = None):
            path_check(**dataframes)
            for key in dataframes:
                dataframes[key].add_spark_session(spark_session)

            try:
                func(spark_session, **dataframes)
            except TypeError:
                func(**dataframes)

        return wrapped

    return transform_func


def transform_df(output: Output, **dataframes: Dict[str, Input]):
    def transform_df_func(func: Callable[[Optional[SparkSession], Dict[str, DataFrame]], Optional[DataFrame]]):
        @wraps(func)
        def wrapped(spark_session: Optional[SparkSession] = None):
            path_check(**(dataframes | {"_": output}))

            for key in dataframes:
                dataframes[key].add_spark_session(spark_session)
                dataframes[key] = dataframes[key].read_df()

            dataframe=None
            try:
                dataframe = func(spark_session, **dataframes)
            except TypeError:
                dataframe = func(**dataframes)
            finally:
                if dataframe is not None:
                    output.write_dataframe(dataframe)

        return wrapped

    return transform_df_func


def time_it(parallel = False):
    def wrapper(func):
        return TimeIt(func, parallel)
    return wrapper

class TimeIt:
    def __init__(self, func: Callable, parallel) -> None:
        self.parallel = parallel
        self.func = func
        self.timed = []

    def __call__(self, *args: Any, **kwds: Any) -> Any:
        # if not self.parallel:
        #     return self.func(*args, **kwds)
        start = time.perf_counter()
        res = self.func(*args, **kwds)
        end = time.perf_counter()
        self.timed.append(end - start)
        return res

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_value, exc_traceback):
        # TODO: report `exc_*` if an exception get raised
        print(f"Finished {self.func.__name__} in {sum(self.timed):.2f} secs")
        return

    def __del__(self):
        print(f"Finished {self.func.__name__} in {sum(self.timed):.2f} secs")
