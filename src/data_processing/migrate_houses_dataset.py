import sys
import os
from pyspark.sql import types as T
from pyspark.sql import functions as F
from pyspark.sql import SparkSession

sys.path.append(os.path.join(os.path.dirname(os.path.abspath(__file__)), os.pardir, os.pardir))
from src.transform_api import configure, transform, Input, Output, DataType, SaveMode

# schema = https://data.gov.sk/dataset/14fae22e-96ff-4d26-82af-435f8d7a4fb6/resource/657a16ac-ac8e-4f2b-afb9-8b802424f88d/download/doc06.html

@configure('MINIO_SPARK_CONF')
@transform(
    out_houses = Output("s3a://climathon/data_gov_budovy", build_datetime=False, data_type=DataType.DELTA, save_mode=SaveMode.OVERWRITE),
    in_houses = Input("/config/workspace/climathon-fiit-re/data/raw/data_gov_budovy.fs.csv", data_type=DataType.CSV),
)
def compute(
    ctx: SparkSession,
    out_houses,
    in_houses,
):

    df = (
        in_houses
        .read_df()
        .withColumn('buildingPurposeName', F.regexp_replace('buildingPurposeName',r"Ăˇ", r"á"))
        .withColumn('changedAt', F.when(F.col('changedAt') == F.lit('None'), F.lit(None)).otherwise(F.to_timestamp(F.col('changedAt'))))
        .withColumn('validFrom', F.when(F.col('validFrom') == F.lit('None'), F.lit(None)).otherwise(F.to_timestamp(F.col('validFrom'))))
        .withColumn('validTo', F.when(F.col('validTo') == F.lit('None'), F.lit(None)).otherwise(F.to_timestamp(F.col('validTo'))))
    )
    df.show()
    out_houses.write_dataframe(df)
