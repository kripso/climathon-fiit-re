import sys
import os
from pyspark.sql import types as T
from pyspark.sql import functions as F
from pyspark.sql import SparkSession

sys.path.append(os.path.join(os.path.dirname(os.path.abspath(__file__)), os.pardir, os.pardir))
from src.transform_api import configure, transform, Input, Output, DataType, SaveMode

# schema = https://data.gov.sk/dataset/db1e7bd7-a7d4-4eac-8e5e-0c843b9dc7d5/resource/05092c06-2d8b-4297-abf9-711c49ac6d74/download/doc07.html

@configure('MINIO_SPARK_CONF')
@transform(
    out_entrances = Output("s3a://climathon/data_gov_vchody", build_datetime=False, data_type=DataType.DELTA, save_mode=SaveMode.OVERWRITE),
    in_entrances = Input("/config/workspace/climathon-fiit-re/data/raw/data_gov_vchody.fs.csv", data_type=DataType.CSV),
)
def compute(
    ctx: SparkSession,
    out_entrances,
    in_entrances,
):

    df = (
        in_entrances
        .read_df()
        .withColumn('validTo', F.when(F.col('validTo') == F.lit('None'), F.lit(None)).otherwise(F.to_timestamp(F.col('validTo'))))
        .withColumn('validFrom', F.when(F.col('validFrom') == F.lit('None'), F.lit(None)).otherwise(F.to_timestamp(F.col('validFrom'))))
        .withColumn('verifiedAt', F.when(F.col('verifiedAt') == F.lit('None'), F.lit(None)).otherwise(F.to_timestamp(F.col('verifiedAt'))))
        .withColumn('changedAt', F.when(F.col('changedAt') == F.lit('None'), F.lit(None)).otherwise(F.to_timestamp(F.col('changedAt'))))
        
    )
    df.show()
    out_entrances.write_dataframe(df)

