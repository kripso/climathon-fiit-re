import sys
import os
from pyspark.sql import types as T
from pyspark.sql import functions as F
from pyspark.sql import SparkSession

sys.path.append(os.path.join(os.path.dirname(os.path.abspath(__file__)), os.pardir, os.pardir))
from src.transform_api import configure, transform, Input, Output, DataType, SaveMode

# schema = https://data.gov.sk/dataset/1277119c-0c20-40c5-9416-30afcc31e7b3/resource/4ddb9857-ef79-40d5-a38d-c6623a043b08/download/doc03.html

@configure('MINIO_SPARK_CONF')
@transform(
    out_addresses = Output("s3a://climathon/data_gov_obce", build_datetime=False, data_type=DataType.DELTA, save_mode=SaveMode.OVERWRITE),
    in_addresses = Input("/config/workspace/climathon-fiit-re/data/raw/data_gov_obce.fs.csv", data_type=DataType.CSV),
)
def compute(
    ctx: SparkSession,
    out_addresses,
    in_addresses,
):

    df = (
        in_addresses
        .read_df()
        .withColumn('validTo', F.when(F.col('validTo') == F.lit('None'), F.lit(None)).otherwise(F.to_timestamp(F.col('validTo'))))
        .withColumn('validFrom', F.when(F.col('validFrom') == F.lit('None'), F.lit(None)).otherwise(F.to_timestamp(F.col('validFrom'))))
        .withColumn('changedAt', F.when(F.col('changedAt') == F.lit('None'), F.lit(None)).otherwise(F.to_timestamp(F.col('changedAt'))))
        
    )
    df.show()
    out_addresses.write_dataframe(df)
