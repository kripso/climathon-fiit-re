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
    out_panels = Output("s3a://climathon/approved_solar_panels", build_datetime=False, data_type=DataType.DELTA, save_mode=SaveMode.OVERWRITE),
    in_panels = Input("/config/workspace/climathon-fiit-re/data/raw/approved_solar_panels.csv", data_type=DataType.CSV),
)
def compute(
    ctx: SparkSession,
    out_panels,
    in_panels,
):

    df = (
        in_panels
        .read_df()
    )
    df.show()
    out_panels.write_dataframe(df)