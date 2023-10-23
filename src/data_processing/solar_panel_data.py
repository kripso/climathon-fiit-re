import sys
import os
import pvlib
from pyspark.sql import types as T
from pyspark.sql import functions as F
from pyspark.sql import SparkSession

sys.path.append(os.path.join(os.path.dirname(os.path.abspath(__file__)), os.pardir, os.pardir))
from src.transform_api import configure, transform, Input, Output, DataType, SaveMode

# schema = https://data.gov.sk/dataset/db1e7bd7-a7d4-4eac-8e5e-0c843b9dc7d5/resource/05092c06-2d8b-4297-abf9-711c49ac6d74/download/doc07.html

@configure('MINIO_SPARK_CONF')
@transform(
    out_solar_panels = Output("s3a://climathon/solar_panel_data", build_datetime=False, data_type=DataType.DELTA, save_mode=SaveMode.OVERWRITE),
)
def compute(
    ctx: SparkSession,
    out_solar_panels,
):
    pd_df = pvlib.pvsystem.retrieve_sam(name='CecMod').T
    pd_df.reset_index(drop=False, inplace=True)
    df = ctx.createDataFrame(pd_df)
    # df.show()
    out_solar_panels.write_dataframe(df.withColumnRenamed('index', 'panel_name'))

