import sys
import os
import re
import requests
from pyspark.sql import types as T
from pyspark.sql import functions as F
from pyspark.sql import SparkSession

sys.path.append(os.path.join(os.path.dirname(os.path.abspath(__file__)), os.pardir))
sys.path.append(os.path.join(os.path.dirname(os.path.abspath(__file__)), os.pardir, os.pardir))
from src.transform_api import configure, transform, Input, Output, DataType, SaveMode

def count_by_usage(source_df, usage):
    df_usage = source_df.sample(fraction=usage).groupBy('municipalityName').agg(
        F.sum('kwh_per_year_for_1_panel').alias('1_panel_total_mwh'),
        F.sum('kwh_per_year_for_4_panel').alias('4_panel_total_mwh'),
        F.sum('kwh_per_year_for_8_panel').alias('8_panel_total_mwh'),
        F.sum('kwh_per_year_for_10_panel').alias('10_panel_total_mwh'),
        F.sum('kwh_per_year_for_12_panel').alias('12_panel_total_mwh'),
    )
    return df_usage

@configure('MINIO_SPARK_CONF')
@transform(
    out_15_perctenge_usage = Output("s3a://climathon/tests/perctenge_usage_15", build_datetime=False, data_type=DataType.DELTA, save_mode=SaveMode.OVERWRITE),
    out_30_perctenge_usage = Output("s3a://climathon/tests/perctenge_usage_30", build_datetime=False, data_type=DataType.DELTA, save_mode=SaveMode.OVERWRITE),
    out_50_perctenge_usage = Output("s3a://climathon/tests/perctenge_usage_50", build_datetime=False, data_type=DataType.DELTA, save_mode=SaveMode.OVERWRITE),
    out_100_perctenge_usage = Output("s3a://climathon/tests/perctenge_usage_100", build_datetime=False, data_type=DataType.DELTA, save_mode=SaveMode.OVERWRITE),
    in_data = Input("s3a://climathon/tests/solar_capacity",data_type=DataType.DELTA)
)
def compute(ctx: SparkSession,
    out_15_perctenge_usage,
    out_30_perctenge_usage,
    out_50_perctenge_usage,
    out_100_perctenge_usage,
    in_data
):
    source_df = in_data.read_dataframe()
    
    out_15_perctenge_usage.write_dataframe(count_by_usage(source_df, usage=0.15))
    out_30_perctenge_usage.write_dataframe(count_by_usage(source_df, usage=0.30))
    out_50_perctenge_usage.write_dataframe(count_by_usage(source_df, usage=0.50))
    out_100_perctenge_usage.write_dataframe(count_by_usage(source_df, usage=1.0))

    
    
    return None
