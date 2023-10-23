import sys
import os
import re
import math
import requests
from pyspark.sql import types as T
from pyspark.sql import functions as F
from pyspark.sql.window import Window as W
from pyspark.sql import SparkSession

sys.path.append(os.path.join(os.path.dirname(os.path.abspath(__file__)), os.pardir))
sys.path.append(os.path.join(os.path.dirname(os.path.abspath(__file__)), os.pardir, os.pardir))
from src.transform_api import configure, transform, Input, Output, DataType, SaveMode

import random
from functools import reduce


def gen_avg(expected_avg=140, n=22, a=40, b=180):
    while True:
        l = [random.randint(a, b) for i in range(n)]
        avg = reduce(lambda x, y: x + y, l) / len(l)

        if avg == expected_avg:
            return l

AVG_VALUES = gen_avg()


@F.udf(returnType=T.IntegerType())
def get_rand_item(index):
    return AVG_VALUES[index]


@configure('MINIO_SPARK_CONF')
@transform(
    out_panel_count = Output("s3a://climathon/tests/panel_count", build_datetime=False, data_type=DataType.DELTA, save_mode=SaveMode.OVERWRITE),
    in_data = Input("s3a://climathon/tests/solar_capacity",data_type=DataType.DELTA)
)
def compute(
    ctx: SparkSession,
    out_panel_count,
    in_data
):
    window = W.orderBy("entranceObjectId")
    source_df = in_data.read_dataframe()
    source_df = (
        source_df
        .withColumn('area', get_rand_item(F.lit(F.row_number().over(window)%22)))
        .withColumn('panel_area', F.lit((1.755 + 1)*(1.038 + 1)))
        .withColumn('possible_panel_count', F.floor(F.col('area') / F.col('panel_area')))
        .withColumn('possible_kwh_per_year_whole_roof', F.col('kwh_per_year_for_1_panel') * F.col('possible_panel_count'))
    )

    source_df.show()

    # out_panel_count.write_dataframe(source_df)
    
    return None
