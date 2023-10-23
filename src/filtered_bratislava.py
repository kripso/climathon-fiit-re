import sys
import os
from pyspark.sql import types as T
from pyspark.sql import functions as F
from pyspark.sql import SparkSession

sys.path.append(os.path.join(os.path.dirname(os.path.abspath(__file__)), os.pardir))
sys.path.append(os.path.join(os.path.dirname(os.path.abspath(__file__)), os.pardir, os.pardir))
from src.transform_api import configure, transform_df, Input, Output, DataType, SaveMode


@configure('MINIO_SPARK_CONF')
@transform_df(
    Output("s3a://climathon/tests/data_gov", build_datetime=False, data_type=DataType.DELTA, save_mode=SaveMode.OVERWRITE),
    in_houses = Input("s3a://climathon/data_gov_budovy", data_type=DataType.DELTA),
    in_entrances = Input("s3a://climathon/data_gov_vchody", data_type=DataType.DELTA),
    in_addresses = Input("s3a://climathon/data_gov_obce", data_type=DataType.DELTA),
)
def compute(ctx: SparkSession, in_houses, in_entrances, in_addresses):

    in_houses = (
        in_houses
        .select(
            F.col("objectId").alias("houseObjectId"),
            "buildingTypeName",
            "changedAt",
            # "buildingTypeCode",
            "propertyRegistrationNumber",
            # "buildingTypeCodelistCode",
            "municipalityIdentifier",
            # "districtIdentifier",
        )
    )

    in_entrances = (
        in_entrances
        .select(
            F.col("objectId").alias("entranceObjectId"),
            # "buildingNumber",
            # "buildingIndex",
            "postalCode",
            "axisB",
            "axisL",
            "propertyRegistrationNumberIdentifier",
            # "streetNameIdentifier",
        )
        .withColumn("axisB", F.when(F.col('axisB')==F.lit('None'), F.lit(None)).otherwise(F.col('axisB')))
        .withColumn("axisL", F.when(F.col('axisL')==F.lit('None'), F.lit(None)).otherwise(F.col('axisL')))
    )

    in_addresses = (
        in_addresses
        .select(
            F.col("objectId").alias("addressObjectId"),
            # "municipalityCode",
            "municipalityName",
            # "countyIdentifier",
            # "status",
            # "cityIdentifier",
        )
    )
    # in_houses.show()
    # in_entrances.show(n=1000)

    df = (
        in_entrances
        .join(in_houses, on=[in_entrances.propertyRegistrationNumberIdentifier == in_houses.houseObjectId], how='inner')
        .join(in_addresses, on=[in_houses.municipalityIdentifier == in_addresses.addressObjectId], how='inner')
        .drop('in_entrances.propertyRegistrationNumberIdentifier', 'in_houses.municipalityIdentifier')
        .filter(F.col('axisB').isNotNull() & F.col('axisL').isNotNull())
        .sort(F.col('changedAt').desc())
        .drop_duplicates(subset=['postalCode', 'propertyRegistrationNumber', 'municipalityName', 'buildingTypeName', 'houseObjectId', 'addressObjectId'])
        # .filter(F.col('postalCode').isin(['94161','94162']))
        # .filter(F.col('propertyRegistrationNumber') == F.lit(204))
        .filter(F.col('buildingTypeName').isin(['Rodinný dom', "Iná budova"]))
        .filter(F.lower(F.col('municipalityName')).contains('bratislava'))
        .limit(25_000)
        .withColumn('area', F.lit(140))
    )

    # return df
