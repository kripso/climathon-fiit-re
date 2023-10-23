import sys
import os
import re
import requests
from pyspark.sql import types as T
from pyspark.sql import functions as F
from pyspark.sql import SparkSession

sys.path.append(os.path.join(os.path.dirname(os.path.abspath(__file__)), os.pardir))
sys.path.append(os.path.join(os.path.dirname(os.path.abspath(__file__)), os.pardir, os.pardir))
from src.transform_api import configure, transform_df, Input, Output, DataType, SaveMode

PV_TECH_MAPPING = {
    'Mono-c-Si': 'crystSi',
    'Multi-c-Si': 'crystSi',
    'Standardny (PV)': 'crystSi',
    'Thin Film': 'Unknown',
    'CdTe': 'CdTe',
    'CIGS': 'CIS',
}

PANEL_NAME = 'Kyocera_Solar_SU53BU'
PANEL_NAME = re.sub(r'\(.*\) ', '', PANEL_NAME)
PANEL_NAME = re.sub(r'[\.\-\s]', '_', PANEL_NAME)


@F.udf(returnType=T.DoubleType())
def get_solar_capacity(lat, lon, kwp='0.346', technology='crystSi'):
    
    pv_tech = PV_TECH_MAPPING.get(technology, 'crystSi')
    
    url = f'https://re.jrc.ec.europa.eu/api/v5_2/PVcalc?outputformat=basic&lat={lat}&lon={lon}&raddatabase=PVGIS-SARAH2&browser=0&peakpower={kwp}&loss=14&mountingplace=building&pvtechchoice={pv_tech}&optimalinclination=1&aspect=0&usehorizon=1&userhorizon=&js=1'
    response = requests.get(url)
    response_body = response.content.decode("utf-8")

    yearly_energy_match = re.search(r'(Year\s+)(\d+.\d+)', response_body)
    if yearly_energy_match:
        return float(yearly_energy_match.group(2)) / 1_000
    else:
        return -1

@configure('MINIO_SPARK_CONF')
@transform_df(
    Output("s3a://climathon/tests/solar_capacity", build_datetime=False, data_type=DataType.DELTA, save_mode=SaveMode.OVERWRITE),
    source_df = Input("s3a://climathon/tests/data_gov", data_type=DataType.DELTA),
    solar_panel_data = Input("s3a://climathon/solar_panel_data", data_type=DataType.DELTA),
)
def compute(ctx: SparkSession, source_df, solar_panel_data):

    # get_pv_energy_per_panel_yearly_kwh(48.152, 17.182, PANEL_NAME='unknown')
    
    # sola_panel = (
    #     solar_panel_data
    #     .filter(F.lower(F.col('panel_name')).startswith(F.lower(F.lit(PANEL_NAME))))
    #     .withColumn('kwp', F.col('STC') / 1_000)
    #     .select('kwp', 'technology')
    #     .limit(1)
    #     .toPandas()
    # )

    panel_power = (
        source_df
        .drop_duplicates(subset=['municipalityName'])
        .withColumn('kwh_per_year_for_1_panel', get_solar_capacity(F.col('axisB'),F.col('axisL')))
        .select('municipalityName', 'kwh_per_year_for_1_panel')
        .withColumn('kwh_per_year_for_4_panel', F.col('kwh_per_year_for_1_panel')*4)
        .withColumn('kwh_per_year_for_8_panel', F.col('kwh_per_year_for_1_panel')*8)
        .withColumn('kwh_per_year_for_10_panel', F.col('kwh_per_year_for_1_panel')*10)
        .withColumn('kwh_per_year_for_12_panel', F.col('kwh_per_year_for_1_panel')*12)
    )


    df_panels_in_city_part = (
        source_df
        .join(panel_power, on=['municipalityName'], how='left')
    )

    # df_panels_in_city_part = df_panels_in_city_part.groupBy

    return df_panels_in_city_part
