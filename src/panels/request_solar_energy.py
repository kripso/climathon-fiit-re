import pvlib
import re
import requests
import pandas as pd

pv_tech_mapping = {
    'Mono-c-Si': 'crystSi',
    'Multi-c-Si': 'crystSi',
    'Standardny (PV)': 'crystSi',
    'Thin Film': 'Unknown',
    'CdTe': 'CdTe',
    'CIGS': 'CIS',
}

zse_panel_params = {
    'kwp': 0.346,
    'pv_tech': pv_tech_mapping['Mono-c-Si']
}

CECMODS = pvlib.pvsystem.retrieve_sam(name='CecMod')
eu_approved_solar_panels = pd.read_csv('data/raw/approved_solar_panels.csv')



def get_cecmod_pv_energy_per_panel_yearly_kwh(lat, lon, pv_panel_model_name):

    pv_panel_model_name = re.sub(r'\(.*\) ', '', pv_panel_model_name)
    pv_panel_model_name = re.sub(r'[\.\-\s]', '_', pv_panel_model_name)

    bool_map = CECMODS.T.index.str.startswith(pv_panel_model_name)
    model_found = CECMODS.T[bool_map]
    
    kwp = zse_panel_params['kwp']
    pv_tech = zse_panel_params['pv_tech']
    if model_found.size:
        model_found = model_found.iloc[0]
        kwp = model_found['STC'] / 1000
        pv_tech = pv_tech_mapping[model_found['Technology']]
    
    url = f'https://re.jrc.ec.europa.eu/api/v5_2/PVcalc?outputformat=basic&lat={lat}&lon={lon}&raddatabase=PVGIS-SARAH2&browser=0&peakpower={kwp}&loss=14&mountingplace=building&pvtechchoice={pv_tech}&optimalinclination=1&aspect=0&usehorizon=1&userhorizon=&js=1'
    response = requests.get(url)
    response_body = response.content.decode("utf-8")

    yearly_energy_match = re.search(r'(Year\s+)(\d+.\d+)', response_body)
    if yearly_energy_match:
        return float(yearly_energy_match.group(2)) / 1000
    else:
        return -1

print(get_pv_energy_per_panel_yearly_kwh(48.152, 17.182, pv_panel_model_name='unknown'))