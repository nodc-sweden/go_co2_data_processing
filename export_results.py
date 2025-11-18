import os
import pandas as pd


def get_home_path():
    return os.path.dirname(os.path.abspath(__file__))


def get_figure_path():
    figure_folder = os.path.join(get_home_path(), "exported_figures")
    os.makedirs(figure_folder, exist_ok=True)
    return figure_folder


def get_data_path():
    data_folder = os.path.join(get_home_path(), "exported_data")
    os.makedirs(data_folder, exist_ok=True)
    return data_folder


def export_fco2(df: pd.DataFrame, start_date: str, end_date: str):
    export_columns = ['Year', 'Month', 'Day', 'Hour', 'Minute', 'Latitude', 'Longitude', 'SST', 'SSS',
                      'pco2_wet_sst', 'fco2_wet_sst', 'pco2_wet_atm', 'fco2_wet_atm',
                      ]
    units = {
        'Year': '',
        'Month': '',
        'Day': '',
        'Hour': '',
        'Minute': '',
        'Latitude': '°N',
        'Longitude': '°E',
        'SST': '°C',
        'SSS': '',
        'pco2_wet_sst': 'μatm',
        'fco2_wet_sst': 'μatm',
        'pco2_wet_atm': 'μatm',
        'fco2_wet_atm': 'μatm',
    }

    filtered_df = df.loc[df['fco2_wet_atm'].notna() | df['fco2_wet_sst'].notna(), export_columns]
    filename = f"Tavastland_fCO2_data_{start_date}_to_{end_date}.txt"
    export_path = os.path.join(get_data_path(), filename)
    filtered_df.to_csv(export_path, sep='\t', index=False)

    units_fb = {
        'CDOM': 'ppb',
        'Phycocyanin': 'μg/l',
        'O2': 'ml/l',
        'Chl_fluorescense': 'μg/l',
        'Turbidity': 'NTU'
    }


    return
