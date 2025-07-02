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


def export_fco2(df: pd.DataFrame):
    export_columns = ['time series', 'Lat combined', 'Lon combined', 'SBE38 FB', 'SBE45 Salinity FB',
                      'pco2_wet_sst', 'fco2_wet_sst', 'pco2_wet_atm', 'fco2_wet_atm']
    filtered_df = df.loc[df['fco2_wet_atm'].notna() | df['fco2_wet_sst'].notna(), export_columns]
    min_date = df['time series'].min().strftime('%Y%m%d')
    max_date = df['time series'].max().strftime('%Y%m%d')
    filename = f"Tavastland_fCO2_data_{min_date}_to_{max_date}.txt"
    export_path = os.path.join(get_data_path(), filename)
    filtered_df.to_csv(export_path, sep='\t', index=False)
    return
