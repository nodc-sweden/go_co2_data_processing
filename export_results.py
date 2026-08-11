import os
import pandas as pd

from file_reader import merge_ferrybox_and_go


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


def export_fco2_ch4(
        df: pd.DataFrame,
        start_date: str,
        end_date: str,
        has_ch4: bool):

    # select columns
    export_columns = [
        'Year',
        'Month',
        'Day',
        'Hour',
        'Minute',
        'Latitude',
        'QF Latitude',
        'Longitude',
        'QF Longitude',
        'SST',
        'SSS',
        'pco2_wet_sst',
        'fco2_wet_sst',
        'pco2_wet_atm',
        'fco2_wet_atm',
    ]
    # add units
    units = {
        'Year': '',
        'Month': '',
        'Day': '',
        'Hour': '',
        'Minute': '',
        'Latitude': '°N',
        'QF Latitude': '',
        'Longitude': '°E',
        'QF Longitude': '',
        'SST': '°C',
        'SSS': '',
        'pco2_wet_sst': 'μatm',
        'fco2_wet_sst': 'μatm',
        'pco2_wet_atm': 'μatm',
        'fco2_wet_atm': 'μatm',
    }
    round_columns = [
        'pco2_wet_sst',
        'fco2_wet_sst',
        'pco2_wet_atm',
        'fco2_wet_atm',
    ]
    if has_ch4:
        export_columns.extend([
            'pch4_wet_sst',
            'ch4_nmol_kg',
            'pch4_wet_atm',
        ])
        units.update({
        'pch4_wet_sst': 'natm',
        'dissolved_ch4_concentration': 'nmol/kg',
        'pch4_wet_atm': 'natm',
        })
        round_columns.extend([
            'pch4_wet_sst',
            'pch4_wet_atm',
        ])
        filtered_df = df.loc[(
            df['fco2_wet_atm'].notna() |
            df['fco2_wet_sst'].notna() |
            df['pch4_wet_sst'].notna() |
            df['ch4_nmol_kg'].notna() |
            df['pch4_wet_atm'].notna()
        ), export_columns]
        filtered_df = filtered_df.rename(columns={
            'ch4_nmol_kg': 'dissolved_ch4_concentration'
        })
        filtered_df.loc[1:, 'dissolved_ch4_concentration'] = filtered_df.loc[1:, 'dissolved_ch4_concentration'].round(4)
        filename = f"Tavastland_CO2_CH4_data_{start_date}_to_{end_date}.txt"
    else:
        filtered_df = df.loc[(
                df['fco2_wet_atm'].notna() |
                df['fco2_wet_sst'].notna()
        ), export_columns]
        filename = f"Tavastland_CO2_data_{start_date}_to_{end_date}.txt"

    # round pCO2, fCO2, pCH4, C to 1 decimal
    filtered_df.loc[1:, round_columns] = filtered_df.loc[1:, round_columns].round(1)

    # add units
    units_row = pd.DataFrame([units], columns=filtered_df.columns)
    df_to_export = pd.concat([units_row, filtered_df], ignore_index=True)

    # export
    export_path = os.path.join(get_data_path(), filename)
    df_to_export.to_csv(export_path, sep='\t', index=False)
    return

def export_ferrybox_with_fco2_ch4(df: pd.DataFrame, df_fb: pd.DataFrame, start_str: str, end_str: str, has_ch4: bool):

    # select co2 and ch4 data to add to ferrybox data
    filter_columns = ['pco2_wet_sst', 'fco2_wet_sst', 'pco2_wet_atm', 'fco2_wet_atm', 'time series']
    round_columns = [
        'pco2_wet_sst',
        'fco2_wet_sst',
        'pco2_wet_atm',
        'fco2_wet_atm',
    ]
    export_columns = ['Time_series', 'Latitude', 'QF Latitude', 'Longitude', 'QF Longitude', 'SST', 'QF SST', 'SSS',
                      'QF SSS', 'Air_temperature', 'QF Air_temperature', 'Atm_pressure', 'QF Atm_pressure', 'QFF',
                      'QF QFF', 'CDOM', 'QF CDOM', 'Phycocyanin', 'QF Phycocyanin', 'O2', 'QF O2', 'Chl_fluorescense',
                      'QF Chl_fluorescense', 'Turbidity', 'QF Turbidity', 'pco2_wet_sst', 'fco2_wet_sst',
                      'pco2_wet_atm', 'fco2_wet_atm',]
    units_merged = {
        'Time_series': '',
        'Latitude': '°N',
        'QF Latitude': '',
        'Longitude': '°E',
        'QF Longitude': '',
        'SST': '°C',
        'QF SST': '',
        'SSS': '',
        'QF SSS': '',
        'Air_temperature': '°C',
        'QF Air_temperature': '',
        'Atm_pressure': 'hPa',
        'QF Atm_pressure': '',
        'QFF': 'hPa',
        'QF QFF': '',
        'CDOM': 'ppb',
        'QF CDOM'
        'Phycocyanin': 'μg/l',
        'QF Phycocyanin': '',
        'O2': 'ml/l',
        'QF O2': '',
        'Chl_fluorescense': 'μg/l',
        'QF Chl_fluorescense': '',
        'Turbidity': 'NTU',
        'QF Turbidity': '',
        'pco2_wet_sst': 'μatm',
        'fco2_wet_sst': 'μatm',
        'pco2_wet_atm': 'μatm',
        'fco2_wet_atm': 'μatm',
    }
    if has_ch4:
        filter_columns.extend([
            'pch4_wet_sst',
            'ch4_nmol_kg',
            'pch4_wet_atm',
        ])
        filtered_df = df.loc[(
                                 df['fco2_wet_atm'].notna() |
                                 df['fco2_wet_sst'].notna() |
                                 df['pch4_wet_sst'].notna() |
                                 df['ch4_nmol_kg'].notna() |
                                 df['pch4_wet_atm'].notna()
        ),filter_columns]
        filtered_df = filtered_df.rename(columns={
            'ch4_nmol_kg': 'dissolved_ch4_concentration'
        })
        filtered_df.loc[1:, 'dissolved_ch4_concentration'] = filtered_df.loc[1:, 'dissolved_ch4_concentration'].round(4)
        round_columns.extend([
            'pch4_wet_sst',
            'pch4_wet_atm',
        ])
        export_columns.extend([
            'pch4_wet_sst',
            'dissolved_ch4_concentration',
            'pch4_wet_atm',
        ])
        units_merged.update({
            'pch4_wet_sst': 'natm',
            'dissolved_ch4_concentration': 'nmol/kg',
            'pch4_wet_atm': 'natm',
        })
    else:
        filtered_df = df.loc[(
            df['fco2_wet_atm'].notna() |
            df['fco2_wet_sst'].notna()
        ), filter_columns]

    # merge datasets
    merged_df = merge_ferrybox_and_go(df_fb, filtered_df)
    merged_df = merged_df.drop(columns=["time series"])

    # round CO2, CH4 to 1 decimal
    merged_df.loc[1:, round_columns] = merged_df.loc[1:, round_columns].round(1)

    # select columns to export and add units
    units_merged_row = pd.DataFrame([units_merged], columns=export_columns)
    merged_df_to_export = pd.concat([units_merged_row, merged_df], ignore_index=True)

    # export
    filename = f"Tavastland_underway_data_{start_str}_to_{end_str}.txt"
    export_path = os.path.join(get_data_path(), filename)
    merged_df_to_export.to_csv(export_path, sep='\t', index=False)
    return
