import pandas as pd
import numpy as np

import geopandas as gpd
from shapely.geometry import Point, LineString

from datetime import datetime


def get_type_flags(df: pd.DataFrame):
    if df['Type'].str.contains("SLEEP").any():
        df['is_sleep'] = df.Type == 'GO TO SLEEP'
    if df['Type'].str.contains("WAKE").any():
        df['is_wake'] = df.Type == 'WAKE UP'
    if df['Type'].str.contains("STD1").any():
        df['is_std1_z'] = (df.Type == 'STD1z') | (df.Type == 'STD1z-DRAIN')
        df['is_std1'] = (df.Type == 'STD1') | (df.Type == 'STD1-DRAIN')
    if df['Type'].str.contains("STD2").any():
        df['is_std2'] = (df.Type == 'STD2') | (df.Type == 'STD2-DRAIN')
        df['is_std2_s'] = (df.Type == 'STD2s') | (df.Type == 'STD2s-DRAIN')
    if df['Type'].str.contains("STD3").any():
        df['is_std3'] = (df.Type == 'STD3') | (df.Type == 'STD3-DRAIN')
        df['is_std3_s'] = (df.Type == 'STD3s') | (df.Type == 'STD3s-DRAIN')
    if df['Type'].str.contains("STD4").any():
        df['is_std4'] = (df.Type == 'STD4') | (df.Type == 'STD4-DRAIN')
        df['is_std4_s'] = (df.Type == 'STD4s') | (df.Type == 'STD4s-DRAIN')
    if df['Type'].str.contains("STD5").any():
        df['is_std5'] = (df.Type == 'STD5') | (df.Type == 'STD5-DRAIN')
        df['is_std5_s'] = (df.Type == 'STD5s') | (df.Type == 'STD5s-DRAIN')
    df['is_equ'] = (df.Type == 'EQU') | (df.Type == 'EQU-DRAIN')
    df['is_atm'] = (df.Type == 'ATM') | (df.Type == 'ATM-DRAIN')
    return df


def geographic_check(df: pd.DataFrame):
    """
    Flag GPS positions that fall within predefined non-ocean areas.

    True  = acceptable ocean position
    False = position is in/near a harbour, canal, etc.
    """

    if 'QF ocean' not in df.columns:
        df['QF ocean'] = True

    points = gpd.GeoDataFrame(
        df[['Latitude', 'Longitude']].copy(),
        geometry=gpd.points_from_xy(
            df['Longitude'],
            df['Latitude']
        ),
        index=df.index,
        crs='EPSG:4326'
    )

    valid_points = points[
        points.geometry.notna()
        & points.geometry.is_valid
        & points['Latitude'].between(-90, 90)
        & points['Longitude'].between(-180, 180)
    ].copy()

    # ---------------------------------------------------------
    # Define areas that should NOT count as ocean
    # ---------------------------------------------------------
    not_ocean = []

    # ---------------------------------------------------------
    # Umeå harbour
    # ---------------------------------------------------------
    umea = Point(20.346, 63.698)
    not_ocean.append(umea.buffer(0.03))

    # ---------------------------------------------------------
    # Zeebrugge / Bruges harbour
    # ---------------------------------------------------------
    zeebrugge = Point(3.195, 51.357)
    not_ocean.append(zeebrugge.buffer(0.02))

    # ---------------------------------------------------------
    # Kiel Canal
    # ---------------------------------------------------------
    kiel_canal_coords = [
        (9.14, 53.89),   # Brunsbüttel
        (9.286, 53.99), #9.283
        (9.350, 54.144),
        (9.581, 54.213),
        (9.641, 54.284),
        (9.701, 54.2991),
        (9.72, 54.325),
        (9.785, 54.36),
        (9.850, 54.364),
        (9.93, 54.3425),
        (9.98, 54.3442),
        (10.02, 54.3593),
        (10.06, 54.3594),
        (10.08, 54.369),
        (10.140, 54.370),  # Kiel-Holtenau
    ]

    kiel_canal = LineString(kiel_canal_coords)
    not_ocean.append(kiel_canal.buffer(0.02))


    not_ocean_areas = gpd.GeoDataFrame(
        geometry=not_ocean,
        crs='EPSG:4326'
    )

    not_ocean_areas = not_ocean_areas.union_all()
    is_not_ocean = valid_points.geometry.within(not_ocean_areas)

    df.loc[valid_points.index, 'QF ocean'] &= ~is_not_ocean
    num_of_not_ocean = (~df['QF ocean']).sum()
    print('Geographic QC')
    print(f'Number of harbour/channel points: {num_of_not_ocean}')

    return df


def range_check(df: pd.DataFrame, has_ch4: bool):
    # Create QC flags
    df['QF period'] = df['time series'] > datetime.strptime('20120413150000', '%Y%m%d%H%M%S')
    df['QF H2O flow'] = (df['H2O flow'] > 1.5) & (df['H2O flow'] < 5)
    df['QF equ temp'] = ((df['equ temp'] > -2) & (df['equ temp'] < 40) & (df['delta temperature'] > 0) &
                         (df['delta temperature'] < 2))
    df['QF delta temperature'] = df['QF equ temp']
    df['QF licor press'] = (df['licor press'] > 900) & (df['licor press'] < 1100)  # Pierrot 900-1040
    df['QF lab press'] = (df['lab press'] > 900) & (df['lab press'] < 1100)  # Pierrot 900-1040
    df['QF equ press'] = (df['equ press'] > -0.5) & (df['equ press'] < 0.5)  # original range -0.5 och 0.5
    df['QF licor flow'] = (df['licor flow'] > 20) & (df['licor flow'] < 500)  # Pierrot
    df['QF vent flow'] = (df['vent flow'] > -5) & (
                df['vent flow'] < 25)  # typical value about 20 ml/min to replace lost air.
    df['QF CO2 ppm'] = (df['CO2 ppm'] > 80) & (df['CO2 ppm'] < 1200)
    df['QF CO2 avg ppm'] = (df['CO2 avg ppm'] > 80) & (df['CO2 avg ppm'] < 1200)
    if has_ch4:
        df['QF CH4 ppb'] = (df['CH4 ppb'] > 100) & (df['CH4 ppb'] < 1000000)  # perhaps 500000 ppb
        df['QF CH4 avg ppb'] = (df['CH4 avg ppb'] > 100) & (df['CH4 avg ppb'] < 1000000)  # perhaps 500000 ppb
    return df


def constant_value(df: pd.DataFrame, has_ch4: bool):
    parameters = [
        'equ temp',
        'licor press',
        'lab press',
        'equ press',
        'CO2 ppm',
        'CO2 avg ppm',
    ]
    if has_ch4:
        parameters.extend([
            'CH4 ppb',
            'CH4 avg ppb',
        ])
    time_series = df['time series']
    for item in parameters:
        series = df[item]
        is_change = series != series.shift()
        group = is_change.cumsum()
        df_grouped = pd.DataFrame({
            'time': time_series,
            'group': group,
            'value': series
        })

        group_durations = df_grouped.groupby('group').agg(
            start_time=('time', 'min'),
            end_time=('time', 'max'),
            value=('value', 'first')
        )
        group_durations['duration_min'] = (group_durations['end_time'] - group_durations[
            'start_time']).dt.total_seconds() / 60
        long_constant_groups = group_durations[group_durations['duration_min'] > 120].index

        is_constant = group.isin(long_constant_groups) & series.notna()
        first_in_group = df.loc[is_constant].groupby(group).head(1).index
        is_constant.loc[first_in_group] = False
        qf_col = f'QF {item}'
        df[qf_col] = df[qf_col] & ~is_constant
        print('Constant value')
        print(item)
        print('Number of bad data points: ')
        print(sum(is_constant))
    return df


def get_outliers(df: pd.DataFrame, parameter: str, selected: str, window: int = 4320, method: str = 'std'):
    selection = pd.Series(True, index=df.index) if selected == 'all_data' else df[selected]
    temp = df[parameter].where(selection)
    rolling_center = temp.rolling(f'{window}min', center=True)
    if method == 'mad':
        rolling_median = rolling_center.median()
        deviation = np.abs(temp - rolling_median)
        smad = 1.4826 * rolling_center.apply(
            lambda x: np.nan if np.all(np.isnan(x)) else np.nanmedian(np.abs(x - np.nanmedian(x))),
            raw=True
        )
        threshold = 5 * smad
    else:
        rolling_mean = rolling_center.mean()
        rolling_std = rolling_center.std()
        deviation = np.abs(temp - rolling_mean)
        threshold = 5 * rolling_std
    is_outlier = deviation > threshold

    df[f'QF {parameter}'] &= ~is_outlier
    print('Outlier test')
    print(parameter)
    print(selected)
    print('Number of bad data points: ')
    print(sum(is_outlier))
    return df


def outlier_check(df: pd.DataFrame, has_ch4: bool):
    # Default is three days to match a typical cruise length with M/V Tavastland. With R/V Kronprins Haakon a window of
    # 20 min was applied, since hard weather conditions resulted in noise in many measured parameters. In the latter
    # case sMAD was used, but for M/V Tavastland the std over a larger window will likely be sufficient.
    parameters = [
        'equ temp',
        'licor press',
        'lab press',
        'equ press',
        'CO2 ppm',
        'CO2 ppm',
        'CO2 avg ppm',
        'CO2 avg ppm',
    ]
    if has_ch4:
        parameters.extend([
            'CH4 ppb',
            'CH4 ppb',
            'CH4 avg ppb',
            'CH4 avg ppb',
        ])

    selections = [
        'all_data',
        'all_data',
        'all_data',
        'all_data',
        'is_atm',
        'is_equ',
        'is_atm',
        'is_equ',
    ]
    if has_ch4:
        selections.extend([
            'is_atm',
            'is_equ',
            'is_atm',
            'is_equ',
        ])
    df = df.set_index('time series')
    for item, selected in zip(parameters,selections):
        df = get_outliers(df, item, selected, 4320, 'std')
    df = df.reset_index()
    return df


def gradient_check(df: pd.DataFrame, has_ch4: bool):
    parameters = [
        'equ temp',
        'licor press',
        'lab press',
        'equ press',
        'CO2 ppm',
        'CO2 avg ppm',
    ]
    if has_ch4:
        parameters.extend([
            'CH4 ppb',
            'CH4 avg ppb',
        ])
    thresholds = [3, 50, 50, 2, 20, 20, 2000, 2000]
    type_sensitive_parameters = [
        'equ press',
        'CO2 ppm',
        'CO2 avg ppm',
    ]
    if has_ch4:
        type_sensitive_parameters.extend([
            'CH4 ppb',
            'CH4 avg ppb',
        ])
    for item, threshold in zip(parameters, thresholds):
        v = df[item]
        type_v = df['Type']
        time_v = df['time series']
        v1 = v.shift(1)
        type_v1 = type_v.shift(1)
        time_v1 = time_v.shift(1)
        v3 = v.shift(-1)
        type_v3 = type_v.shift(-1)
        time_v3 = time_v.shift(-1)

        is_nan = v1.isna() | v.isna() | v3.isna()
        unacceptable_time_diff = (time_v3 - time_v1).dt.total_seconds() > 300
        not_type = (type_v1 != type_v) | (type_v3 != type_v)
        gradient = (v - (v1 + v3) / 2).abs()
        if item in type_sensitive_parameters:
            is_gradient = (gradient < threshold) | is_nan | not_type | unacceptable_time_diff
        else:
            is_gradient = (gradient < threshold) | is_nan | unacceptable_time_diff
        df[f'QF {item}'] &= is_gradient
        print('Gradient test')
        print(item)
        print('Number of bad data points: ')
        print(sum(~is_gradient))
    return df
