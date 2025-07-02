import pandas as pd
import numpy as np
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


def range_check(df: pd.DataFrame):
    # Create QC flags
    df['QF period'] = df['time series'] > datetime.strptime('20120413150000', '%Y%m%d%H%M%S')
    df['QF H2O flow'] = (df['H2O flow'] > 1.5) & (df['H2O flow'] < 5)
    df['QF equ temp'] = ((df['equ temp'] > -2) & (df['equ temp'] < 40) & (df['delta temperature'] > 0) &
                         (df['delta temperature'] < 2))
    df['QF delta temperature'] = df['QF equ temp']
    df['QF licor press'] = (df['licor press'] > 900) & (df['licor press'] < 1100)
    df['QF lab press'] = (df['lab press'] > 900) & (df['lab press'] < 1100)
    df['QF equ press'] = (df['equ press'] > -0.5) & (df['equ press'] < 0.5)  # original range -0.5 och 0.5
    df['QF licor flow'] = (df['licor flow'] > 20) & (df['licor flow'] < 500)
    df['QF vent flow'] = (df['vent flow'] > -5) & (
                df['vent flow'] < 25)  # typical value about 20 ml/min to replace lost air.
    df['QF CO2 ppm'] = (df['CO2 ppm'] > 80) & (df['CO2 ppm'] < 1200)
    df['QF CO2 avg ppm'] = (df['CO2 avg ppm'] > 80) & (df['CO2 avg ppm'] < 1200)
    return df


def constant_value(df: pd.DataFrame):
    parameters = ['equ temp', 'licor press', 'lab press', 'equ press', 'CO2 ppm', 'CO2 avg ppm']
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


def outlier_check(df: pd.DataFrame):
    # Default is three days to match a typical cruise length with M/V Tavastland. With R/V Kronprins Haakon a window of
    # 20 min was applied, since hard weather conditions resulted in noise in many measured parameters. In the latter
    # case sMAD was used, but for M/V Tavastland the std over a larger window will likely be sufficient.
    parameters = ['equ temp', 'licor press', 'lab press', 'equ press', 'CO2 ppm', 'CO2 ppm', 'CO2 avg ppm',
                  'CO2 avg ppm']
    all_data = pd.Series(True, index=df.index)
    selections = ['all_data', 'all_data', 'all_data', 'all_data','is_atm', 'is_equ', 'is_atm', 'is_equ']
    df = df.set_index('time series')
    for item, selected in zip(parameters,selections):
        df = get_outliers(df, item, selected, 4320, 'std')
    df = df.reset_index()
    return df


def gradient_check(df: pd.DataFrame):
    parameters = ['equ temp', 'licor press', 'lab press', 'equ press', 'CO2 ppm', 'CO2 avg ppm']
    thresholds = [3, 50, 50, 2, 20, 20]
    type_sensitive_parameters = ['equ press', 'CO2 ppm', 'CO2 avg ppm']
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
