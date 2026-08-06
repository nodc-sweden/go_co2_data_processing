import pandas as pd
import numpy as np


def get_median_and_interpolate(has_ch4: bool, df: pd.DataFrame, standard: str):
    bool_col = f'is_std{standard}'
    co2_values = df['CO2 ppm'].copy()
    is_co2_avg = df['CO2 avg ppm'].notna()
    co2_values.loc[is_co2_avg] = df.loc[is_co2_avg, 'CO2 avg ppm']
    if has_ch4:
        ch4_values = df['CH4 ppb'].copy()
        is_ch4_avg = df['CH4 avg ppb'].notna()
        ch4_values.loc[is_ch4_avg] = df.loc[is_ch4_avg, 'CH4 avg ppb']
    if bool_col not in df.columns:
        raise ValueError(f"Column 'is_std{standard}' not found in the DataFrame.")
    df[f'median_std{standard}_co2'] = np.nan
    if has_ch4:
        df[f'median_std{standard}_ch4'] = np.nan
    bool_series_diff = df[bool_col].astype(int).diff().fillna(0)

    if df[bool_col].iloc[0] == 1:
        bool_series_diff.iloc[0] = 1

    if df[bool_col][len(df[bool_col]) - 1] == 1:
        bool_series_diff[len(df[bool_col])] = -1

    start_indices = bool_series_diff[bool_series_diff == 1].index
    end_indices = bool_series_diff[bool_series_diff == -1].index
    for start_idx, end_idx in zip(start_indices, end_indices):
        temp_values = co2_values.iloc[start_idx:end_idx].values
        if np.all(np.isnan(temp_values)):
            median_value = np.nan
        else:
            median_value = np.nanmedian(temp_values)
        df.loc[start_idx:end_idx - 1, f'median_std{standard}_co2'] = median_value
        if has_ch4:
            temp_values = ch4_values.iloc[start_idx:end_idx].values
            if np.all(np.isnan(temp_values)):
                median_value = np.nan
            else:
                median_value = np.nanmedian(temp_values)
            df.loc[start_idx:end_idx - 1, f'median_std{standard}_ch4'] = median_value

    # interpolate
    df[f'interpolated_std{standard}_co2'] = np.nan
    df[f'interpolated_std{standard}_co2'].values[~np.isnan(df[f'median_std{standard}_co2'])] = (
        df[f'median_std{standard}_co2'].values)[~np.isnan(df[f'median_std{standard}_co2'])]
    if has_ch4:
        df[f'interpolated_std{standard}_ch4'] = np.nan
        df[f'interpolated_std{standard}_ch4'].values[~np.isnan(df[f'median_std{standard}_ch4'])] = (
            df[f'median_std{standard}_ch4'].values)[~np.isnan(df[f'median_std{standard}_ch4'])]

    # CO2
    for i in range(len(start_indices) - 1):
        gap_between_std_curves = (
                                         df.loc[start_indices[i + 1], 'elapsed time (s)']
                                         - df.loc[end_indices[i] - 1, 'elapsed time (s)']
        ) > 12 * 3600
        bool_gap_between_std_curves = ((df['elapsed time (s)'] > df.loc[end_indices[i] - 1, 'elapsed time (s)']) &
                  (df['elapsed time (s)'] <= df.loc[end_indices[i] - 1, 'elapsed time (s)'] + 12 * 3600))
        if gap_between_std_curves:
            df.loc[bool_gap_between_std_curves, f'interpolated_std{standard}_co2'] = df.loc[
                end_indices[i] - 1, f'median_std{standard}_co2']
        elif (df.loc[start_indices[i + 1], f'reference_std{standard}_co2'] ==
              df.loc[end_indices[i] - 1, f'reference_std{standard}_co2']):
            df.loc[end_indices[i]:start_indices[i + 1] - 1, f'interpolated_std{standard}_co2'] = \
                (np.interp(df.loc[end_indices[i]:start_indices[i + 1] - 1, 'elapsed time (s)'].values,
                           df.loc[[end_indices[i] - 1, start_indices[i + 1]], 'elapsed time (s)'].values,
                           df.loc[[end_indices[i] - 1, start_indices[i + 1]], f'median_std{standard}_co2'].values))
        else:
            df.loc[end_indices[i]:start_indices[i + 1] - 1, f'interpolated_std{standard}_co2'] = df.loc[
                end_indices[i] - 1, f'median_std{standard}_co2']
        # CH4
        if has_ch4:
            if gap_between_std_curves:
                df.loc[bool_gap_between_std_curves, f'interpolated_std{standard}_ch4'] = df.loc[
                    end_indices[i] - 1, f'median_std{standard}_ch4']
            elif (df.loc[start_indices[i + 1], f'reference_std{standard}_ch4'] ==
                  df.loc[end_indices[i] - 1, f'reference_std{standard}_ch4']):
                df.loc[end_indices[i]:start_indices[i + 1] - 1, f'interpolated_std{standard}_ch4'] = \
                    (np.interp(df.loc[end_indices[i]:start_indices[i + 1] - 1, 'elapsed time (s)'].values,
                               df.loc[[end_indices[i] - 1, start_indices[i + 1]], 'elapsed time (s)'].values,
                               df.loc[
                                   [end_indices[i] - 1, start_indices[i + 1]], f'median_std{standard}_ch4'].values))
            else:
                df.loc[end_indices[i]:start_indices[i + 1] - 1, f'interpolated_std{standard}_ch4'] = df.loc[
                    end_indices[i] - 1, f'median_std{standard}_ch4']
    return df


def get_standard_reference_value(has_ch4: bool, df: pd.DataFrame, df_stds: pd.DataFrame):
    for _, row in df_stds.iterrows():
        standard = str(row['STD channel'])
        if df['Type'].str.contains(standard).any():
            if f'reference_{standard.lower()}_co2' not in df.columns:
                df[f'reference_{standard.lower()}_co2'] = np.nan
            if f'reference_{standard.lower()}_ch4' not in df.columns and has_ch4:
                df[f'reference_{standard.lower()}_ch4'] = np.nan
            co2_value = row['CO2 ppm']
            ch4_value = row['CH4 ppb']
            start_time = row['Start time series']
            end_time = row['End time series']
            bool_ref = (
                    (df['time series'] >= start_time) &
                    (df['time series'] <= end_time)
            )
            df.loc[bool_ref, f'reference_{standard.lower()}_co2'] = co2_value
            if has_ch4:
                df.loc[bool_ref, f'reference_{standard.lower()}_ch4'] = ch4_value
    return df
