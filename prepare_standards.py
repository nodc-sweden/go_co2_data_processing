import pandas as pd
import numpy as np


def get_median_and_interpolate(df: pd.DataFrame, standard: str):
    bool_col = f'is_std{standard}'
    co2_values = df['CO2 ppm'].copy()
    is_co2_avg = df['CO2 avg ppm'].notna()
    co2_values.loc[is_co2_avg] = df.loc[is_co2_avg, 'CO2 avg ppm']
    if bool_col not in df.columns:
        raise ValueError(f"Column 'is_std{standard}' not found in the DataFrame.")
    df[f'median_std{standard}'] = np.nan
    df[f'median_std{standard}_CO2_avg'] = np.nan
    bool_series_diff = df[bool_col].astype(int).diff().fillna(0)

    if df[bool_col].iloc[0] == 1:
        bool_series_diff.iloc[0] = 1

    if df[bool_col][len(df[bool_col]) - 1] == 1:
        bool_series_diff[len(df[bool_col])] = -1

    start_indices = bool_series_diff[bool_series_diff == 1].index
    end_indices = bool_series_diff[bool_series_diff == -1].index
    for start_idx, end_idx in zip(start_indices, end_indices):
        # temp_values = df['CO2 ppm'].iloc[start_idx:end_idx].values
        temp_values = co2_values.iloc[start_idx:end_idx].values
        median_value = np.nanmedian(temp_values)
        df.loc[start_idx:end_idx - 1, f'median_std{standard}'] = median_value

    # interpolate
    df[f'interpolated_std{standard}'] = np.nan
    df[f'interpolated_std{standard}'].values[~np.isnan(df[f'median_std{standard}'])] = (
        df[f'median_std{standard}'].values)[~np.isnan(df[f'median_std{standard}'])]

    for i in range(len(start_indices) - 1):
        if ((df.loc[start_indices[i + 1], 'elapsed time (s)'] - df.loc[end_indices[i] - 1, 'elapsed time (s)']) >
                12 * 3600):
            bool_t = ((df['elapsed time (s)'] > df.loc[end_indices[i] - 1, 'elapsed time (s)']) &
                      (df['elapsed time (s)'] <= df.loc[end_indices[i] - 1, 'elapsed time (s)'] + 12 * 3600))
            df.loc[bool_t, f'interpolated_std{standard}'] = df.loc[
                end_indices[i] - 1, f'median_std{standard}']
        elif (df.loc[start_indices[i + 1], f'reference_std{standard}'] ==
              df.loc[end_indices[i] - 1, f'reference_std{standard}']):
            df.loc[end_indices[i]:start_indices[i + 1] - 1, f'interpolated_std{standard}'] = \
                (np.interp(df.loc[end_indices[i]:start_indices[i + 1] - 1, 'elapsed time (s)'].values,
                           df.loc[[end_indices[i] - 1, start_indices[i + 1]], 'elapsed time (s)'].values,
                           df.loc[[end_indices[i] - 1, start_indices[i + 1]], f'median_std{standard}'].values))
        else:
            df.loc[end_indices[i]:start_indices[i + 1] - 1, f'interpolated_std{standard}'] = df.loc[
                end_indices[i] - 1, f'median_std{standard}']
    return df


def get_standard_reference_value(df: pd.DataFrame, df_stds: pd.DataFrame):
    for _, row in df_stds.iterrows():
        standard = str(row['STD channel'])
        if df['Type'].str.contains(standard).any():
            if f'reference_{standard.lower()}' not in df.columns:
                df[f'reference_{standard.lower()}'] = np.nan
            co2_value = row['CO2 ppm']
            start_time = row['Start time series']
            end_time = row['End time series']
            bool_ref = (
                    (df['time series'] >= start_time) &
                    (df['time series'] <= end_time)
            )
            df.loc[bool_ref, f'reference_{standard.lower()}'] = co2_value
    return df
