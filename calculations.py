import numpy as np
import pandas as pd
from scipy import stats
import math
from datetime import datetime


# function from ICOS workshop,not in use
def pres_at_sea_level(pressure, temperature_c, height):
    return pressure * pow((1 - (0.0065 * height) / (temperature_c + 0.0065 * height + 273.15)), -5.257)


# function used by SMHI
def calculate_qff(temperature_c, latitude, height: int, pressure):
    #  QFF: the air pressure at the monitoring station reduced to sea level, typically using local temperature
    #  observations (e.g. use "Air_temperature"). This is in contrast to QNH, which is the sea level pressure calculated
    #  assuming a standard atmosphere.
    latitude = latitude.fillna(60)
    temperature_c = temperature_c.fillna(15)

    valid_pressure = (pressure >= 600) & (pressure <= 1100)
    b = 3.4163 * (1 - 0.0026373 * np.cos(2 * latitude * math.pi)) / 100
    t1 = pd.Series(np.zeros(len(temperature_c)), index=temperature_c.index)

    bool_low = temperature_c < -7
    bool_middle = (temperature_c >= -7) & (temperature_c < 2)
    bool_high = temperature_c >= 2

    t1[bool_low] = temperature_c[bool_low] * 0.5 + 275.0
    t1[bool_middle] = temperature_c[bool_middle] * 0.535 + 275.6
    t1[bool_high] = temperature_c[bool_high] * 1.07 + 274.5

    qff = pd.Series(np.nan, index=pressure.index)
    qff[valid_pressure] = pressure[valid_pressure] * np.exp(height *
                                                            b[valid_pressure] / t1[valid_pressure])
    return qff


def get_qff(df: pd.DataFrame) -> pd.DataFrame:
    df['qff'] = np.nan
    bool_qff = ((df['time series'] < datetime(2023, 1, 1, 0, 0, 0)) &
                df['QF QFF'])
    df.loc[bool_qff, 'qff'] = df.loc[bool_qff, 'QFF']
    bool_atm_pres = (~bool_qff) & df['QF Atm_pressure'] & df['QF Air_temperature'] & (df['QF Latitude'] < 3)
    df.loc[bool_atm_pres, 'qff'] = calculate_qff(df.loc[bool_atm_pres, 'Air_temperature'],
                                                 df.loc[bool_atm_pres, 'Latitude'], 27,
                                                 df.loc[bool_atm_pres,'Atm_pressure'])
    return df


def get_p_equ_p_atm(df: pd.DataFrame) -> pd.DataFrame:
    df['P_equ'] = np.nan
    is_pressure = (
        df['lab press'].notna() &
        df['QF lab press'] &
        df['QF equ press']
    )
    df.loc[is_pressure, 'P_equ'] = (
        df.loc[is_pressure, 'lab press'] +
        df.loc[is_pressure, 'equ press']
    ) / 1013.25

    is_pressure_2 = (
        df['licor press'].notna() &
        df['lab press'].isna() &
        df['QF licor press'] &
        df['QF equ press']
    )
    df.loc[is_pressure_2, 'P_equ'] = (
        df.loc[is_pressure_2, 'licor press'] +
        df.loc[is_pressure_2, 'equ press']
    ) / 1013.25
    df['P_equ_is_from_QFF'] = df['P_equ'].isna() & df['qff'].notna()
    df.loc[df['P_equ_is_from_QFF'], 'P_equ'] = df['qff'] / 1013.25

    df['P_atm_sea'] = df['qff'] / 1013.25
    return df


def get_delta_temperature(df: pd.DataFrame) -> pd.DataFrame:
    df['delta temperature'] = df['equ temp'] - df['SST']
    return df


def correct_co2_based_on_standards(df: pd.DataFrame, standards: list, start_time: datetime, calibration_threshold: int = 10,
                                   standard_threshold: int = 10) -> pd.DataFrame:
    df['xco2_cal'] = np.nan
    df['standard_slope'] = np.nan
    df['standard_intercept'] = np.nan
    df['standard_r_square'] = np.nan
    df['number_of_standards'] = np.nan
    df['QF xco2_cal'] = True

    # use CO2 avg ppm if existing
    co2_values = df['CO2 avg ppm'].copy()
    is_co2_avg = df['CO2 avg ppm'].notna()
    # else use CO2 ppm
    is_co2 = ~is_co2_avg
    co2_values.loc[is_co2] = df.loc[is_co2, 'CO2 ppm']

    # only use std1 when there's too few of the others
    if '1' in standards and len(standards) > 3 and start_time < datetime(2025, 1, 1, 0, 0, 0):
        standards = [s for s in standards if s != '1']

    for idx, j in enumerate(co2_values):
        interpolated_stds = []
        reference_stds = []
        for item in standards:
            interpolated_stds.append(df[f'interpolated_std{item}'].iloc[idx])
            reference_stds.append(df[f'reference_std{item}'].iloc[idx])
        combined = [(ref, interp) for ref, interp in zip(reference_stds, interpolated_stds)
                    if not pd.isna(ref) and not pd.isna(interp)]
        if len(combined) < 2:
            df.loc[idx, 'QF xco2_cal'] = False
            continue
        combined.sort(key=lambda x: x[0])
        reference_stds_sorted, interpolated_stds_sorted = zip(*combined)
        slope, intercept, r, p, std_err = stats.linregress(reference_stds_sorted, interpolated_stds_sorted)
        converted_slope = 1 / slope
        converted_intercept = (intercept * -1) / slope
        df.loc[idx, 'xco2_cal'] = co2_values.loc[idx] * converted_slope + converted_intercept
        df.loc[idx, 'standard_slope'] = slope
        df.loc[idx, 'standard_intercept'] = intercept
        df.loc[idx, 'standard_r_square'] = r**2
        df.loc[idx, 'number_of_standards'] = len(reference_stds_sorted)
        for ref, interp in zip(reference_stds_sorted, interpolated_stds_sorted):
            df.loc[idx, 'QF xco2_cal'] &= abs(ref - interp) <= standard_threshold
    df.loc[is_co2_avg, 'QF xco2_cal'] &= (df.loc[is_co2_avg, 'QF CO2 avg ppm'] &
                                          ((df.loc[is_co2_avg, 'xco2_cal'] -
                                            df.loc[is_co2_avg, 'CO2 avg ppm']).abs() <= calibration_threshold))

    df.loc[is_co2, 'QF xco2_cal'] &= (df.loc[is_co2, 'QF CO2 ppm'] &
                                          ((df.loc[is_co2, 'xco2_cal'] -
                                            df.loc[is_co2, 'CO2 ppm']).abs() <= calibration_threshold))
    return df


def calculate_pco2_dry(df: pd.DataFrame, is_valid_equ: pd.Series, is_valid_atm: pd.Series) -> pd.DataFrame:
    df = get_p_equ_p_atm(df)
    df['pco2_dry'] = np.nan
    df.loc[df['is_equ'] & is_valid_equ, 'pco2_dry'] = (df.loc[df['is_equ'] & is_valid_equ, 'xco2_cal'] *
                                                       df.loc[df['is_equ'] & is_valid_equ, 'P_equ'])
    df.loc[df['is_atm'] & is_valid_atm, 'pco2_dry'] = (df.loc[df['is_atm'] & is_valid_atm, 'xco2_cal'] *
                                                       df.loc[df['is_atm'] & is_valid_atm, 'P_atm_sea'])

    return df


def calculate_ph2o(temperature_c, salinity):
    temperature_k = temperature_c + 273.15
    return np.exp(24.4543 - 67.4509 * (100 / temperature_k) -
                  4.8489 * np.log(temperature_k / 100) - 0.000544 * salinity)


def calculate_pco2_wet(df: pd.DataFrame, is_valid_equ: pd.Series, is_valid_atm: pd.Series) -> pd.DataFrame:
    df['pco2_wet'] = np.nan
    df.loc[df['is_equ'] & is_valid_equ, 'pco2_wet'] = \
        (df.loc[df['is_equ'] & is_valid_equ, 'xco2_cal'] *
         (df.loc[df['is_equ'] & is_valid_equ, 'P_equ'] -
          calculate_ph2o(df.loc[df['is_equ'] & is_valid_equ, 'equ temp'],
                         df.loc[df['is_equ'] & is_valid_equ, 'SSS'])))
    df.loc[df['is_atm'] & is_valid_atm, 'pco2_wet'] = \
        (df.loc[df['is_atm'] & is_valid_atm, 'xco2_cal'] *
         (df.loc[df['is_atm'] & is_valid_atm, 'P_atm_sea'] -
          calculate_ph2o(df.loc[df['is_atm'] & is_valid_atm, 'SST'],
                         df.loc[df['is_atm'] & is_valid_atm, 'SSS'])))
    df['pco2_wet_atm'] = np.nan
    df.loc[df['is_atm'] & is_valid_atm, 'pco2_wet_atm'] = df.loc[df['is_atm'] & is_valid_atm, 'pco2_wet']

    return df


def calculate_fco2(temperature_c, pressure, pco2_wet, xco2_cal):
    t_k = temperature_c + 273.15
    # virial coefficient, B
    b_virial_coef = -1636.75 + 12.0408 * t_k - 0.0327957 * pow(t_k, 2) + (3.16528 * 1e-5) * pow(t_k, 3)
    # virial coefficient, delta
    delta = 57.7 - 0.118 * t_k
    r_gas_constant = 82.0578  # atm cm3 K-1 mol-1, from DOE2 and recommended by Pierrot et al. (2009)
    return pco2_wet * np.exp((pressure * (b_virial_coef + 2 * pow(1 - xco2_cal * 1e-6, 2) * delta))
                             / (r_gas_constant * t_k))


def calculate_fco2_wet(df: pd.DataFrame, is_valid_equ: pd.Series, is_valid_atm: pd.Series) -> pd.DataFrame:
    df['fco2_wet'] = np.nan
    df.loc[df['is_equ'] & is_valid_equ, 'fco2_wet'] = calculate_fco2(df.loc[df['is_equ'] & is_valid_equ, 'equ temp'],
                                                                     df.loc[df['is_equ'] & is_valid_equ, 'P_equ'],
                                                                     df.loc[df['is_equ'] & is_valid_equ, 'pco2_wet'],
                                                                     df.loc[df['is_equ'] & is_valid_equ, 'xco2_cal'])
    df.loc[df['is_atm'] & is_valid_atm, 'fco2_wet'] = calculate_fco2(df.loc[df['is_atm'] & is_valid_atm, 'SST'],
                                                                     df.loc[df['is_atm'] & is_valid_atm, 'P_atm_sea'],
                                                                     df.loc[df['is_atm'] & is_valid_atm, 'pco2_wet'],
                                                                     df.loc[df['is_atm'] & is_valid_atm, 'xco2_cal'])
    df['fco2_wet_atm'] = np.nan
    df.loc[df['is_atm'] & is_valid_atm, 'fco2_wet_atm'] = df.loc[df['is_atm'] & is_valid_atm, 'fco2_wet']
    return df


def calculate_pco2_fco2_in_situ(df: pd.DataFrame, is_valid_equ: pd.Series) -> pd.DataFrame:
    df['pco2_wet_sst'] = np.nan
    df['fco2_wet_sst'] = np.nan
    df.loc[df['is_equ'] & is_valid_equ, 'pco2_wet_sst'] = \
        (df.loc[df['is_equ'] & is_valid_equ, 'pco2_wet'] *
         np.exp(0.0423 * (df.loc[df['is_equ'] & is_valid_equ, 'SST'] -
                          df.loc[df['is_equ'] & is_valid_equ, 'equ temp'])))
    df.loc[df['is_equ'], 'fco2_wet_sst'] = \
        (df.loc[df['is_equ'] & is_valid_equ, 'fco2_wet'] *
         np.exp(0.0423 * (df.loc[df['is_equ'] & is_valid_equ, 'SST'] -
                          df.loc[df['is_equ'] & is_valid_equ, 'equ temp'])))

    return df




