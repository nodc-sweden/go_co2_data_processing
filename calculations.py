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


def correct_based_on_standards(parameter: str,
                               unit: str,
                               df: pd.DataFrame,
                               standards: list,
                               start_time: datetime,
                               calibration_threshold: int = 10,
                               standard_threshold: int = 10) -> pd.DataFrame:
    parameter_upper = parameter.upper()
    parameter = parameter.lower()
    df[f'x{parameter}_cal'] = np.nan
    df[f'standard_slope_{parameter}'] = np.nan
    df[f'standard_intercept_{parameter}'] = np.nan
    df[f'standard_r_square_{parameter}'] = np.nan
    df[f'number_of_standards_{parameter}'] = np.nan
    df[f'QF x{parameter}_cal'] = True

    # use avg if existing
    values = df[f'{parameter_upper} avg {unit}'].copy()
    is_avg = df[f'{parameter_upper} avg {unit}'].notna()
    is_not_avg = ~is_avg
    values.loc[is_not_avg] = df.loc[is_not_avg, f'{parameter_upper} {unit}']

    # only use std1 when there's too few of the others
    if '1' in standards and len(standards) > 3 and start_time < datetime(2025, 1, 1, 0, 0, 0):
        standards = [s for s in standards if s != '1']

    for idx, value in enumerate(values):
        interpolated_stds = []
        reference_stds = []
        for item in standards:
            interpolated_stds.append(df[f'interpolated_std{item}_{parameter}'].iloc[idx])
            reference_stds.append(df[f'reference_std{item}_{parameter}'].iloc[idx])
        combined = [(ref, interp) for ref, interp in zip(reference_stds, interpolated_stds)
                    if not pd.isna(ref) and not pd.isna(interp)]
        if len(combined) < 2:
            df.loc[idx, f'QF x{parameter}_cal'] = False
            continue
        combined.sort(key=lambda x: x[0])
        reference_stds_sorted, interpolated_stds_sorted = zip(*combined)
        slope, intercept, r, p, std_err = stats.linregress(reference_stds_sorted, interpolated_stds_sorted)
        if (not np.isfinite([slope, r]).all() # if -inf, inf or nan
                or r**2 < 0.98): # from Quantitative Chemical Analysis, Daniel C. Harris
            df.loc[idx, f'QF x{parameter}_cal'] = False
            continue
        converted_slope = 1 / slope
        converted_intercept = (intercept * -1) / slope
        df.loc[idx, f'x{parameter}_cal'] = values.loc[idx] * converted_slope + converted_intercept
        df.loc[idx, f'standard_slope_{parameter}'] = slope
        df.loc[idx, f'standard_intercept_{parameter}'] = intercept
        df.loc[idx, f'standard_r_square_{parameter}'] = r**2
        df.loc[idx, f'number_of_standards_{parameter}'] = len(reference_stds_sorted)
        for ref, interp in zip(reference_stds_sorted, interpolated_stds_sorted):
            df.loc[idx, f'QF x{parameter}_cal'] &= abs(ref - interp) <= standard_threshold
    df.loc[is_avg, f'QF x{parameter}_cal'] &= (df.loc[is_avg, f'QF {parameter_upper} avg {unit}'] &
                                          ((df.loc[is_avg, f'x{parameter}_cal'] -
                                            df.loc[is_avg, f'{parameter_upper} avg {unit}']).abs() <= calibration_threshold))

    df.loc[is_not_avg, f'QF x{parameter}_cal'] &= (df.loc[is_not_avg, f'QF {parameter_upper} {unit}'] &
                                          ((df.loc[is_not_avg, f'x{parameter}_cal'] -
                                            df.loc[is_not_avg, f'{parameter_upper} {unit}']).abs() <= calibration_threshold))
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


def calculate_ph2o_equ_atm(df: pd.DataFrame, is_valid_equ: pd.Series, is_valid_atm: pd.Series) -> pd.DataFrame:
    df['ph2o'] = np.nan
    df.loc[df['is_equ'] & is_valid_equ, 'ph2o'] = calculate_ph2o(df.loc[df['is_equ'] & is_valid_equ, 'equ temp'],
                                                                 df.loc[df['is_equ'] & is_valid_equ, 'SSS'])
    df.loc[df['is_atm'] & is_valid_atm, 'ph2o'] = calculate_ph2o(df.loc[df['is_atm'] & is_valid_atm, 'SST'],
                                                                 df.loc[df['is_atm'] & is_valid_atm, 'SSS'])
    return df


def calculate_pco2_wet(df: pd.DataFrame, is_valid_equ: pd.Series, is_valid_atm: pd.Series) -> pd.DataFrame:
    df['pco2_wet'] = np.nan
    df.loc[df['is_equ'] & is_valid_equ, 'pco2_wet'] = \
        (df.loc[df['is_equ'] & is_valid_equ, 'xco2_cal'] *
         (df.loc[df['is_equ'] & is_valid_equ, 'P_equ'] -
          df.loc[df['is_equ'] & is_valid_equ, 'ph2o']))
    df.loc[df['is_atm'] & is_valid_atm, 'pco2_wet'] = \
        (df.loc[df['is_atm'] & is_valid_atm, 'xco2_cal'] *
         (df.loc[df['is_atm'] & is_valid_atm, 'P_atm_sea'] -
          df.loc[df['is_atm'] & is_valid_atm, 'ph2o']))
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


def calculate_pch4_dry(df: pd.DataFrame, is_valid_equ: pd.Series, is_valid_atm: pd.Series) -> pd.DataFrame:
    # Note xCH4 is given as ppb, the resulting unit will be natm
    df = get_p_equ_p_atm(df)
    df['pch4_dry'] = np.nan
    df.loc[df['is_equ'] & is_valid_equ, 'pch4_dry'] = (df.loc[df['is_equ'] & is_valid_equ, 'xch4_cal'] *
                                                       df.loc[df['is_equ'] & is_valid_equ, 'P_equ'])
    df.loc[df['is_atm'] & is_valid_atm, 'pch4_dry'] = (df.loc[df['is_atm'] & is_valid_atm, 'xch4_cal'] *
                                                       df.loc[df['is_atm'] & is_valid_atm, 'P_atm_sea'])
    return df


def calculate_pch4_wet(df: pd.DataFrame, is_valid_equ: pd.Series, is_valid_atm: pd.Series) -> pd.DataFrame:
    # Note xCH4 is given as ppb, the resulting unit will be natm
    df['pch4_wet'] = np.nan
    df.loc[df['is_equ'] & is_valid_equ, 'pch4_wet'] = \
        (df.loc[df['is_equ'] & is_valid_equ, 'xch4_cal'] *
         (df.loc[df['is_equ'] & is_valid_equ, 'P_equ'] -
          df.loc[df['is_equ'] & is_valid_equ, 'ph2o']))
    df.loc[df['is_atm'] & is_valid_atm, 'pch4_wet'] = \
        (df.loc[df['is_atm'] & is_valid_atm, 'xch4_cal'] *
         (df.loc[df['is_atm'] & is_valid_atm, 'P_atm_sea'] -
          df.loc[df['is_atm'] & is_valid_atm, 'ph2o']))
    df['pch4_wet_atm'] = np.nan
    df.loc[df['is_atm'] & is_valid_atm, 'pch4_wet_atm'] = df.loc[df['is_atm'] & is_valid_atm, 'pch4_wet']
    return df


def calculate_bunsen_solubility_coefficient(
        temperature_c,
        salinity
):
    # Constants determined by Wiesenburg and Guinasso, 1979,
    # for calculation of the bunsen solubility coefficient.
    # Note that methane is treated as an ideal gas and the
    # Bunsen coefficient is defined as the volume of gas, reduced
    # to 0 degC and 1 atm of pressure (standard temperature and
    # pressure) contained in a unit volume of water at the temperature
    # of the measurement when the partial pressure of the gas is 1
    # atm.
    # The equation is originally derived by Weiss (1970, 1971).
    temperature_k = temperature_c + 273.15
    a1 = -68.8862
    a2 = 101.4956
    a3 = 28.7314
    b1 = -0.076146
    b2 = 0.043970
    b3 = -0.0068672
    return np.exp(a1 + a2 * (100 / temperature_k) + a3 * np.log(temperature_k / 100) +
           salinity * (b1 + b2 * (temperature_k / 100) + b3 * (temperature_k / 100) ** 2))


def seawater_density_at_1_atm(
        temperature_c,
        salinity
):
    # Calculate the density of pure water (S=0) from Bigg, 1967.
    dens_h2o= (
            999.842594
            + 6.793952e-2 * temperature_c
            - 9.095290e-3 * temperature_c ** 2
            + 1.001685e-4 * temperature_c ** 3
            - 1.120083e-6 * temperature_c ** 4
            + 6.536332e-9 * temperature_c ** 5
    )
    # Calculate density of seawater at 1 atm(p=0) from Millero & Poisson 1981
    a = (
            8.24493e-1
            - 4.0899e-3 * temperature_c
            + 7.6438e-5 * temperature_c ** 2
            - 8.2467e-7 * temperature_c ** 3
            + 5.3875e-9 * temperature_c ** 4
    )
    b = (
            -5.72466e-3
            + 1.0227e-4 * temperature_c
            - 1.6546e-6 * temperature_c ** 2
    )
    c = 4.8314e-4
    return (
            dens_h2o
            + a * salinity
            + b * salinity ** 1.5
            + c * salinity ** 2
    ) / 1000  # g/cm3, kg/L


def calculate_ch4_nmol_kg_and_pch4_in_situ(df: pd.DataFrame, is_valid_equ: pd.Series) -> pd.DataFrame:
    # Molar volume for an ideal gas at 1 atm (101.325 kPa) from NIST:
    Vm = 22.41396954 # L mol-1

    # Bunsen coefficient equ chamber
    beta_equ = calculate_bunsen_solubility_coefficient(
        df.loc[is_valid_equ, 'equ temp'],
        df.loc[is_valid_equ, 'SSS']
    )

    # Seawater density at 1 atm in equilibrator
    dens_equ = seawater_density_at_1_atm(
        df.loc[is_valid_equ, 'equ temp'],
        df.loc[is_valid_equ, 'SSS']
    )
    # CH4 concentration
    df['ch4_nmol_kg'] = np.nan
    df.loc[is_valid_equ, 'ch4_nmol_kg'] = (df.loc[is_valid_equ, 'pch4_wet'] *
                                           beta_equ / (Vm * dens_equ)
                                           )

    # Bunsen coefficient in situ
    beta_in_situ = calculate_bunsen_solubility_coefficient(
        df.loc[is_valid_equ, 'SST'],
        df.loc[is_valid_equ, 'SSS']
    )
    # Seawater density at 1 atm in situ
    dens_in_situ = seawater_density_at_1_atm(
        df.loc[is_valid_equ, 'SST'],
        df.loc[is_valid_equ, 'SSS']
    )
    df['pch4_wet_sst'] = np.nan
    df.loc[df['is_equ'] & is_valid_equ, 'pch4_wet_sst'] =  (df.loc[is_valid_equ, 'ch4_nmol_kg'] * dens_in_situ / (
        beta_in_situ / Vm)
    )

    return df





