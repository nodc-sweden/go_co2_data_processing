import pandas as pd
import os
import polars as pl
from datetime import datetime


#  method to list all files in a folder
def list_files(folder: str):
    file_list = [os.path.join(folder, f) for f in
                 os.listdir(folder) if os.path.join(folder, f).endswith('.txt') and ('dat' in f)]
    return file_list


def list_ferrybox_files(folder: str, start_time: datetime, end_time: datetime):
    file_list = []
    for f in os.listdir(folder):
        if (
            f.endswith('.txt')
            and ('Tavastland' in f or 'TransPaper' in f)
            and ('Region' not in f)
            and ('OK' in f)
        ):
            try:
                parts = f.split('_')
                start_time_str = parts[2]
                end_time_str = parts[3]
                start_time_file = datetime.strptime(start_time_str, "%Y%m%d%H%M%S")
                end_time_file = datetime.strptime(end_time_str, "%Y%m%d%H%M%S")
                if (end_time_file >= start_time) and (start_time_file <= end_time):
                    file_list.append(os.path.join(folder, f))
            except (IndexError, ValueError):
                continue

    return file_list


#  method to read all listed files into a pandas dataframe
def read_files_dynamic(file_list: list):
    base_cols = ['Type', 'error', 'PC Date', 'PC Time', 'LI7810_SECONDS', 'equ temp', 'CO2 std val', 'CO2 ppm',
                 'CO2 avg ppm', 'CO2 std ppm', 'CH4 ppb', 'CH4 avg ppb', 'CH4 std ppb', 'H2O ppt', 'H2O avg ppt',
                 'H2O std ppt', 'licor temp', 'licor press', 'equ press', 'H2O flow', 'licor flow', 'equ pump',
                 'vent flow', 'atm cond', 'equ cond', 'drip 1', 'cond temp', 'dry box temp', 'lab press', 'GPS Date',
                 'GPS Time', 'Lat', 'Lon', 'SBE38', 'SBE45 Salinity', 'atm pressure', 'ALICAT press']
    cols_str = ['Type', 'error', 'PC Date', 'PC Time', 'GPS Date', 'GPS Time']
    cols_float = ['LI7810_SECONDS', 'equ temp', 'CO2 std val', 'CO2 ppm', 'CO2 avg ppm', 'CO2 std ppm', 'CH4 ppb',
                  'CH4 avg ppb', 'CH4 std ppb', 'H2O ppt', 'H2O avg ppt', 'H2O std ppt', 'licor temp', 'licor press',
                  'equ press', 'H2O flow', 'licor flow', 'equ pump', 'vent flow', 'atm cond', 'equ cond', 'drip 1',
                  'cond temp', 'dry box temp', 'lab press', 'Lat', 'Lon', 'SBE38', 'SBE45 Salinity', 'atm pressure',
                  'ALICAT press']
    df = pl.DataFrame({
        col: pl.Series([], dtype=pl.Utf8) if col in cols_str else pl.Series([], dtype=pl.Float64)
        for col in base_cols
    })

    extra_cols = ['Type', 'error', 'PC Date', 'PC Time', 'CO2a W', 'CO2b W', 'H2Oa W', 'H2Ob W',
                  'LI7810_DIAG', 'LI7810_CAVITY_P_kPa', 'LI7810_CAVITY_T_degC', 'LI7810_LASER_PHASE_P',
                  'LI7810_LASER_T_RESIDUAL', 'LI7810_RING_DOWN_TIME', 'LI7810_THERMAL_ENCLOSURE_T',
                  'LI7810_PHASE_ERROR', 'LI7810_LASER_T_SHIFT', 'drip 2'
                  ]
    df_extra = pl.DataFrame({
        col: pl.Series([], dtype=pl.Utf8) for col in extra_cols
    })

    for f in file_list:
        print(f)
        df_temp = pl.read_csv(
            f,
            encoding="utf8",
            separator="\t",
            infer_schema=False,
            missing_utf8_is_empty_string=True,
            try_parse_dates=False,
            truncate_ragged_lines=True
        )

        if df_temp.height == 0:
            continue

        df_temp = df_temp.rename({col: col.lstrip() for col in df_temp.columns})
        df_temp = df_temp.filter(~pl.col('Type').str.contains('X'))  # remove rows with erroneous readings
        if 'EquPress' in df_temp.columns:  # remove ragged rows using too many chars in equ press
            df_temp = df_temp.filter(pl.col('EquPress').str.len_chars() <= 7)
        else:
            df_temp = df_temp.filter(pl.col('equ press').str.len_chars() <= 7)
        df_temp = df_temp.with_columns(pl.all().str.replace_all(',', '.'))  # replace ,
        # Handle missing data
        df_temp = df_temp.with_columns([
            pl.when(pl.col(c) == "").then(None).otherwise(pl.col(c)).alias(c) for c
            in cols_float if c in df_temp.columns
        ])

        if 'GO139v2' in f:
            if 'LI7810_H2O_ppm' in df_temp.columns:
                df_temp = df_temp.with_columns(
                    (pl.col('LI7810_H2O_ppm').cast(pl.Float64, strict=False) / 1000).alias('H2O ppt')
                )

            if 'LI7810_H2O_ppm_avg' in df_temp.columns:
                df_temp = df_temp.with_columns(
                    (pl.col('LI7810_H2O_ppm_avg').cast(pl.Float64, strict=False) / 1000).alias('H2O avg ppt')
                )

            if 'LI7810_H2O_ppm_stdev' in df_temp.columns:
                df_temp = df_temp.with_columns(
                    (pl.col('LI7810_H2O_ppm_stdev').cast(pl.Float64, strict=False) / 1000).alias('H2O std ppt')
                )

            df_temp = df_temp.rename({
                'Error': 'error',
                'PcDate': 'PC Date',
                'PcTime': 'PC Time',
                'EquTemp': 'equ temp',
                'CO2StdValue': 'CO2 std val',
                'LI7810_CO2_ppm': 'CO2 ppm',
                'LI7810_CO2_ppm_avg': 'CO2 avg ppm',
                'LI7810_CO2_ppm_stdev': 'CO2 std ppm',
                'LI7810_CH4_ppb': 'CH4 ppb',
                'LI7810_CH4_ppb_avg': 'CH4 avg ppb',
                'LI7810_CH4_ppb_stdev': 'CH4 std ppb',
                'EquPress': 'equ press',
                'EquH2OFlow': 'H2O flow',
                'LicorFlow': 'licor flow',
                'VentFlow': 'vent flow',
                'AtmCond': 'atm cond',
                'EquCond': 'equ cond',
                'Drip1': 'drip 1',
                'CondTemp': 'cond temp',
                'DryBoxTemp': 'dry box temp'
            })
        else:
            df_temp = df_temp.rename({'std val': 'CO2 std val',
                                      'CO2 um/m': 'CO2 ppm',
                                      'H2O mm/m': 'H2O ppt'},
                                     )
        if 'Date' in df_temp.columns:
            df_temp = df_temp.rename({
                'Date': 'PC Date',
            })

        # Convert to numeric
        df_temp = df_temp.with_columns([
            pl.col(c).cast(pl.Float64, strict=False) for c in cols_float if c in df_temp.columns
        ])

        # Replace missing data=-999 with null. Not an issue in recent files, can be silenced depending on data set.
        df_temp = df_temp.with_columns([
            pl.when(pl.col(c) == -999).then(None).otherwise(pl.col(c)).alias(c) for c
            in cols_float if c in df_temp.columns
        ])

        # Replace missing equ temp==0 with null. Not an issue in recent files, can be silenced depending on data set.
        df_temp = df_temp.with_columns([
            pl.when(pl.col('equ temp') == 0).then(None).otherwise(pl.col('equ temp')).alias('equ temp')
        ])

        missing_cols = [col for col in base_cols if col not in df_temp.columns]
        df_temp = df_temp.with_columns([
            pl.lit(None, dtype=pl.Utf8).alias(col) if col in cols_str else pl.lit(None, dtype=pl.Float64).alias(col)
            for col in missing_cols
        ])

        missing_cols = [col for col in extra_cols if col not in df_temp.columns]
        df_temp = df_temp.with_columns([
            pl.lit(None, dtype=pl.Utf8).alias(col) for col in missing_cols
        ])

        df = pl.concat([df, df_temp.select(base_cols)], how="vertical", rechunk=True)
        df_extra = pl.concat([df_extra, df_temp.select(extra_cols)], how="vertical", rechunk=True)

    # remove duplicated rows if existing
    df = df.unique()
    df_extra = df_extra.unique()

    # add datetime time series
    df = df.with_columns(
        (
                pl.col("PC Date") + " " + pl.col("PC Time")
        ).str.strptime(pl.Datetime, format="%d/%m/%y %H:%M:%S").alias("time series")
    )

    # add year, month, day, hour, min
    df = df.with_columns([
        pl.col("time series").dt.year().alias("Year"),
        pl.col("time series").dt.month().alias("Month"),
        pl.col("time series").dt.day().alias("Day"),
        pl.col("time series").dt.hour().alias("Hour"),
        pl.col("time series").dt.minute().alias("Minute"),
    ])

    df = df.sort('time series')
    df = df.with_columns(
        (pl.col("time series") - pl.col("time series").first()).dt.total_seconds().alias("elapsed time (s)")
    )
    df_extra = df_extra.with_columns(
        (
                pl.col("PC Date") + " " + pl.col("PC Time")
        ).str.strptime(pl.Datetime, format="%d/%m/%y %H:%M:%S").alias("time series")
    )
    df_extra = df_extra.sort('time series')
    return df, df_extra


def read_standards(standards_path: str):
    df_stds = pd.read_excel(standards_path)
    df_stds['Start time'] = pd.to_timedelta(df_stds['Start time'].astype(str))
    df_stds['End time'] = pd.to_timedelta(df_stds['End time'].astype(str))
    df_stds['Start time series'] = df_stds['Start date'] + df_stds['Start time']
    df_stds['End time series'] = df_stds['End date'] + df_stds['End time']
    df_stds['End time series'] = df_stds['End time series'].fillna(pd.Timestamp.now())
    return df_stds


def read_ferrybox_files_dynamic(file_list: list):
    base_cols = ['38055', '38003', '8002', '88002', '8003', '88003', '8172', '88172',
                 '8181', '88181', '8179', '88179', '8180', '88180', '72', '80072', '70',
                 '80070', '8032', '88032', '8165', '88165', '8173', '88173', '8191', '88191',
                 '8063', '88063', '8174','88174', 'Time_series']

    df = pl.DataFrame()

    for f in file_list:
        print(f)
        df_temp = pl.read_csv(
            f,
            encoding='utf8',
            separator='\t',
            infer_schema=False,
            has_header=True,
            try_parse_dates=False,
            missing_utf8_is_empty_string=True,
            truncate_ragged_lines=True
        )

        df_temp = df_temp.rename({col: col.lstrip() for col in df_temp.columns})
        if "38003" in df_temp.columns:
            df_temp = df_temp.with_columns([pl.col("38003").
                                            str.strptime(pl.Datetime, strict=False,
                                                         format="%Y%m%d%H%M%S").alias("Time_series")])
        else:
            df_temp = df_temp.with_columns([pl.col("38055").
                                           str.strptime(pl.Datetime, format="%Y%m%d%H%M%S",
                                                        strict=False).alias("Time_series")])

        for col in base_cols:
            if col not in df_temp.columns:
                df_temp = df_temp.with_columns(pl.lit(None, dtype=pl.Utf8).alias(col))

        df = pl.concat([df, df_temp.select(base_cols)], how="vertical", rechunk=True)

    df = df.unique()
    df = df.sort("Time_series")
    df = df.rename({
        "8002": "Latitude",
        "88002": "QF Latitude",
        "8003": "Longitude",
        "88003": "QF Longitude",
        "8172": "Water_flow",
        "88172": "QF Water_flow",
        "8179": "SST",
        "88179": "QF SST",
        "8181": "SSS",
        "88181": "QF SSS",
        "72": "Air_temperature",
        "80072": "QF Air_temperature",
        "70": "Atm_pressure",
        "80070": "QF Atm_pressure",
        "8032": "QFF FB",
        "88032": "QF QFF FB",
        "8165": "CDOM",
        "88165": "QF CDOM",
        "8173": "Phycocyanin",
        "88173": "QF Phycocyanin",
        "8191": "O2",
        "88191": "QF O2",
        "8063": "Chl_fluorescense",
        "88063": "QF Chl_fluorescense",
        "8174": "Turbidity",
        "88174": "QF Turbidity"
    })

    cols = ["Latitude", "QF Latitude", "Longitude", "QF Longitude", "Water_flow", "QF Water_flow", "SST", "QF SST",
            "SSS", "QF SSS", "Air_temperature", "QF Air_temperature", "Atm_pressure", "QF Atm_pressure", "QFF FB",
            "QF QFF FB", "CDOM", "QF CDOM", "Phycocyanin", "QF Phycocyanin", "O2", "QF O2", "Chl_fluorescense",
            "QF Chl_fluorescense", "Turbidity", "QF Turbidity"]

    for col in cols:
        df = df.with_columns(
            pl.col(col)
            .str.strip_chars()
            .replace("", None)
            .cast(pl.Float64)
            .alias(col)
        )

    df = df.filter(
        (pl.col("QF Water_flow") >= 0) & (pl.col("QF Water_flow") < 3)
    )

    df = df.with_columns([
        pl.when(pl.col(col) == -999).then(None).otherwise(pl.col(col)).alias(col) for col in cols
    ])

    df_fb = df.select([
        "Time_series",
        "Latitude",
        "QF Latitude",
        "Longitude",
        "QF Longitude",
        "SST",
        "QF SST",
        "SSS",
        "QF SSS",
        "Air_temperature",
        "QF Air_temperature",
        "Atm_pressure",
        "QF Atm_pressure",
        "QFF FB",
        "QF QFF FB",
        "CDOM",
        "QF CDOM",
        "Phycocyanin",
        "QF Phycocyanin",
        "O2",
        "QF O2",
        "Chl_fluorescense",
        "QF Chl_fluorescense",
        "Turbidity",
        "QF Turbidity"
    ])
    return df_fb


def merge_go_and_ferrybox(df: pd.DataFrame, df_fb: pd.DataFrame):
    df = pd.merge_asof(
        df.reset_index(),
        df_fb,
        left_on='time series', right_on='Time_series',
        direction='nearest', tolerance=pd.Timedelta(seconds=60)).set_index('index').loc[df.index]
    return df




