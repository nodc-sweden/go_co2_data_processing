import folium
import os
import pandas as pd
from plotly.subplots import make_subplots
import plotly.graph_objects as go
from datetime import datetime
import webbrowser

from export_results import get_figure_path


def plot_ship_track(df: pd.DataFrame, start_date: str, end_date: str):
    coord = df.dropna(subset=['Latitude', 'Longitude']).copy()
    mid_lat = (
        coord['Latitude'].min()
        + (coord['Latitude'].max() - coord['Latitude'].min()) / 2
    )
    mid_lon = (
        coord['Longitude'].min()
        + (coord['Longitude'].max() - coord['Longitude'].min()) / 2
    )
    trailMap = folium.Map(
        location=[mid_lat, mid_lon],
        zoom_start=10
    )

    for _, row in coord.iterrows():
        if row['QF ocean']:
            color = 'blue'
        else:
            color = 'red'

        folium.CircleMarker(
            location=[row['Latitude'], row['Longitude']],
            radius=1,
            weight=2,
            color=color,
            fill=True,
            fill_color=color,
            fill_opacity=1
        ).add_to(trailMap)

    trailMap.fit_bounds(trailMap.get_bounds())
    export_file_path = os.path.join(
        get_figure_path(),
        f'ship_track_{start_date}_{end_date}.html'
    )
    trailMap.save(export_file_path)
    webbrowser.open(export_file_path)


def add_parameter_to_subplot(fig, df: pd.DataFrame, y: str, row: int, col: int = 1, marker_color: str = 'blue'):
    if not df[y].empty:
        fig.add_trace(go.Scatter(x=df['time series'], y=df[y], mode='markers',
                                 name=y, marker=dict(color=marker_color)), row=row, col=col)
        return


def add_parameter_with_qf_to_subplot(fig, df: pd.DataFrame, y: str, row: int, col: int = 1):
    qf_col = f'QF {y}'
    if not df.loc[df[qf_col], y].empty:
        fig.add_trace(go.Scatter(x=df.loc[df[qf_col], 'time series'], y=df.loc[df[qf_col], y], mode='markers',
                                 name=f'Ok {y}', marker=dict(color='blue')), row=row, col=col)
    if not df.loc[~df[qf_col], y].empty:
        fig.add_trace(go.Scatter(x=df.loc[~df[qf_col], 'time series'], y=df.loc[~df[qf_col], y], mode='markers',
                                 name=f'Bad {y}', marker=dict(color='red')), row=row, col=col)
    return


def plot_with_subplots(df: pd.DataFrame, parameters_extended: list, exception: list, start_date: str,
                                 end_date: str,):
    parameters = []
    for item in parameters_extended:
        if item in df.columns and df[item].notna().any():
            parameters.append(item)

    if not parameters:
        return

    fig = make_subplots(rows=len(parameters), cols=1, subplot_titles=parameters, shared_xaxes=True, vertical_spacing=0.1)
    row = 1
    for item in parameters:
        if item in exception:
            fig.add_trace(go.Scatter(x=df['time series'], y=df[item], mode='markers', name=item,
                                     marker=dict(color='blue')), row=row, col=1)
            row = row+1
        else:
            add_parameter_with_qf_to_subplot(fig, df, item, row)
            row = row + 1

    fig.update_layout(height=1000, showlegend=False, font=dict(size=14))
    fig.show()
    if parameters[0] == "SST":
        export_file_path = os.path.join(get_figure_path(),
                                        f'{parameters[-1]}_{start_date}_{end_date}.html')
    else:
        export_file_path = os.path.join(get_figure_path(),
                                        f'{parameters[0]}_{start_date}_{end_date}.html')
    fig.write_html(export_file_path)
    return


def plot_with_subplots_selection(df: pd.DataFrame, parameters_extended: list,
                                 selection_extended: list, start_date: str,
                                 end_date: str,):
    parameters = []
    selection = []
    subplot_names = []
    for item in selection_extended:
        if item in df.columns and (df[item] == 1).any():
            selection.append(item)

    for param in parameters_extended:
        if param in df.columns and df[param].notna().any():
            parameters.append(param)
            for key in selection:
                subplot_names.append(f'{param} {key[3:]}')

    if not parameters:
        return

    fig = make_subplots(rows=len(subplot_names), cols=1, subplot_titles=subplot_names, shared_xaxes=True,
                        vertical_spacing=0.1)
    row = 1
    for y in parameters:
        for item in selection:
            filtered_df = df[df[item] == 1]
            add_parameter_with_qf_to_subplot(fig, filtered_df, y, row)
            row = row + 1

    fig.update_layout(height=1000, showlegend=False, font=dict(size=14))
    fig.show()
    export_file_path = os.path.join(get_figure_path(),
                                    f'{parameters[0]}_{start_date}_{end_date}.html')
    fig.write_html(export_file_path)
    return


def plot_with_subplots_selection_no_qf(df: pd.DataFrame, parameters_extended: list,
                                 selection_extended: list, start_date: str,
                                 end_date: str,):
    parameters = []
    selection = []
    subplot_names = []
    for item in selection_extended:
        if item in df.columns and (df[item] == 1).any():
            selection.append(item)

    for param in parameters_extended:
        if param in df.columns and df[param].notna().any():
            parameters.append(param)
            for key in selection:
                subplot_names.append(f'{param} {key[3:]}')

    if not parameters:
        return

    fig = make_subplots(rows=len(subplot_names), cols=1, subplot_titles=subplot_names, shared_xaxes=True,
                        vertical_spacing=0.1)
    row = 1
    for y in parameters:
        for item in selection:
            filtered_df = df[df[item] == 1]
            if filtered_df[y].notna().any():
                add_parameter_to_subplot(fig, filtered_df, y, row)
                row = row + 1

    fig.update_layout(height=1000, showlegend=False, font=dict(size=14))
    fig.show()
    export_file_path = os.path.join(get_figure_path(),
                                    f'{parameters[0]}_{start_date}_{end_date}.html')
    fig.write_html(export_file_path)
    return


def plot_housekeeping_parameters(df: pd.DataFrame, start_date: str, end_date: str):
    parameters_extended = ["H2O flow", "equ temp", "delta temperature", "SST", "SSS"]
    plot_with_subplots(df, parameters_extended, [], start_date, end_date)
    parameters_extended = ['vent flow', 'equ press', 'lab press', 'licor press', 'qff']
    plot_with_subplots(df, parameters_extended, ['qff'], start_date, end_date)
    selection_extended = ['is_std1_z', 'is_std1', 'is_std2', 'is_std2_s', 'is_std3', 'is_std3_s', 'is_std4',
                          'is_std4_s', 'is_std5', 'is_std5_s', 'is_atm', 'is_equ']

    plot_with_subplots_selection(df, ['licor flow'], selection_extended, start_date, end_date)
    selection_extended = ['is_std1', 'is_std2', 'is_std3', 'is_std4','is_std5', 'is_atm', 'is_equ']
    plot_with_subplots_selection_no_qf(df, ["H2O ppt",], selection_extended, start_date, end_date)
    selection_extended = ['is_std1', 'is_std2', 'is_std3', 'is_std4','is_std5', 'is_atm', 'is_equ']
    plot_with_subplots_selection_no_qf(df, ["H2O avg ppt",], selection_extended, start_date, end_date)
    plot_with_subplots(df, ["cond temp","equ cond", "atm cond"], ["cond temp", "equ cond", "atm cond"], start_date, end_date)
    selection_extended = ['is_atm', 'is_equ']
    plot_with_subplots_selection(df, ['CO2 ppm', 'CO2 avg ppm'], selection_extended,
                                 start_date, end_date)
    plot_with_subplots_selection(df, ['CH4 ppb', 'CH4 avg ppb'], selection_extended,
                                 start_date, end_date)
    return


def plot_with_subplots_standards(parameter: str,
                                 unit: str,
                                 df: pd.DataFrame,
                                 selection_extended: list,
                                 start_date: str,
                                 end_date: str,
                                 param: str = 'CO2 avg ppm',
                                  ):
    parameter = parameter.lower()
    parameter_upper = parameter.upper()
    y = f'{parameter_upper} avg {unit}' if \
        (f'{parameter_upper} avg {unit}' in df.columns
         and df[f'{parameter_upper} avg {unit}'].notna().any()) \
        else f'{parameter_upper} {unit}'
    if df[y].isna().all():
        return
    selection = []
    subplot_names = []
    for item in selection_extended:
        if item in df.columns and (df[item] == 1).any():
            selection.append(item)
            subplot_names.append(item[3:])
    fig = make_subplots(rows=len(subplot_names), cols=1, subplot_titles=subplot_names, shared_xaxes=True,
                        vertical_spacing=0.1)
    row = 1
    for item in selection:
        filtered_df = df[df[item] == 1]
        add_parameter_to_subplot(fig, filtered_df, y, row)
        if parameter == "co2":
            add_parameter_to_subplot(fig, filtered_df, f"{parameter_upper} std val", row, 1, 'magenta')
        add_parameter_to_subplot(fig, filtered_df, f"reference_{item[3:7]}_{parameter}", row, 1, 'cyan')
        row = row + 1

    fig.update_layout(height=1000, showlegend=True, font=dict(size=14))
    fig.show()
    export_file_path = os.path.join(get_figure_path(),
                                    f'standards_{parameter}_{start_date}_{end_date}.html')
    fig.write_html(export_file_path)
    return


def plot_standards(df: pd.DataFrame, start_date: str, end_date: str):
    selection_extended = ['is_std1_z', 'is_std1', 'is_std2', 'is_std2_s', 'is_std3', 'is_std3_s', 'is_std4',
                          'is_std4_s', 'is_std5', 'is_std5_s']
    plot_with_subplots_standards("CO2", "ppm", df, selection_extended, start_date, end_date)
    plot_with_subplots_standards("CH4", "ppb", df, selection_extended, start_date, end_date)
    return


def plot_intercept_slope(parameter:str, df: pd.DataFrame, start_date: str, end_date: str):
    parameter = parameter.lower()
    parameters_extended = [
        f'standard_slope_{parameter}',
        f'standard_intercept_{parameter}',
        f'standard_r_square_{parameter}',
        f'number_of_standards_{parameter}']
    plot_with_subplots(df, parameters_extended, parameters_extended, start_date, end_date)
    return


def plot_fco2_in_situ(df: pd.DataFrame, start_date: str, end_date: str):
    parameters_extended = ['SST', 'SSS', 'fco2_wet_sst']
    plot_with_subplots(df, parameters_extended, parameters_extended, start_date, end_date)
    return


def plot_ch4_in_situ(df: pd.DataFrame, start_date: str, end_date: str):
    parameters_extended = ['SST', 'SSS', 'ch4_nmol_kg', 'pch4_wet_sst']
    plot_with_subplots(df, parameters_extended, parameters_extended, start_date, end_date)
    return








