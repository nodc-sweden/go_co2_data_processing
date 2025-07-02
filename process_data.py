import sys

from file_reader import (list_files, list_ferrybox_files, read_files_dynamic, read_standards,
                         read_ferrybox_files_dynamic, merge_go_and_ferrybox, combine_data)
from plot_co2_data import (plot_ship_track, plot_housekeeping_parameters, plot_standards,
                           plot_fco2_in_situ)
from flag import get_type_flags, range_check, constant_value, outlier_check, gradient_check
from prepare_standards import get_median_and_interpolate, get_standard_reference_value
from calculations import (correct_co2_based_on_standards, get_qff, get_delta_temperature, calculate_pco2_dry,
                          calculate_pco2_wet, calculate_fco2_wet, calculate_pco2_fco2_in_situ)
from export_results import export_fco2

# directory for CO2 files
# co2_folder = r'\\winfs-proj\data\proj\havgem\MOL\Teknikverksamheten\Transpaper_drift\16_CO2_data\DATA\2014\all_dat_files_2014'
# co2_folder = r'\\winfs-proj\data\proj\havgem\MOL\Teknikverksamheten\Transpaper_drift\16_CO2_data\DATA\2017\all_dat_files_2017'
# co2_folder = r'\\winfs-proj\data\proj\havgem\MOL\Teknikverksamheten\Transpaper_drift\16_CO2_data\DATA\2018\all_dat_files_2018'
# co2_folder = r'\\winfs-proj\data\proj\havgem\MOL\Teknikverksamheten\Transpaper_drift\16_CO2_data\DATA\2019\Leak'
# co2_folder = r'\\winfs-proj\data\proj\havgem\MOL\Teknikverksamheten\Transpaper_drift\16_CO2_data\DATA\2024'
co2_folder = r'\\winfs-proj\data\proj\havgem\MOL\Teknikverksamheten\Transpaper_drift\16_CO2_data\DATA\2025'

# list files in folder
co2_files = list_files(co2_folder)

# read co2 files in folder
df, df_extra = read_files_dynamic(co2_files)

# get start and end dates
if df.shape[0] > 0:
    start_time = df['time series'][0]
    end_time = df['time series'][-1]
else:
    sys.exit("Ingen data")

# directory for ferrybox files
fb_folder = r'\\Winfs\data\prod\Obs_Oceanografi\Arkiv\Ferrybox\txt'

# list ferrybox files
fb_list = list_ferrybox_files(fb_folder, start_time, end_time)

# read ferrybox files
df_fb = read_ferrybox_files_dynamic(fb_list)

# merge ferrybox with co2 data
df = df.to_pandas()
df_fb = df_fb.to_pandas()
df = merge_go_and_ferrybox(df, df_fb)
df = get_qff(df)
df = get_delta_temperature(df)

# # create combined Lat, Lon if some are missing.
df = combine_data(df)

# flag Type
df = get_type_flags(df)

# get quality flags: range check, constant value, outlier and gradient check
df = range_check(df)
df = constant_value(df)
df = outlier_check(df)
df = gradient_check(df)

# plot ship track
plot_ship_track(df)

# plot housekeeping parameters
plot_housekeeping_parameters(df)

# read certified standard gases
standards_path = r'\\winfs-proj\data\proj\havgem\MOL\Teknikverksamheten\Transpaper_drift\16_CO2_data\Standard gases\Standard_gases.xlsx'
df_stds = read_standards(standards_path)
df = get_standard_reference_value(df, df_stds)
standards = []
for item in ['1', '2', '3', '4', '5']:
    if df['Type'].str.contains(item).any():
        df = get_median_and_interpolate(df, item)
        standards.append(item)

# plot standards
plot_standards(df)

# correct measurements using standards
# define calibration and standard threshold, i.e. acceptable limits for how much calibrated xco2 may differ from
# measured values and how much the reference standard values may differ from measured values.
# to get data from 2025 a limit of 20 ppm is necessary for the period Jan-Apr... this is a highly questionable limit...
# QuinCe limit is 4 ppm, default here is set to 10 ppm.
df = correct_co2_based_on_standards(df, standards, 10, 10)

# calculate partial pressure of co2 for dry air
is_valid_equ = df['QF xco2_cal'] & df['QF period'] & df['QF licor flow'] & df['QF H2O flow']
is_valid_atm = df['QF xco2_cal'] & df['QF period'] & df['QF licor flow']
df = calculate_pco2_dry(df, is_valid_equ, is_valid_atm)

# calculate partial pressure of co2 for wet air
is_valid_equ &= df['QF equ temp']
df = calculate_pco2_wet(df, is_valid_equ, is_valid_atm)

# calculate fugacity of co2 for wet air
df = calculate_fco2_wet(df, is_valid_equ, is_valid_atm)

# calculate fugacity of co2 for wet air at sea surface temperature
df = calculate_pco2_fco2_in_situ(df, is_valid_equ)

# plot fco2 wet at in situ temperature together with in situ temperature and salinity
plot_fco2_in_situ(df)

# export fco2 data
export_fco2(df)






