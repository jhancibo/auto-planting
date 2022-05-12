# Constants and parameters needed for the analysis

# %%
# Version of SALUS currently being used
SALUS_VERSION = "salus-11.3.4"  # salus-11.3.4 or salus-11.2.6

# Local path to the tabulated Excel files
data_local_dir = "./jeremy_tillage_data/"

# S3 path where SALUS results will be read from
salus_s3_dir = "/com-cibo-continuum-storage/v1/ParcelRunnerToCsv/geomancer/tillage/parcels/"

# Names of error/warning/info files written to working directory by the script
err_file_depths = "ERROR_nonequal_initial_vs_final_depths.csv"
info_file_elts_trt = "INFO_n_elts_averaged_per_trt.csv"
warning_file_nonequal_dur = "WARNING_nonequal_practice_durations.csv"
warning_file_na_rows = "WARNING_na_rows_removed_during_profile_agg.csv"

# Organic C to organic matter conversion factor
OC_TO_OM_FACTOR = 1.724

# Compare atomic numbers of CO2 and Carbon
C_TO_CO2 = 44 / 12

# Clay content (percent) for different soil texture classes
USDA_CLAY = {'sand': 5,
             'loamy fine sand': 6,
             'loamy sand': 6,
             'fine sandy loam': 15,
             'loam': 20,
             'sandy loam': 10,
             'silt loam': 15,
             'silty loam': 15,
             'sandy clay loam': 30,
             'clay loam': 35,
             'clay_loam': 35,
             'silty clay loam': 37,
             'silty clay': 45,
             'clay': 50
             }
