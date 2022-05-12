# Does average harvest allow crops to mature?
# Case 1: Run SALUS with average planting and average harvest dates.
# Case 2: Run SALUS with average planting, but auto-harvest. 
# Case 3: Run SALUS with auto-planting and auto-harvest. 
# Extract the date the crop matured and the yield from the results of the three cases. Answer the questions: 
# How do yields compare between Case 1 and Case 2?
# How do yields compare between Case 1 and Case 3?
# On what fraction of parcels is the Case 2 maturity date after the Case 1 harvest date?
# If there are differences, is there a temporal pattern? (i.e. Does this happen in some years, but not others?)
# If there are differences, is there a geographic pattern? (i.e. Does this happen in some places, but not others?)
# Making 1:1 plots will be helpful for the first three questions. Use correlation_scatter_with_stats() from:
# https://github.com/cibotech/science-org/blob/master/python_utils/plotting/correlation_plots.py 

# auto-planting
# aws s3 sync s3://com-cibo-continuum-storage/v1/ParcelRunnerToCsv/jkeillor/planting-dates-harvest/parcels/ /Users/james/Downloads/jeremy-auto-planting --exclude "*" --include "*zoneResults*"
# This script is for downloading all tillage results from Jeremy new tillage folder and aggregate them in a single csv.
# %% LIBRARIES
import os
from IPython.display import display
#import pandas as pd
import pandas as pd
#import modin.pandas as pd
import numpy as np
import altair as alt
import json
from pathlib import Path
from s3path import S3Path  # S3 version of Path (compatible with pathlib)
import pint_pandas
import logging
import analysis_utils_tillage as au
from analysis_config import SALUS_VERSION, data_local_dir, salus_s3_dir
import csv, glob 
# S3 path where SALUS results will be read from
SALUS_RESULTS_S3_PATH = S3Path(salus_s3_dir)
# Local path to the tabulated Excel files
TABULATED_DATA_DIR = Path(data_local_dir)
import seaborn as sns #visualisation
import matplotlib.pyplot as plt
from pandarallel import pandarallel
sns.set(color_codes=True)
#pd.set_option('display.max_columns', 100)
#pd.set_option('display.max_rows', 200)
from glob import glob
from ciboplot import *
import matplotlib.pyplot as plt
from altair_saver import save
import correlation_plots as cp
from datetime import datetime

# %%
# Function for reading the dataset from Jeremy for tillage depht and tillage parameters sensitivities
def read_data(FOLDER):
    """
    Args:
        FOLDER (_type_): _description_
    """
    PATH = f"/Users/james/Downloads/{FOLDER}/"
    EXT = "zoneResults.csv"
    for path, subdir, files in os.walk(PATH):
        print("---------------")
        print("Path", path)
        print("Subdir", subdir)
        print("Files", files)
    all_csv_files = []
    for path, subdir, files in os.walk(PATH):
        for file in glob(os.path.join(path, EXT)):
            all_csv_files.append(file)
    df = pd.concat((pd.read_csv(f) for f in all_csv_files))
    df.to_csv(f"./tabulated_data/{FOLDER}.csv")
    return df

# %%
df = read_data("jeremy-auto-planting") # aws s3 cp --recursive s3://com-cibo-continuum-storage/v1/ParcelRunnerToCsv/jkeillor/tillage-implements-default-depth/ /Users/james/Downloads/jeremy-tillage-implements-default-depth # with Offset Disk

# %%
df = df[['experimentDescription', 'uuid','stateCode','countyFIPS','centerX',	'centerY','area','locationId', 'cropId','plantingDate','harvestDate', 'harvestYear','plantingPopulation','cumulativeThermTime','cumulativePrecipitation','irrigated','soilClay','soilSilt','soilOC',
                     'ESAD',	'FOMBl',	'FOMAb',	'C_StDead',	'C_Out',	'C_In',	'BD',	'SWCN',	'SW',	'ST',	'C_ActOrg',	'C_SloOrg',	'C_ResOrg',	'CWAD',	'GWAD', 'soilDepth','soilBulkDensity','maturityDate']]

# %%
print(df.isnull().sum())
# %%
df.count()
# %%
df = df.dropna()    # Dropping the missing values.
df.count()

# %%
def add_digit(a_number):
    number_str = str(int(a_number))
    zero_filled_number = number_str.zfill(3)
    return zero_filled_number

def state_2_code(state):
    try:
        state_codes = {
    'WA': '53', 'DE': '10', 'DC': '11', 'WI': '55', 'WV': '54', 'HI': '15',
    'FL': '12', 'WY': '56', 'PR': '72', 'NJ': '34', 'NM': '35', 'TX': '48',
    'LA': '22', 'NC': '37', 'ND': '38', 'NE': '31', 'TN': '47', 'NY': '36',
    'PA': '42', 'AK': '02', 'NV': '32', 'NH': '33', 'VA': '51', 'CO': '08',
    'CA': '06', 'AL': '01', 'AR': '05', 'VT': '50', 'IL': '17', 'GA': '13',
    'IN': '18', 'IA': '19', 'MA': '25', 'AZ': '04', 'ID': '16', 'CT': '09',
    'ME': '23', 'MD': '24', 'OK': '40', 'OH': '39', 'UT': '49', 'MO': '29',
    'MN': '27', 'MI': '26', 'RI': '44', 'KS': '20', 'MT': '30', 'MS': '28',
    'SC': '45', 'KY': '21', 'OR': '41', 'SD': '46'
}
        x = state_codes.get(state)
    except:
        print("State code not found!")
    return x
# %%
df['str_county_code'] = df.apply(lambda row: add_digit(row.countyFIPS), axis=1)
df['state_code'] = df.apply(lambda row: state_2_code(row.stateCode),axis=1)
df['new_countyFIPS'] = df["state_code"].astype(str) + df["str_county_code"].astype(str)
df['new_countyFIPS'].head()

# %%
# %%
df  = df[["uuid","new_countyFIPS","experimentDescription",'cropId',"harvestYear",'plantingDate','maturityDate','harvestDate','plantingPopulation','CWAD','GWAD']]
# %%
# %%
df_MZ = df[df['cropId'].str.contains('MZ')]
df_SB = df[df['cropId'].str.contains('SB')]
frames = [df_MZ,df_SB]
df_yield=pd.concat(frames)
# %%
# for corn median yield
df_MZ_median = df_MZ.groupby(['uuid','experimentDescription','cropId'])['GWAD'].median()
# df_MZ_median = df_SB.groupby(['uuid','experimentDescription','cropId',"new_countyFIPS"])['GWAD'].median()
df_MZ_median = df_MZ_median.to_frame().reset_index()
# %%
df_Auto_Planting_Auto_Harvest = df_MZ_median.query('experimentDescription=="Auto_Planting_Auto_Harvest"')
df_Avg_Planting_Auto_Harvest = df_MZ_median.query('experimentDescription=="Avg_Planting_Auto_Harvest"')
df_Avg_Planting_Avg_Harvest = df_MZ_median.query('experimentDescription=="Avg_Planting_Avg_Harvest"')

# %%
#import correlation_plots as cp

# %%
merge_0 = pd.merge(df_Avg_Planting_Avg_Harvest, df_Avg_Planting_Auto_Harvest, on="uuid", how="inner")

# %%
%matplotlib inline 
#cp.correlation_scatter_with_stats(df_Avg_Planting_Avg_Harvest["GWAD"],df_Avg_Planting_Auto_Harvest["GWAD"],group=pd.Series(),marker_size = 8,xlab='Avg_Planting_Avg_Harvest',ylab='Avg_Planting_Auto_Harvest',title='Avg_Planting_Avg_Harvest vs. Avg_Planting_Auto_Harvest')
cp.correlation_scatter_with_stats(merge_0["GWAD_x"],merge_0["GWAD_y"],group=pd.Series(),marker_size = 8,xlab='Avg_Planting_Avg_Harvest',ylab='Avg_Planting_Auto_Harvest',title='Corn Avg_Planting_Avg_Harvest vs. Avg_Planting_Auto_Harvest')
# %%
merge = pd.merge(df_Avg_Planting_Avg_Harvest, df_Auto_Planting_Auto_Harvest, on="uuid", how="inner")

# %%
%matplotlib inline 
#cp.correlation_scatter_with_stats(df_Avg_Planting_Avg_Harvest["GWAD"],df_Auto_Planting_Auto_Harvest["GWAD"],group=pd.Series(),marker_size = 8,xlab='Avg_Planting_Avg_Harvest',ylab='df_Auto_Planting_Auto_Harvest',title='Avg_Planting_Avg_Harvest vs. Avg_Planting_Auto_Harvest')
cp.correlation_scatter_with_stats(merge["GWAD_x"], merge["GWAD_y"],group=pd.Series(),marker_size = 8,xlab='Avg_Planting_Avg_Harvest',ylab='Auto_Planting_Auto_Harvest',title='Corn Avg_Planting_Avg_Harvest vs. Auto_Planting_Auto_Harvest')
# %%
merge_1 = pd.merge(df_Avg_Planting_Auto_Harvest, df_Auto_Planting_Auto_Harvest, on="uuid", how="inner")
# %%
%matplotlib inline 
cp.correlation_scatter_with_stats(merge_1["GWAD_x"], merge_1["GWAD_y"],group=pd.Series(),marker_size = 8,xlab='Avg_Planting_Auto_Harvest',ylab='Auto_Planting_Auto_Harvest',title='Corn Avg_Planting_Auto_Harvest vs. Auto_Planting_Auto_Harvest')
# %%

################################################################# 
# Soybean
# %%
df_SB = df[df['cropId'].str.contains('SB')]
# %%
# for corn median yield
df_SB_median = df_SB.groupby(['uuid','experimentDescription','cropId'])['GWAD'].median()
df_SB_median = df_SB_median.to_frame().reset_index()
# %%
df_Auto_Planting_Auto_Harvest_SB = df_SB_median.query('experimentDescription=="Auto_Planting_Auto_Harvest"')
df_Avg_Planting_Auto_Harvest_SB = df_SB_median.query('experimentDescription=="Avg_Planting_Auto_Harvest"')
df_Avg_Planting_Avg_Harvest_SB = df_SB_median.query('experimentDescription=="Avg_Planting_Avg_Harvest"')

# %%
#import correlation_plots as cp

# %%
merge_0 = pd.merge(df_Avg_Planting_Avg_Harvest_SB, df_Avg_Planting_Auto_Harvest_SB, on="uuid", how="inner")

# %%
%matplotlib inline 
#cp.correlation_scatter_with_stats(df_Avg_Planting_Avg_Harvest["GWAD"],df_Avg_Planting_Auto_Harvest["GWAD"],group=pd.Series(),marker_size = 8,xlab='Avg_Planting_Avg_Harvest',ylab='Avg_Planting_Auto_Harvest',title='Avg_Planting_Avg_Harvest vs. Avg_Planting_Auto_Harvest')
cp.correlation_scatter_with_stats(merge_0["GWAD_x"],merge_0["GWAD_y"],group=pd.Series(),marker_size = 8,xlab='Avg_Planting_Avg_Harvest',ylab='Avg_Planting_Auto_Harvest',title='Soybean Avg_Planting_Avg_Harvest vs. Avg_Planting_Auto_Harvest')
# %%
merge = pd.merge(df_Avg_Planting_Avg_Harvest_SB, df_Auto_Planting_Auto_Harvest_SB, on="uuid", how="inner")

# %%
%matplotlib inline 
#cp.correlation_scatter_with_stats(df_Avg_Planting_Avg_Harvest["GWAD"],df_Auto_Planting_Auto_Harvest["GWAD"],group=pd.Series(),marker_size = 8,xlab='Avg_Planting_Avg_Harvest',ylab='df_Auto_Planting_Auto_Harvest',title='Avg_Planting_Avg_Harvest vs. Avg_Planting_Auto_Harvest')
cp.correlation_scatter_with_stats(merge["GWAD_x"], merge["GWAD_y"],group=pd.Series(),marker_size = 8,xlab='Avg_Planting_Avg_Harvest',ylab='Auto_Planting_Auto_Harvest',title='Soybean Avg_Planting_Avg_Harvest vs. Auto_Planting_Auto_Harvest')
# %%
merge_1 = pd.merge(df_Avg_Planting_Auto_Harvest_SB, df_Auto_Planting_Auto_Harvest_SB, on="uuid", how="inner")
# %%
%matplotlib inline 
#cp.correlation_scatter_with_stats(merge_1["GWAD_x"], merge_1["GWAD_y"],group=pd.Series(),marker_size = 8,xlab='Avg_Planting_Auto_Harvest',ylab='Auto_Planting_Auto_Harvest',title='Soybean Avg_Planting_Auto_Harvest vs. Auto_Planting_Auto_Harvest')

cp.correlation_scatter_with_stats(merge_1["GWAD_x"], merge_1["GWAD_y"],group=pd.Series(),marker_size = 8,xlab='Avg_Planting_Auto_Harvest',ylab='Auto_Planting_Auto_Harvest',title='Soybean Avg_Planting_Auto_Harvest vs. Auto_Planting_Auto_Harvest')

################### Corn raw data
# %%
# for corn parcel yield
MZ_parcel_Auto_Planting_Auto_Harvest = df_MZ.query('experimentDescription=="Auto_Planting_Auto_Harvest"')
MZ_parcel_Avg_Planting_Auto_Harvest = df_MZ.query('experimentDescription=="Avg_Planting_Auto_Harvest"')
MZ_parcel_Avg_Planting_Avg_Harvest = df_MZ.query('experimentDescription=="Avg_Planting_Avg_Harvest"')

# %%
merge = pd.merge(MZ_parcel_Avg_Planting_Avg_Harvest, MZ_parcel_Avg_Planting_Auto_Harvest, on=["uuid","harvestYear"], how="inner")
#merge = pd.merge(MZ_parcel_Avg_Planting_Avg_Harvest, MZ_parcel_Avg_Planting_Auto_Harvest, on=["uuid"], how="inner")

# %%
%matplotlib inline 
#cp.correlation_scatter_with_stats(df_Avg_Planting_Avg_Harvest["GWAD"],df_Avg_Planting_Auto_Harvest["GWAD"],group=pd.Series(),marker_size = 8,xlab='Avg_Planting_Avg_Harvest',ylab='Avg_Planting_Auto_Harvest',title='Avg_Planting_Avg_Harvest vs. Avg_Planting_Auto_Harvest')
#cp.correlation_scatter_with_stats(merge["GWAD_x"],merge["GWAD_y"],group=pd.Series(merge.harvestYear_x),marker_size = 8,xlab='Avg_Planting_Avg_Harvest',ylab='Avg_Planting_Auto_Harvest',title='Corn parcel Avg_Planting_Avg_Harvest vs. Avg_Planting_Auto_Harvest')
#cp.correlation_scatter_with_stats(merge["GWAD_x"],merge["GWAD_y"],group=pd.Series(merge.harvestYear_y),marker_size = 8,xlab='Avg_Planting_Avg_Harvest',ylab='Avg_Planting_Auto_Harvest',title='Corn parcel Avg_Planting_Avg_Harvest vs. Avg_Planting_Auto_Harvest')
cp.correlation_scatter_with_stats(merge["GWAD_x"],merge["GWAD_y"],group=pd.Series(merge.harvestYear),marker_size = 8,xlab='Avg_Planting_Avg_Harvest',ylab='Avg_Planting_Auto_Harvest',title='Corn parcel Avg_Planting_Avg_Harvest vs. Avg_Planting_Auto_Harvest')
# %%
merge_1 = pd.merge(MZ_parcel_Avg_Planting_Avg_Harvest, MZ_parcel_Auto_Planting_Auto_Harvest, on=["uuid","harvestYear"], how="inner")

# %%
%matplotlib inline 
cp.correlation_scatter_with_stats(merge_1["GWAD_x"],merge_1["GWAD_y"],group=merge_1.harvestYear,marker_size = 8,xlab='Avg_Planting_Avg_Harvest',ylab='Auto_Planting_Auto_Harvest',title='Corn parcel Avg_Planting_Avg_Harvest vs. Auto_Planting_Auto_Harvest')

# %%
merge_2 = pd.merge(MZ_parcel_Avg_Planting_Auto_Harvest, MZ_parcel_Auto_Planting_Auto_Harvest, on="uuid", how="inner")

# %%
#%matplotlib inline 
#cp.correlation_scatter_with_stats(merge_2["GWAD_x"],merge_2["GWAD_y"],group=pd.Series(),marker_size = 8,xlab='Avg_Planting_Auto_Harvest',ylab='Auto_Planting_Auto_Harvest',title='Corn parcel Avg_Planting_Auto_Harvest vs. Auto_Planting_Auto_Harvest')
cp.correlation_scatter_with_stats(merge_2["GWAD_x"],merge_2["GWAD_y"],group=pd.Series(),marker_size = 8,xlab='Avg_Planting_Auto_Harvest',ylab='Auto_Planting_Auto_Harvest',title='Corn parcel Avg_Planting_Auto_Harvest vs. Auto_Planting_Auto_Harvest')

### Soybean raw parcel
# %%
# for soybean parcel yield
SB_parcel_Auto_Planting_Auto_Harvest = df_SB.query('experimentDescription=="Auto_Planting_Auto_Harvest"')
SB_parcel_Avg_Planting_Auto_Harvest = df_SB.query('experimentDescription=="Avg_Planting_Auto_Harvest"')
SB_parcel_Avg_Planting_Avg_Harvest = df_SB.query('experimentDescription=="Avg_Planting_Avg_Harvest"')

# %%
merge_sb = pd.merge(SB_parcel_Avg_Planting_Avg_Harvest, SB_parcel_Avg_Planting_Auto_Harvest, on=["uuid","harvestYear"], how="inner")

# %%
%matplotlib inline 
cp.correlation_scatter_with_stats(merge_sb["GWAD_x"],merge_sb["GWAD_y"],group=pd.Series(merge_sb.harvestYear),marker_size = 8,xlab='Avg_Planting_Avg_Harvest',ylab='Avg_Planting_Auto_Harvest',title='Sobean parcel Avg_Planting_Avg_Harvest vs. Avg_Planting_Auto_Harvest')
# %%
merge_sb_1 = pd.merge(SB_parcel_Avg_Planting_Avg_Harvest, SB_parcel_Auto_Planting_Auto_Harvest, on=["uuid","harvestYear"], how="inner")

# %%
%matplotlib inline 
cp.correlation_scatter_with_stats(merge_sb_1["GWAD_x"],merge_sb_1["GWAD_y"],group=merge_sb_1.harvestYear,marker_size = 8,xlab='Avg_Planting_Avg_Harvest',ylab='Auto_Planting_Auto_Harvest',title='Soybean parcel Avg_Planting_Avg_Harvest vs. Auto_Planting_Auto_Harvest')


# %%
#df_MZ
MZ_Avg_Planting_Avg_Harvest = df_MZ.query('experimentDescription=="Avg_Planting_Avg_Harvest"')
MZ_Avg_Planting_Auto_Harvest = df_MZ.query('experimentDescription=="Avg_Planting_Auto_Harvest"')
MZ_Auto_Planting_Auto_Harvest = df_MZ.query('experimentDescription=="Auto_Planting_Auto_Harvest"')
merge_MZ_Avg_Avg_Avg_Auto = pd.merge(MZ_Avg_Planting_Avg_Harvest, MZ_Avg_Planting_Auto_Harvest, on=["uuid","harvestYear"], how="inner")
merge_MZ_Avg_Avg_Avg_Auto = merge_MZ_Avg_Avg_Avg_Auto.reset_index()
merge_MZ_Avg_Avg_Auto_Auto = pd.merge(MZ_Avg_Planting_Avg_Harvest, MZ_Auto_Planting_Auto_Harvest, on=["uuid", "harvestYear"], how="inner")
merge_MZ_Avg_Avg_Auto_Auto = merge_MZ_Avg_Avg_Auto_Auto.reset_index()
# %%
#merge_MZ_Avg_Avg_Avg_Auto_2003 = merge_MZ_Avg_Avg_Avg_Auto.query('harvestYear=="2003"')
merge_MZ_Avg_Avg_Avg_Auto_2003 = merge_MZ_Avg_Avg_Avg_Auto.loc[merge_MZ_Avg_Avg_Avg_Auto['harvestYear'] == 2003]
merge_MZ_Avg_Avg_Avg_Auto_2005 = merge_MZ_Avg_Avg_Avg_Auto.loc[merge_MZ_Avg_Avg_Avg_Auto['harvestYear'] == 2005]
merge_MZ_Avg_Avg_Avg_Auto_2007 = merge_MZ_Avg_Avg_Avg_Auto.loc[merge_MZ_Avg_Avg_Avg_Auto['harvestYear'] == 2007]
merge_MZ_Avg_Avg_Avg_Auto_2009 = merge_MZ_Avg_Avg_Avg_Auto.loc[merge_MZ_Avg_Avg_Avg_Auto['harvestYear'] == 2009]
merge_MZ_Avg_Avg_Avg_Auto_2011 = merge_MZ_Avg_Avg_Avg_Auto.loc[merge_MZ_Avg_Avg_Avg_Auto['harvestYear'] == 2011]
merge_MZ_Avg_Avg_Avg_Auto_2013 = merge_MZ_Avg_Avg_Avg_Auto.loc[merge_MZ_Avg_Avg_Avg_Auto['harvestYear'] == 2013]
merge_MZ_Avg_Avg_Avg_Auto_2015 = merge_MZ_Avg_Avg_Avg_Auto.loc[merge_MZ_Avg_Avg_Avg_Auto['harvestYear'] == 2015]
merge_MZ_Avg_Avg_Avg_Auto_2017 = merge_MZ_Avg_Avg_Avg_Auto.loc[merge_MZ_Avg_Avg_Avg_Auto['harvestYear'] == 2017]
merge_MZ_Avg_Avg_Avg_Auto_2019 = merge_MZ_Avg_Avg_Avg_Auto.loc[merge_MZ_Avg_Avg_Avg_Auto['harvestYear'] == 2019]

# %%
#df_SB
SB_Avg_Planting_Avg_Harvest = df_SB.query('experimentDescription=="Avg_Planting_Avg_Harvest"')
SB_Avg_Planting_Auto_Harvest = df_SB.query('experimentDescription=="Avg_Planting_Auto_Harvest"')
SB_Auto_Planting_Auto_Harvest = df_SB.query('experimentDescription=="Auto_Planting_Auto_Harvest"')
merge_SB_Avg_Avg_Avg_Auto = pd.merge(SB_Avg_Planting_Avg_Harvest, SB_Avg_Planting_Auto_Harvest, on=["uuid","harvestYear"], how="inner")
merge_SB_Avg_Avg_Avg_Auto = merge_SB_Avg_Avg_Avg_Auto.reset_index()
merge_SB_Avg_Avg_Auto_Auto = pd.merge(SB_Avg_Planting_Avg_Harvest, SB_Auto_Planting_Auto_Harvest, on=["uuid", "harvestYear"], how="inner")
merge_SB_Avg_Avg_Auto_Auto = merge_SB_Avg_Avg_Auto_Auto.reset_index()
# %%
#merge_MZ_Avg_Avg_Avg_Auto_2003 = merge_MZ_Avg_Avg_Avg_Auto.query('harvestYear=="2003"')
merge_SB_Avg_Avg_Avg_Auto_2003 = merge_SB_Avg_Avg_Avg_Auto.loc[merge_SB_Avg_Avg_Avg_Auto['harvestYear'] == 2004]
merge_SB_Avg_Avg_Avg_Auto_2005 = merge_SB_Avg_Avg_Avg_Auto.loc[merge_SB_Avg_Avg_Avg_Auto['harvestYear'] == 2006]
merge_SB_Avg_Avg_Avg_Auto_2007 = merge_SB_Avg_Avg_Avg_Auto.loc[merge_SB_Avg_Avg_Avg_Auto['harvestYear'] == 2008]
merge_SB_Avg_Avg_Avg_Auto_2009 = merge_SB_Avg_Avg_Avg_Auto.loc[merge_SB_Avg_Avg_Avg_Auto['harvestYear'] == 2010]
merge_SB_Avg_Avg_Avg_Auto_2011 = merge_SB_Avg_Avg_Avg_Auto.loc[merge_SB_Avg_Avg_Avg_Auto['harvestYear'] == 2012]
merge_SB_Avg_Avg_Avg_Auto_2013 = merge_SB_Avg_Avg_Avg_Auto.loc[merge_SB_Avg_Avg_Avg_Auto['harvestYear'] == 2014]
merge_SB_Avg_Avg_Avg_Auto_2015 = merge_SB_Avg_Avg_Avg_Auto.loc[merge_SB_Avg_Avg_Avg_Auto['harvestYear'] == 2016]
merge_SB_Avg_Avg_Avg_Auto_2017 = merge_SB_Avg_Avg_Avg_Auto.loc[merge_SB_Avg_Avg_Avg_Auto['harvestYear'] == 2018]


# %%
%matplotlib inline 
cp.correlation_scatter_with_stats(merge_MZ_Avg_Avg_Avg_Auto_2003["GWAD_x"],merge_MZ_Avg_Avg_Avg_Auto_2003["GWAD_y"],group=pd.Series(),marker_size = 8,xlab='2003_Avg_Planting_Auto_Harvest',ylab='2003_Auto_Planting_Auto_Harvest',title='2003 Corn parcel Avg_Planting_Auto_Harvest vs. Auto_Planting_Auto_Harvest')

# %%
%matplotlib inline 
cp.correlation_scatter_with_stats(merge_MZ_Avg_Avg_Avg_Auto_2005["GWAD_x"],merge_MZ_Avg_Avg_Avg_Auto_2005["GWAD_y"],group=pd.Series(),marker_size = 8,xlab='2005_Avg_Planting_Auto_Harvest',ylab='2005_Auto_Planting_Auto_Harvest',title='2005 Corn parcel Avg_Planting_Auto_Harvest vs. Auto_Planting_Auto_Harvest')

# %%
%matplotlib inline 
cp.correlation_scatter_with_stats(merge_MZ_Avg_Avg_Avg_Auto_2007["GWAD_x"],merge_MZ_Avg_Avg_Avg_Auto_2007["GWAD_y"],group=pd.Series(),marker_size = 8,xlab='2007_Avg_Planting_Auto_Harvest',ylab='2007_Auto_Planting_Auto_Harvest',title='2007 Corn parcel Avg_Planting_Auto_Harvest vs. Auto_Planting_Auto_Harvest')
# %%

my_year = 2009
cp.correlation_scatter_with_stats(merge_MZ_Avg_Avg_Avg_Auto_2009["GWAD_x"],merge_MZ_Avg_Avg_Avg_Auto_2009["GWAD_y"],group=pd.Series(),marker_size = 8,xlab='2009_Avg_Planting_Auto_Harvest',ylab='2009_Auto_Planting_Auto_Harvest',title='2009 Corn parcel Avg_Planting_Auto_Harvest vs. Auto_Planting_Auto_Harvest')
plt.savefig(f"{my_year}_Corn parcel Avg_Planting_Auto_Harvest vs. Auto_Planting_Auto_Harvest.png")

# %%
from matplotlib import pyplot as plt
fig, ax = plt.subplots(figsize =(10, 7))
ax = plt.gca()
ax.relim()
ax.autoscale_view()
my_year = 2017
graph = cp.correlation_scatter_with_stats(merge_MZ_Avg_Avg_Avg_Auto_2017["GWAD_x"],merge_MZ_Avg_Avg_Avg_Auto_2017["GWAD_y"],group=pd.Series(),marker_size = 8,xlab='2017_Avg_Planting_Auto_Harvest',ylab='2017_Auto_Planting_Auto_Harvest',title='2017 Corn parcel Avg_Planting_Auto_Harvest vs. Auto_Planting_Auto_Harvest')
#save(graph, f"./graph/MZ_Avg_Avg_Avg_Auto/{my_year}_Corn parcel Avg_Planting_Auto_Harvest vs. Auto_Planting_Auto_Harvest.png")
#plt.savefig(f"{my_year}_Corn parcel Avg_Planting_Auto_Harvest vs. Auto_Planting_Auto_Harvest.pdf")
#fig.suptitle(f"{my_year}_Corn parcel Avg_Planting_Auto_Harvest vs. Auto_Planting_Auto_Harvest")
fig.savefig(f"./graph/MZ_Avg_Avg_Avg_Auto/{my_year}_Corn parcel Avg_Planting_Auto_Harvest vs. Auto_Planting_Auto_Harvest.png",facecolor='w', edgecolor='w',orientation='portrait')

# %%
def get_MZ_Avg_Avg_Avg_Auto(merged_crop_df,year_list):
    from matplotlib import pyplot as plt
    for year in year_list:
        plt.close('all')
        #plt.clf()
        df_MZ_Avg_Avg_Avg_Auto = merged_crop_df.query(f'harvestYear=={year}')
        fig, ax = plt.subplots(figsize =(10, 7))
        ax = plt.gca()
        ax.relim()
        ax.autoscale_view()
        graph = cp.correlation_scatter_with_stats(df_MZ_Avg_Avg_Avg_Auto["GWAD_x"],df_MZ_Avg_Avg_Avg_Auto["GWAD_y"],group=pd.Series(),marker_size = 8,xlab=f'{year}_Avg_Planting_Avg_Harvest',ylab=f'{year}_Avg_Planting_Auto_Harvest',title=f'{year} Corn parcel Avg_Planting_Avg_Harvest vs. Avg_Planting_Auto_Harvest')
        fig.savefig(f"./graph/MZ_Avg_Avg_Avg_Auto/{year}_Corn parcel Avg_Planting_Avg_Harvest vs. Avg_Planting_Auto_Harvest.png",facecolor='w', edgecolor='w',orientation='portrait')
        #plt.close(fig)

# %%
year_list = [2003, 2005, 2007, 2009, 2011, 2013, 2015, 2017, 2019]
get_MZ_Avg_Avg_Avg_Auto(merge_MZ_Avg_Avg_Avg_Auto, year_list)

# %%
def get_MZ_Avg_Avg_Auto_Auto(merged_crop_df,year_list):
    from matplotlib import pyplot as plt
    for year in year_list:
        plt.close('all')
        #plt.clf()
        df_MZ_Avg_Avg_Auto_Auto = merged_crop_df.query(f'harvestYear=={year}')
        fig, ax = plt.subplots(figsize =(10, 7))
        ax = plt.gca()
        ax.relim()
        ax.autoscale_view()
        graph = cp.correlation_scatter_with_stats(df_MZ_Avg_Avg_Auto_Auto["GWAD_x"],df_MZ_Avg_Avg_Auto_Auto["GWAD_y"],group=pd.Series(),marker_size = 8,xlab=f'{year}_Avg_Planting_Avg_Harvest',ylab=f'{year}_Auto_Planting_Auto_Harvest',title=f'{year} Corn parcel Avg_Planting_Avg_Harvest vs. Auto_Planting_Auto_Harvest')
        fig.savefig(f"./graph/MZ_Avg_Avg_Auto_Auto/{year}_Corn parcel Avg_Planting_Avg_Harvest vs. Auto_Planting_Auto_Harvest.png",facecolor='w', edgecolor='w',orientation='portrait')
        #plt.close(fig)

# %%
year_list = [2003, 2005, 2007, 2009, 2011, 2013, 2015, 2017, 2019]
get_MZ_Avg_Avg_Auto_Auto(merge_MZ_Avg_Avg_Auto_Auto, year_list)

################################################################
# %%
def get_SB_Avg_Avg_Avg_Auto(merged_crop_df,year_list):
    from matplotlib import pyplot as plt
    for year in year_list:
        plt.close('all')
        #plt.clf()
        df_SB_Avg_Avg_Avg_Auto = merged_crop_df.query(f'harvestYear=={year}')
        fig, ax = plt.subplots(figsize =(10, 7))
        ax = plt.gca()
        ax.relim()
        ax.autoscale_view()
        graph = cp.correlation_scatter_with_stats(df_SB_Avg_Avg_Avg_Auto["GWAD_x"],df_SB_Avg_Avg_Avg_Auto["GWAD_y"],group=pd.Series(),marker_size = 8,xlab=f'{year}_Avg_Planting_Avg_Harvest',ylab=f'{year}_Avg_Planting_Auto_Harvest',title=f'{year} Soybean parcel Avg_Planting_Avg_Harvest vs. Avg_Planting_Auto_Harvest')
        fig.savefig(f"./graph/SB_Avg_Avg_Avg_Auto/{year}_Soybean parcel Avg_Planting_Avg_Harvest vs. Avg_Planting_Auto_Harvest.png",facecolor='w', edgecolor='w',orientation='portrait')
        #plt.close(fig)

# %%
year_list = [2004, 2006, 2008, 2010, 2012, 2014, 2016, 2018]
get_SB_Avg_Avg_Avg_Auto(merge_SB_Avg_Avg_Avg_Auto, year_list)

# %%
def get_SB_Avg_Avg_Auto_Auto(merged_crop_df,year_list):
    from matplotlib import pyplot as plt
    for year in year_list:
        plt.close('all')
        #plt.clf()
        df_SB_Avg_Avg_Auto_Auto = merged_crop_df.query(f'harvestYear=={year}')
        fig, ax = plt.subplots(figsize =(10, 7))
        ax = plt.gca()
        ax.relim()
        ax.autoscale_view()
        graph = cp.correlation_scatter_with_stats(df_SB_Avg_Avg_Auto_Auto["GWAD_x"],df_SB_Avg_Avg_Auto_Auto["GWAD_y"],group=pd.Series(),marker_size = 8,xlab=f'{year}_Avg_Planting_Avg_Harvest',ylab=f'{year}_Auto_Planting_Auto_Harvest',title=f'{year} Soybean parcel Avg_Planting_Avg_Harvest vs. Auto_Planting_Auto_Harvest')
        fig.savefig(f"./graph/SB_Avg_Avg_Auto_Auto/{year}_Soybean parcel Avg_Planting_Avg_Harvest vs. Auto_Planting_Auto_Harvest.png",facecolor='w', edgecolor='w',orientation='portrait')
        #plt.close(fig)

# %%
year_list = [2004, 2006, 2008, 2010, 2012, 2014, 2016, 2018]
get_SB_Avg_Avg_Auto_Auto(merge_SB_Avg_Avg_Auto_Auto, year_list)

################################################################
# %%
# Q3: On what fraction of parcels is the Case 2 maturity date after the Case 1 harvest date?
df_MZ = df[df['cropId'].str.contains('MZ')]
df_SB = df[df['cropId'].str.contains('SB')]
# %%
#df_MZ
MZ_Avg_Planting_Avg_Harvest = df_MZ.query('experimentDescription=="Avg_Planting_Avg_Harvest"') # Case 1
MZ_Avg_Planting_Auto_Harvest = df_MZ.query('experimentDescription=="Avg_Planting_Auto_Harvest"') # Case 2
merge_MZ_Avg_Avg_Avg_Auto = pd.merge(MZ_Avg_Planting_Avg_Harvest, MZ_Avg_Planting_Auto_Harvest, on=["uuid","harvestYear","new_countyFIPS","cropId","plantingDate"], how="inner")
merge_MZ_Avg_Avg_Avg_Auto = merge_MZ_Avg_Avg_Avg_Auto.reset_index()

# %% 
merge_MZ_Avg_Avg_Avg_Auto['maturity_obj_Avg_Auto'] =  pd.to_datetime(merge_MZ_Avg_Avg_Avg_Auto['maturityDate_y'])
merge_MZ_Avg_Avg_Avg_Auto['doy_maturity_Avg_Auto'] = merge_MZ_Avg_Avg_Avg_Auto['maturity_obj_Avg_Auto'].dt.dayofyear

# %%
merge_MZ_Avg_Avg_Avg_Auto['harvest_obj_Avg_Avg'] =  pd.to_datetime(merge_MZ_Avg_Avg_Avg_Auto['harvestDate_x'])
merge_MZ_Avg_Avg_Avg_Auto['doy_harvest_Avg_Avg'] = merge_MZ_Avg_Avg_Avg_Auto['harvest_obj_Avg_Avg'].dt.dayofyear

# %%
merge_MZ_Avg_Avg_Avg_Auto["maturityDoy_harvestYearDoy"] = merge_MZ_Avg_Avg_Avg_Auto.apply(lambda row: round((row['doy_maturity_Avg_Auto']/row['doy_harvest_Avg_Avg'])*100,1), axis=1)

# %% 
merge_MZ_Avg_Avg_Avg_Auto["greater_100"] = merge_MZ_Avg_Avg_Avg_Auto.apply(lambda row: "Yes" if row.maturityDoy_harvestYearDoy>100 else "NO", axis=1)

# %%
A = merge_MZ_Avg_Avg_Avg_Auto.query('greater_100 == "Yes"').greater_100.count()
B = merge_MZ_Avg_Avg_Avg_Auto.shape
corn_percent = A/B

# %%
%matplotlib inline
sns.displot(merge_MZ_Avg_Avg_Avg_Auto, x="maturityDoy_harvestYearDoy", binwidth=3)
plt.xlabel("Percent (%)")
plt.title("Corn MaturityDoy / HarvestYearDoy")

# %%
#df_SB
SB_Avg_Planting_Avg_Harvest = df_SB.query('experimentDescription=="Avg_Planting_Avg_Harvest"') # Case 1
SB_Avg_Planting_Auto_Harvest = df_SB.query('experimentDescription=="Avg_Planting_Auto_Harvest"') # Case 2
merge_SB_Avg_Avg_Avg_Auto = pd.merge(SB_Avg_Planting_Avg_Harvest, SB_Avg_Planting_Auto_Harvest, on=["uuid","harvestYear","new_countyFIPS","cropId","plantingDate"], how="inner")
merge_SB_Avg_Avg_Avg_Auto = merge_SB_Avg_Avg_Avg_Auto.reset_index()

# %% 
merge_SB_Avg_Avg_Avg_Auto['maturity_obj_Avg_Auto'] =  pd.to_datetime(merge_SB_Avg_Avg_Avg_Auto['maturityDate_y'])
merge_SB_Avg_Avg_Avg_Auto['doy_maturity_Avg_Auto'] = merge_SB_Avg_Avg_Avg_Auto['maturity_obj_Avg_Auto'].dt.dayofyear

# %%
merge_SB_Avg_Avg_Avg_Auto['harvest_obj_Avg_Avg'] =  pd.to_datetime(merge_SB_Avg_Avg_Avg_Auto['harvestDate_x'])
merge_SB_Avg_Avg_Avg_Auto['doy_harvest_Avg_Avg'] = merge_SB_Avg_Avg_Avg_Auto['harvest_obj_Avg_Avg'].dt.dayofyear

# %%
merge_SB_Avg_Avg_Avg_Auto["maturityDoy_harvestYearDoy"] = merge_SB_Avg_Avg_Avg_Auto.apply(lambda row: round((row['doy_maturity_Avg_Auto']/row['doy_harvest_Avg_Avg'])*100,1), axis=1)

# %% 
merge_SB_Avg_Avg_Avg_Auto["greater_100"] = merge_SB_Avg_Avg_Avg_Auto.apply(lambda row: "Yes" if row.maturityDoy_harvestYearDoy>100 else "NO", axis=1)

# %%
C = merge_SB_Avg_Avg_Avg_Auto.query('greater_100 == "Yes"').greater_100.count()
D = merge_SB_Avg_Avg_Avg_Auto.shape
soybean_percent = C/D


# %%
%matplotlib inline 
sns.displot(merge_SB_Avg_Avg_Avg_Auto, x="maturityDoy_harvestYearDoy", binwidth=3)
plt.xlabel("Percent (%)")
plt.title("Soybean MaturityDoy / HarvestYearDoy")

# %%
# Q4: If there are differences, is there a temporal pattern? (i.e. Does this happen in some years, but not others?)
# %% calculate
def elegant(df):
    lm = df.apply(lambda row: round((row['doy_maturity_Avg_Auto']/row['doy_harvest_Avg_Avg'])*100,1), axis=1)
    return lm 
# %% 
# Corn yearly percent
merge_MZ_Avg_Avg_Avg_Auto_yealy = merge_MZ_Avg_Avg_Avg_Auto.groupby(["uuid","new_countyFIPS","harvestYear","cropId","plantingDate"]).apply(elegant)
# %%
merge_MZ_Avg_Avg_Avg_Auto_yealy = merge_MZ_Avg_Avg_Avg_Auto_yealy.reset_index()

# %%
merge_MZ_Avg_Avg_Avg_Auto_yealy.rename(columns = {0:'yearlyPecent'}, inplace = True)

# %%
def yearly_percent_corn(merge_MZ_Avg_Avg_Avg_Auto_yealy,year_list):
    for year in year_list:
      plt.close('all')
      corn_yearly_percent = merge_MZ_Avg_Avg_Avg_Auto_yealy.query(f'harvestYear=={year}')
      #corn_yearly_percent = corn_yearly_percent.query(f'harvestYear=={year}')
      graph = sns.displot(corn_yearly_percent, x="yearlyPecent", binwidth=3)
      plt.xlabel("Percent (%)")
      plt.title(f"{year} Corn MaturityDoy / HarvestYearDoy")
      graph.figure.savefig(f"./graph/corn_yearly_percent/{year}_corn_yearly_percent.png",facecolor='w', edgecolor='w',orientation='portrait')

# %% 
year_list = [2003, 2005, 2007, 2009, 2011, 2013, 2015, 2017, 2019]
yearly_percent_corn(merge_MZ_Avg_Avg_Avg_Auto_yealy, year_list)

# %% 
# Soybean yearly percent
merge_SB_Avg_Avg_Avg_Auto_yealy = merge_SB_Avg_Avg_Avg_Auto.groupby(["uuid","new_countyFIPS","harvestYear","cropId","plantingDate"]).apply(elegant)
# %%
merge_SB_Avg_Avg_Avg_Auto_yealy = merge_SB_Avg_Avg_Avg_Auto_yealy.reset_index()

# %%
merge_SB_Avg_Avg_Avg_Auto_yealy.rename(columns = {0:'yearlyPecent'}, inplace = True)

# %%
def yearly_percent_soybean(merge_SB_Avg_Avg_Avg_Auto_yealy,year_list):
    for year in year_list:
      plt.close('all')
      soybean_yearly_percent = merge_SB_Avg_Avg_Avg_Auto_yealy.query(f'harvestYear=={year}')
      #soybean_yearly_percent = soybean_yearly_percent.query(f'harvestYear=={year}')
      graph = sns.displot(soybean_yearly_percent, x="yearlyPecent", binwidth=3)
      plt.xlabel("Percent (%)")
      plt.title(f"{year} Soybean MaturityDoy / HarvestYearDoy")
      graph.figure.savefig(f"./graph/soybean_yearly_percent/{year}_soybean_yearly_percent.png",facecolor='w', edgecolor='w',orientation='portrait')

# %% 
year_list = [2004, 2006, 2008, 2010, 2012, 2014, 2016, 2018]
yearly_percent_soybean(merge_SB_Avg_Avg_Avg_Auto_yealy, year_list)
# %%
# Q5: If there are differences, is there a geographic pattern? (i.e. Does this happen in some places, but not others?)
from ciboplot import *
county_data = pd.read_csv('/Users/james/Downloads/county_data_for_viz.csv')
draw_map_states_or_counties(county_data,'countyFips','seasonalNitrogenApplied')

# %%
#  
from ciboplot import *
# %%
df = read_data("jeremy-auto-planting") # aws s3 cp --recursive s3://com-cibo-continuum-storage/v1/ParcelRunnerToCsv/jkeillor/tillage-implements-default-depth/ /Users/james/Downloads/jeremy-tillage-implements-default-depth # with Offset Disk

# %%
df = df[['experimentDescription', 'uuid','stateCode','countyFIPS','centerX',	'centerY','area','locationId', 'cropId','plantingDate','harvestDate', 'harvestYear','plantingPopulation','cumulativeThermTime','cumulativePrecipitation','irrigated','soilClay','soilSilt','soilOC',
                     'ESAD',	'FOMBl',	'FOMAb',	'C_StDead',	'C_Out',	'C_In',	'BD',	'SWCN',	'SW',	'ST',	'C_ActOrg',	'C_SloOrg',	'C_ResOrg',	'CWAD',	'GWAD', 'soilDepth','soilBulkDensity','maturityDate']]

# %%
df['str_county_code'] = df.apply(lambda row: add_digit(row.countyFIPS), axis=1)
df['state_code'] = df.apply(lambda row: state_2_code(row.stateCode),axis=1)
df['new_countyFIPS'] = df["state_code"].astype(str) + df["str_county_code"].astype(str)
df['new_countyFIPS'].head()

# %%
df  = df[["uuid","countyFIPS","stateCode","str_county_code","state_code","new_countyFIPS","experimentDescription",'cropId',"harvestYear",'plantingDate','maturityDate','harvestDate','plantingPopulation','CWAD','GWAD']]
# %%
# %%
df_MZ = df[df['cropId'].str.contains('MZ')]
df_SB = df[df['cropId'].str.contains('SB')]
frames = [df_MZ,df_SB]
df_yield=pd.concat(frames)

# %%
def yearly_percent_corn_cibo(merge_MZ_Avg_Avg_Avg_Auto_yealy,year_list):
    for year in year_list:
      plt.close('all')
      corn_yearly_percent = merge_MZ_Avg_Avg_Avg_Auto_yealy.query(f'harvestYear=={year}')
      graph = draw_map_states_or_counties(corn_yearly_percent,'new_countyFIPS','yearlyPecent',tooltip=['new_countyFIPS','yearlyPecent'],title=f"{year} Corn MaturityDoy/HarvestDoy (%)")
      #plt.xlabel("Percent (%)")
      #plt.title(f"{year} Corn MaturityDoy / HarvestYearDoy")
      graph.save(f"./graph/corn_cibo_map/{year}_corn_yearly_percent_map.png",facecolor='w', edgecolor='w',orientation='portrait')
# %%
year_list = year_list = [2003, 2005, 2007, 2009, 2011, 2013, 2015, 2017, 2019]
yearly_percent_corn_cibo(merge_MZ_Avg_Avg_Avg_Auto_yealy, year_list)
# %%
# %%
def yearly_percent_soybean_cibo(merge_SB_Avg_Avg_Avg_Auto_yealy,year_list):
    for year in year_list:
      plt.close('all')
      soybean_yearly_percent = merge_SB_Avg_Avg_Avg_Auto_yealy.query(f'harvestYear=={year}')
      graph = draw_map_states_or_counties(soybean_yearly_percent,'new_countyFIPS','yearlyPecent',tooltip=['new_countyFIPS','yearlyPecent'],title=f"{year} Soybean MaturityDoy/HarvestDoy (%)")
      #plt.xlabel("Percent (%)")
      #plt.title(f"{year} Corn MaturityDoy / HarvestYearDoy")
      graph.save(f"./graph/soybean_cibo_map/{year}_soybean_yearly_percent_map.png",facecolor='w', edgecolor='w',orientation='portrait')
# %%
year_list = year_list = [2004, 2006, 2008, 2010, 2012, 2014, 2016, 2018]
yearly_percent_soybean_cibo(merge_SB_Avg_Avg_Avg_Auto_yealy, year_list)
# %%


# %%
# Q6: Duration of planting to harvest and duration of planting to maturity to maturity for 1:1 plot
df_MZ = df[df['cropId'].str.contains('MZ')]
df_SB = df[df['cropId'].str.contains('SB')]



# %%
# CORN Planting to harvest (maturity) duration for avg avg avg auto
#df_MZ
MZ_Avg_Planting_Avg_Harvest = df_MZ.query('experimentDescription=="Avg_Planting_Avg_Harvest"') # Case 1 x 
MZ_Avg_Planting_Auto_Harvest = df_MZ.query('experimentDescription=="Avg_Planting_Auto_Harvest"') # Case 2 y
merge_MZ_Avg_Avg_Avg_Auto = pd.merge(MZ_Avg_Planting_Avg_Harvest, MZ_Avg_Planting_Auto_Harvest, on=["uuid","harvestYear","new_countyFIPS","cropId","plantingDate"], how="inner")
merge_MZ_Avg_Avg_Avg_Auto = merge_MZ_Avg_Avg_Avg_Auto.reset_index()
# %%
merge_MZ_Avg_Avg_Avg_Auto.head()
# %%
# %% 

merge_MZ_Avg_Avg_Avg_Auto['maturity_obj_Avg_Auto'] =  pd.to_datetime(merge_MZ_Avg_Avg_Avg_Auto['maturityDate_y']) # avg_auto
merge_MZ_Avg_Avg_Avg_Auto['doy_maturity_Avg_Auto'] = merge_MZ_Avg_Avg_Avg_Auto['maturity_obj_Avg_Auto'].dt.dayofyear # avg_auto

merge_MZ_Avg_Avg_Avg_Auto['harvest_obj_Avg_Auto'] =  pd.to_datetime(merge_MZ_Avg_Avg_Avg_Auto['harvestDate_y']) # avg_auto
merge_MZ_Avg_Avg_Avg_Auto['doy_harvest_Avg_Auto'] = merge_MZ_Avg_Avg_Avg_Auto['harvest_obj_Avg_Auto'].dt.dayofyear # avg_auto

merge_MZ_Avg_Avg_Avg_Auto['maturity_obj_Avg_Avg'] =  pd.to_datetime(merge_MZ_Avg_Avg_Avg_Auto['maturityDate_x']) # avg_avg
merge_MZ_Avg_Avg_Avg_Auto['doy_maturity_Avg_Avg'] = merge_MZ_Avg_Avg_Avg_Auto['maturity_obj_Avg_Avg'].dt.dayofyear # avg_avg

merge_MZ_Avg_Avg_Avg_Auto['harvest_obj_Avg_Avg'] =  pd.to_datetime(merge_MZ_Avg_Avg_Avg_Auto['harvestDate_x']) # avg_avg
merge_MZ_Avg_Avg_Avg_Auto['doy_harvest_Avg_Avg'] = merge_MZ_Avg_Avg_Avg_Auto['harvest_obj_Avg_Avg'].dt.dayofyear # avg_avg

merge_MZ_Avg_Avg_Avg_Auto['planting_obj'] =  pd.to_datetime(merge_MZ_Avg_Avg_Avg_Auto['plantingDate']) # avg_avg
merge_MZ_Avg_Avg_Avg_Auto['doy_planting'] = merge_MZ_Avg_Avg_Avg_Auto['planting_obj'].dt.dayofyear # avg_avg

# %%
#merge_MZ_Avg_Avg_Avg_Auto["maturityDoy_harvestYearDoy"] = merge_MZ_Avg_Avg_Avg_Auto.apply(lambda row: round((row['doy_maturity_Avg_Auto']/row['doy_harvest_Avg_Avg'])*100,1), axis=1)
merge_MZ_Avg_Avg_Avg_Auto["harvest_planting_Avg_Auto"] = merge_MZ_Avg_Avg_Avg_Auto.apply(lambda row: row['doy_harvest_Avg_Auto'] - row['doy_planting'], axis=1)
merge_MZ_Avg_Avg_Avg_Auto["maturity_planting_Avg_Auto"] = merge_MZ_Avg_Avg_Avg_Auto.apply(lambda row: row['doy_maturity_Avg_Auto'] - row['doy_planting'], axis=1)

merge_MZ_Avg_Avg_Avg_Auto["harvest_planting_Avg_Avg"] = merge_MZ_Avg_Avg_Avg_Auto.apply(lambda row: row['doy_harvest_Avg_Avg'] - row['doy_planting'], axis=1)
merge_MZ_Avg_Avg_Avg_Auto["maturity_planting_Avg_Avg"] = merge_MZ_Avg_Avg_Avg_Auto.apply(lambda row: row['doy_maturity_Avg_Avg'] - row['doy_planting'], axis=1)

# %% 
merge_MZ_Avg_Avg_Avg_Auto["delta_avg_auto"] = merge_MZ_Avg_Avg_Avg_Auto["harvest_planting_Avg_Auto"] - merge_MZ_Avg_Avg_Avg_Auto["maturity_planting_Avg_Auto"]
merge_MZ_Avg_Avg_Avg_Auto["delta_avg_avg"] = merge_MZ_Avg_Avg_Avg_Auto["harvest_planting_Avg_Avg"] - merge_MZ_Avg_Avg_Avg_Auto["maturity_planting_Avg_Avg"]

# %%
%matplotlib inline 
cp.correlation_scatter_with_stats(merge_MZ_Avg_Avg_Avg_Auto["harvest_planting_Avg_Avg"],merge_MZ_Avg_Avg_Avg_Auto["harvest_planting_Avg_Auto"],group=pd.Series(),marker_size = 8,xlab='Harvest-PlantingAvgPlantingAvgHarvest',ylab='Harvest-PlantingAvgPlantingAutoHarvest',title='Corn parcel Harvest-Planting Date Avg_Avg vs Avg_Auto')

# %%
%matplotlib inline
cp.correlation_scatter_with_stats(merge_MZ_Avg_Avg_Avg_Auto["maturity_planting_Avg_Avg"],merge_MZ_Avg_Avg_Avg_Auto["maturity_planting_Avg_Auto"],group=pd.Series(),marker_size = 8,xlab='Maturity-PlantingAvgPlantingAvgHarvest',ylab='Maturity-PlantingAvgPlantingAutoHarvest',title='Corn parcel Maturity-Planting Date Avg_Avg vs Avg_Auto')

# %%
%matplotlib inline
cp.correlation_scatter_with_stats(merge_MZ_Avg_Avg_Avg_Auto["delta_avg_avg"],merge_MZ_Avg_Avg_Avg_Auto["delta_avg_auto"],group=pd.Series(),marker_size = 8,xlab='Harvest-MaturityAvgPlantingAvgHarvest',ylab='Harvest-MaturityAvgPlantingAutoHarvest',title='Corn parcel Harvest-maturity Date Avg_Avg vs Avg_Auto')

# %%
################################################################# 
# Soybean Planting to harvest (maturity) duration for avg avg avg auto

# %%
#df_SB
SB_Avg_Planting_Avg_Harvest = df_SB.query('experimentDescription=="Avg_Planting_Avg_Harvest"') # Case 1 x 
SB_Avg_Planting_Auto_Harvest = df_SB.query('experimentDescription=="Avg_Planting_Auto_Harvest"') # Case 2 y
merge_SB_Avg_Avg_Avg_Auto = pd.merge(SB_Avg_Planting_Avg_Harvest, SB_Avg_Planting_Auto_Harvest, on=["uuid","harvestYear","new_countyFIPS","cropId","plantingDate"], how="inner")
merge_SB_Avg_Avg_Avg_Auto = merge_SB_Avg_Avg_Avg_Auto.reset_index()
# %%
merge_SB_Avg_Avg_Avg_Auto['maturity_obj_Avg_Auto'] =  pd.to_datetime(merge_SB_Avg_Avg_Avg_Auto['maturityDate_y']) # avg_auto
merge_SB_Avg_Avg_Avg_Auto['doy_maturity_Avg_Auto'] = merge_SB_Avg_Avg_Avg_Auto['maturity_obj_Avg_Auto'].dt.dayofyear # avg_auto

merge_SB_Avg_Avg_Avg_Auto['harvest_obj_Avg_Auto'] =  pd.to_datetime(merge_SB_Avg_Avg_Avg_Auto['harvestDate_y']) # avg_auto
merge_SB_Avg_Avg_Avg_Auto['doy_harvest_Avg_Auto'] = merge_SB_Avg_Avg_Avg_Auto['harvest_obj_Avg_Auto'].dt.dayofyear # avg_auto

merge_SB_Avg_Avg_Avg_Auto['maturity_obj_Avg_Avg'] =  pd.to_datetime(merge_SB_Avg_Avg_Avg_Auto['maturityDate_x']) # avg_avg
merge_SB_Avg_Avg_Avg_Auto['doy_maturity_Avg_Avg'] = merge_SB_Avg_Avg_Avg_Auto['maturity_obj_Avg_Avg'].dt.dayofyear # avg_avg

merge_SB_Avg_Avg_Avg_Auto['harvest_obj_Avg_Avg'] =  pd.to_datetime(merge_SB_Avg_Avg_Avg_Auto['harvestDate_x']) # avg_avg
merge_SB_Avg_Avg_Avg_Auto['doy_harvest_Avg_Avg'] = merge_SB_Avg_Avg_Avg_Auto['harvest_obj_Avg_Avg'].dt.dayofyear # avg_avg

merge_SB_Avg_Avg_Avg_Auto['planting_obj'] =  pd.to_datetime(merge_SB_Avg_Avg_Avg_Auto['plantingDate']) # avg_avg
merge_SB_Avg_Avg_Avg_Auto['doy_planting'] = merge_SB_Avg_Avg_Avg_Auto['planting_obj'].dt.dayofyear # avg_avg

# %%
#merge_MZ_Avg_Avg_Avg_Auto["maturityDoy_harvestYearDoy"] = merge_MZ_Avg_Avg_Avg_Auto.apply(lambda row: round((row['doy_maturity_Avg_Auto']/row['doy_harvest_Avg_Avg'])*100,1), axis=1)
merge_SB_Avg_Avg_Avg_Auto["harvest_planting_Avg_Auto"] = merge_SB_Avg_Avg_Avg_Auto.apply(lambda row: row['doy_harvest_Avg_Auto'] - row['doy_planting'], axis=1)
merge_SB_Avg_Avg_Avg_Auto["maturity_planting_Avg_Auto"] = merge_SB_Avg_Avg_Avg_Auto.apply(lambda row: row['doy_maturity_Avg_Auto'] - row['doy_planting'], axis=1)

merge_SB_Avg_Avg_Avg_Auto["harvest_planting_Avg_Avg"] = merge_SB_Avg_Avg_Avg_Auto.apply(lambda row: row['doy_harvest_Avg_Avg'] - row['doy_planting'], axis=1)
merge_SB_Avg_Avg_Avg_Auto["maturity_planting_Avg_Avg"] = merge_SB_Avg_Avg_Avg_Auto.apply(lambda row: row['doy_maturity_Avg_Avg'] - row['doy_planting'], axis=1)

# %%
%matplotlib inline 
cp.correlation_scatter_with_stats(merge_SB_Avg_Avg_Avg_Auto["harvest_planting_Avg_Avg"],merge_SB_Avg_Avg_Avg_Auto["harvest_planting_Avg_Auto"],group=pd.Series(),marker_size = 8,xlab='Harvest-PlantingAvgPlantingAvgHarvest',ylab='Harvest-PlantingAvgPlantingAutoHarvest',title='Soybean parcel Harvest-Planting Date Avg_Avg vs Avg_Auto')

# %%
%matplotlib inline
cp.correlation_scatter_with_stats(merge_SB_Avg_Avg_Avg_Auto["maturity_planting_Avg_Avg"],merge_SB_Avg_Avg_Avg_Auto["maturity_planting_Avg_Auto"],group=pd.Series(),marker_size = 8,xlab='Maturity-PlantingAvgPlantingAvgHarvest',ylab='Maturity-PlantingAvgPlantingAutoHarvest',title='Soybean parcel Maturity-Planting Date Avg_Avg vs Avg_Auto')

# %%
########################################################################
# average planting average harvest vs auto planting and auto harvest 

# %%
# CORN Planting to harvest (maturity) duration for avg avg auto auto
#df_MZ
MZ_Avg_Planting_Avg_Harvest = df_MZ.query('experimentDescription=="Avg_Planting_Avg_Harvest"') # Case 1 x 
MZ_Auto_Planting_Auto_Harvest = df_MZ.query('experimentDescription=="Auto_Planting_Auto_Harvest"') # Case 2 y
merge_MZ_Avg_Avg_Auto_Auto = pd.merge(MZ_Avg_Planting_Avg_Harvest, MZ_Auto_Planting_Auto_Harvest, on=["uuid","harvestYear","new_countyFIPS","cropId","plantingDate"], how="inner")
merge_MZ_Avg_Avg_Auto_Auto = merge_MZ_Avg_Avg_Auto_Auto.reset_index()
# %%
merge_MZ_Avg_Avg_Auto_Auto.head()
# %%
# %% 

merge_MZ_Avg_Avg_Auto_Auto['maturity_obj_Auto_Auto'] =  pd.to_datetime(merge_MZ_Avg_Avg_Auto_Auto['maturityDate_y']) # avg_auto
merge_MZ_Avg_Avg_Auto_Auto['doy_maturity_Auto_Auto'] = merge_MZ_Avg_Avg_Auto_Auto['maturity_obj_Auto_Auto'].dt.dayofyear # avg_auto

merge_MZ_Avg_Avg_Auto_Auto['harvest_obj_Auto_Auto'] =  pd.to_datetime(merge_MZ_Avg_Avg_Auto_Auto['harvestDate_y']) # avg_auto
merge_MZ_Avg_Avg_Auto_Auto['doy_harvest_Auto_Auto'] = merge_MZ_Avg_Avg_Auto_Auto['harvest_obj_Auto_Auto'].dt.dayofyear # avg_auto

merge_MZ_Avg_Avg_Auto_Auto['maturity_obj_Avg_Avg'] =  pd.to_datetime(merge_MZ_Avg_Avg_Auto_Auto['maturityDate_x']) # avg_avg
merge_MZ_Avg_Avg_Auto_Auto['doy_maturity_Avg_Avg'] = merge_MZ_Avg_Avg_Auto_Auto['maturity_obj_Avg_Avg'].dt.dayofyear # avg_avg

merge_MZ_Avg_Avg_Auto_Auto['harvest_obj_Avg_Avg'] =  pd.to_datetime(merge_MZ_Avg_Avg_Auto_Auto['harvestDate_x']) # avg_avg
merge_MZ_Avg_Avg_Auto_Auto['doy_harvest_Avg_Avg'] = merge_MZ_Avg_Avg_Auto_Auto['harvest_obj_Avg_Avg'].dt.dayofyear # avg_avg

merge_MZ_Avg_Avg_Auto_Auto['planting_obj'] =  pd.to_datetime(merge_MZ_Avg_Avg_Auto_Auto['plantingDate']) # avg_avg
merge_MZ_Avg_Avg_Auto_Auto['doy_planting'] = merge_MZ_Avg_Avg_Auto_Auto['planting_obj'].dt.dayofyear # avg_avg

# %%
#merge_MZ_Avg_Avg_Avg_Auto["maturityDoy_harvestYearDoy"] = merge_MZ_Avg_Avg_Avg_Auto.apply(lambda row: round((row['doy_maturity_Avg_Auto']/row['doy_harvest_Avg_Avg'])*100,1), axis=1)
merge_MZ_Avg_Avg_Auto_Auto["harvest_planting_Auto_Auto"] = merge_MZ_Avg_Avg_Auto_Auto.apply(lambda row: row['doy_harvest_Auto_Auto'] - row['doy_planting'], axis=1)
merge_MZ_Avg_Avg_Auto_Auto["maturity_planting_Auto_Auto"] = merge_MZ_Avg_Avg_Auto_Auto.apply(lambda row: row['doy_maturity_Auto_Auto'] - row['doy_planting'], axis=1)

merge_MZ_Avg_Avg_Auto_Auto["harvest_planting_Avg_Avg"] = merge_MZ_Avg_Avg_Auto_Auto.apply(lambda row: row['doy_harvest_Avg_Avg'] - row['doy_planting'], axis=1)
merge_MZ_Avg_Avg_Auto_Auto["maturity_planting_Avg_Avg"] = merge_MZ_Avg_Avg_Auto_Auto.apply(lambda row: row['doy_maturity_Avg_Avg'] - row['doy_planting'], axis=1)

# %%
merge_MZ_Avg_Avg_Auto_Auto["delta_auto_auto"] = merge_MZ_Avg_Avg_Auto_Auto["harvest_planting_Auto_Auto"] - merge_MZ_Avg_Avg_Auto_Auto["maturity_planting_Auto_Auto"]
merge_MZ_Avg_Avg_Auto_Auto["delta_avg_avg"] = merge_MZ_Avg_Avg_Auto_Auto["harvest_planting_Avg_Avg"] - merge_MZ_Avg_Avg_Auto_Auto["maturity_planting_Avg_Avg"]


# %%
%matplotlib inline 
cp.correlation_scatter_with_stats(merge_MZ_Avg_Avg_Auto_Auto["harvest_planting_Avg_Avg"],merge_MZ_Avg_Avg_Auto_Auto["harvest_planting_Auto_Auto"],group=pd.Series(),marker_size = 8,xlab='Harvest-PlantingAvgPlantingAvgHarvest',ylab='Harvest-PlantingAutoPlantingAutoHarvest',title='Corn parcel Harvest-Planting Date Avg_Avg vs Auto_Auto')

# %%
%matplotlib inline
cp.correlation_scatter_with_stats(merge_MZ_Avg_Avg_Auto_Auto["maturity_planting_Avg_Avg"],merge_MZ_Avg_Avg_Auto_Auto["maturity_planting_Auto_Auto"],group=pd.Series(),marker_size = 8,xlab='Maturity-PlantingAvgPlantingAvgHarvest',ylab='Maturity-PlantingAutoPlantingAutoHarvest',title='Corn parcel Maturity-Planting Date Avg_Avg vs Auto_Auto')

# %%
%matplotlib inline
cp.correlation_scatter_with_stats(merge_MZ_Avg_Avg_Auto_Auto["delta_avg_avg"],merge_MZ_Avg_Avg_Auto_Auto["delta_auto_auto"],group=pd.Series(),marker_size = 8,xlab='Harvest-maturityAvgPlantingAvgHarvest',ylab='Harvest-maturityAutoPlantingAutoHarvest',title='Corn parcel Harvest-maturity Date Avg_Avg vs Auto_Auto')

# %%
################################################################# 
# Soybean Planting to harvest (maturity) duration for avg avg auto auto

# %%
#df_SB
SB_Avg_Planting_Avg_Harvest = df_SB.query('experimentDescription=="Avg_Planting_Avg_Harvest"') # Case 1 x 
SB_Auto_Planting_Auto_Harvest = df_SB.query('experimentDescription=="Auto_Planting_Auto_Harvest"') # Case 2 y
merge_SB_Avg_Avg_Auto_Auto = pd.merge(SB_Avg_Planting_Avg_Harvest, SB_Auto_Planting_Auto_Harvest, on=["uuid","harvestYear","new_countyFIPS","cropId","plantingDate"], how="inner")
merge_SB_Avg_Avg_Auto_Auto = merge_SB_Avg_Avg_Auto_Auto.reset_index()
# %%
merge_SB_Avg_Avg_Auto_Auto['maturity_obj_Auto_Auto'] =  pd.to_datetime(merge_SB_Avg_Avg_Auto_Auto['maturityDate_y']) # avg_auto
merge_SB_Avg_Avg_Auto_Auto['doy_maturity_Auto_Auto'] = merge_SB_Avg_Avg_Auto_Auto['maturity_obj_Auto_Auto'].dt.dayofyear # avg_auto

merge_SB_Avg_Avg_Auto_Auto['harvest_obj_Auto_Auto'] =  pd.to_datetime(merge_SB_Avg_Avg_Auto_Auto['harvestDate_y']) # avg_auto
merge_SB_Avg_Avg_Auto_Auto['doy_harvest_Auto_Auto'] = merge_SB_Avg_Avg_Auto_Auto['harvest_obj_Auto_Auto'].dt.dayofyear # avg_auto

merge_SB_Avg_Avg_Auto_Auto['maturity_obj_Avg_Avg'] =  pd.to_datetime(merge_SB_Avg_Avg_Auto_Auto['maturityDate_x']) # avg_avg
merge_SB_Avg_Avg_Auto_Auto['doy_maturity_Avg_Avg'] = merge_SB_Avg_Avg_Auto_Auto['maturity_obj_Avg_Avg'].dt.dayofyear # avg_avg

merge_SB_Avg_Avg_Auto_Auto['harvest_obj_Avg_Avg'] =  pd.to_datetime(merge_SB_Avg_Avg_Auto_Auto['harvestDate_x']) # avg_avg
merge_SB_Avg_Avg_Auto_Auto['doy_harvest_Avg_Avg'] = merge_SB_Avg_Avg_Auto_Auto['harvest_obj_Avg_Avg'].dt.dayofyear # avg_avg

merge_SB_Avg_Avg_Auto_Auto['planting_obj'] =  pd.to_datetime(merge_SB_Avg_Avg_Auto_Auto['plantingDate']) # avg_avg
merge_SB_Avg_Avg_Auto_Auto['doy_planting'] = merge_SB_Avg_Avg_Auto_Auto['planting_obj'].dt.dayofyear # avg_avg

# %%
#merge_MZ_Avg_Avg_Avg_Auto["maturityDoy_harvestYearDoy"] = merge_MZ_Avg_Avg_Avg_Auto.apply(lambda row: round((row['doy_maturity_Avg_Auto']/row['doy_harvest_Avg_Avg'])*100,1), axis=1)
merge_SB_Avg_Avg_Auto_Auto["harvest_planting_Auto_Auto"] = merge_SB_Avg_Avg_Auto_Auto.apply(lambda row: row['doy_harvest_Auto_Auto'] - row['doy_planting'], axis=1)
merge_SB_Avg_Avg_Auto_Auto["maturity_planting_Auto_Auto"] = merge_SB_Avg_Avg_Auto_Auto.apply(lambda row: row['doy_maturity_Auto_Auto'] - row['doy_planting'], axis=1)

merge_SB_Avg_Avg_Auto_Auto["harvest_planting_Avg_Avg"] = merge_SB_Avg_Avg_Auto_Auto.apply(lambda row: row['doy_harvest_Avg_Avg'] - row['doy_planting'], axis=1)
merge_SB_Avg_Avg_Auto_Auto["maturity_planting_Avg_Avg"] = merge_SB_Avg_Avg_Auto_Auto.apply(lambda row: row['doy_maturity_Avg_Avg'] - row['doy_planting'], axis=1)

# %%
%matplotlib inline 
cp.correlation_scatter_with_stats(merge_SB_Avg_Avg_Auto_Auto["harvest_planting_Avg_Avg"],merge_SB_Avg_Avg_Auto_Auto["harvest_planting_Auto_Auto"],group=pd.Series(),marker_size = 8,xlab='Harvest-PlantingAvgPlantingAvgHarvest',ylab='Harvest-PlantingAutoPlantingAutoHarvest',title='Soybean parcel Harvest-Planting Date Avg_Avg vs Auto_Auto')

# %%
%matplotlib inline
cp.correlation_scatter_with_stats(merge_SB_Avg_Avg_Auto_Auto["maturity_planting_Avg_Avg"],merge_SB_Avg_Avg_Auto_Auto["maturity_planting_Auto_Auto"],group=pd.Series(),marker_size = 8,xlab='Maturity-PlantingAvgPlantingAvgHarvest',ylab='Maturity-PlantingAutoPlantingAutoHarvest',title='Soybean parcel Maturity-Planting Date Avg_Avg vs Auto_Auto')

# %%
