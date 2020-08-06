
import os
import glob
import pandas as pd
import numpy as np
import datetime
from google_drive_downloader import GoogleDriveDownloader as gdd


file_estimates = ('/var/lib/lookiero/stock/stock_tool/eval_estimates.csv.gz')

file_real = ('/var/lib/lookiero/stock/stock_tool/eval_real_data.csv.gz')

file_settings = ('/var/lib/lookiero/stock/stock_tool/eval_settings.csv.gz')

df_estimates_raw = pd.read_csv(file_estimates)

df_estimates_date = df_estimates_raw[df_estimates_raw['date_week'] == date_start_str]

df_estimates_date = df_estimates_date[df_estimates_date['id_stuart'] == df_estimates_date['id_stuart'].max()]

df_estimates_date = df_estimates_date[df_estimates_date['caracteristica'] == 'size']


df_estimates_date = df_estimates_date.rename(columns={'clase': 'size',
                                            'q': 'q_estimates'})

df_estimates_date = df_estimates_date[df_estimates_date['clima'] != 'sin_clase']

df_real = pd.read_csv(file_real)

df_real = df_real.rename(columns={'q': 'q_real'})

dic_clima = {'0.0': '0',
             '1.0': '1',

             '2.0': '2',
             '3.0':  '3'}

df_real['clima'] = df_real['clima'].replace(dic_clima)

df = pd.merge(df_estimates_date, df_real,
              on=['date_week', 'family_desc', 'clima', 'size', 'info_type'],
              how='outer')

df['q_real'] = df['q_real'].fillna(0)





df_estimates = df_estimates.rename(columns={'clase': 'size',
                                            'q': 'q_estimates'})
df_real = pd.read_csv(file_real)

df_real = df_real.rename(columns={'q': 'q_real'})






# plot
import seaborn as sns

df = pd.read_csv('/var/lib/lookiero/stock/stock_tool/eval_estimates_real.csv.gz')

df_stock_est = df[df['info_type'] == 'stock'][['family_desc', 'size', 'q_estimates']]
# flights = flights.pivot("month", "year", "passengers")
# df_plot = df_stock_est.pivot('family_desc', 'size', 'q_estimates')

df_plot = df_stock_est.groupby(['family_desc', 'size'])['q_estimates'].sum().unstack(-1)

# df_plot['q_estimates'] = df_plot['q_estimates'].astype(int)

df_plot = df_plot.fillna(0)


ax = sns.heatmap(df_plot, annot=True)


flights = sns.load_dataset("flights")
flights_piv = flights.pivot("month", "year", "passengers")
ax = sns.heatmap(flights)



