
import os
import glob
import pandas as pd
import numpy as np
import datetime


########################
# path
file_eval_real = ('/var/lib/lookiero/stock/stock_tool/eval_estimates_real.csv.gz')

file_eval = ('/var/lib/lookiero/stock/stock_tool/eval_estimates.csv.gz')

file_eval_compra = ('/var/lib/lookiero/stock/stock_tool/eval_estimates_real_compra.csv.gz')


####################
# parameters
threshold = 20

############################3
#OKR - 1- Difference Shopping and Stuart




df_eval_compra['q_dif_abs'] = np.abs(df_eval_compra['q_dif'])
df_compra_date_fam = df_eval_compra.groupby(['date_shopping', 'family_desc']).agg({'q_dif_abs': 'sum',
                                                                                   'q_estimate': 'sum',
                                                                                   'q_real': 'sum'}).reset_index()



df_compra_date_fam['q_dif_abs_pct'] = df_compra_date_fam['q_dif_abs'] / df_compra_date_fam['q_estimate']


df_compra_date_fam['q_dif_div'] = np.abs(df_compra_date_fam['q_real'] / df_compra_date_fam['q_estimate'] - 1) * 100


df_compra_date_fam.loc[(df_compra_date_fam['q_estimate'] == 0) & (df_compra_date_fam['q_real'] == 0), 'q_dif_div'] = 0

df_compra_date_fam.loc[(df_compra_date_fam['q_estimate'] == 0) & (df_compra_date_fam['q_real'] != 0), 'q_dif_div'] = 100

df_compra_date_fam.loc[(df_compra_date_fam['q_estimate'] != 0) & (df_compra_date_fam['q_real'] == 0), 'q_dif_div'] = 100


df_compra_date_fam.loc[df_compra_date_fam['q_dif_div'] > threshold, 'q_dif_thresh'] = 1

df_compra_date_fam['q_dif_thresh'] = df_compra_date_fam['q_dif_thresh'].fillna(0)


df_date_number_fam = df_compra_date_fam.groupby(['date_shopping']).agg({'q_dif_thresh': 'sum',
                                                                        'family_desc': 'count'}).reset_index()

df_date_number_fam['q_dif_family_pct'] = np.round(df_date_number_fam['q_dif_thresh'] / df_date_number_fam['family_desc'] * 100, 0)


df_compra_date_fam.plot.bar(x='family_desc', y='q_dif_div', rot=90)


#####
# envios


df_eval = pd.read_csv(file_eval)

df_eval_compra = pd.read_csv(file_eval_compra)