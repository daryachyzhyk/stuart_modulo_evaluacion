
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


df_eval_compra = pd.read_csv(file_eval_compra)

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
# okr 2- envios


df_eval_real = pd.read_csv(file_eval_real)

df_envios = df_eval_real[df_eval_real['info_type']=='envios']

df_envios['q_dif_alg_abs'] = np.abs(df_envios['q_dif_alg'])

df_envios['q_dif_alg_abs_pct'] = df_envios['q_dif_alg_abs'] / df_envios['q_real_rel'] * 100
df_envios['q_dif_alg_pct'] = df_envios['q_dif_alg'] / df_envios['q_real_rel'] * 100


df_envios.loc[(df_envios['q_estimates_alg'] == 0) & (df_envios['q_real_rel'] == 0), 'q_dif_alg_pct'] = 0
df_envios.loc[(df_envios['q_estimates_alg'] == 0) & (df_envios['q_real_rel'] != 0), 'q_dif_alg_pct'] = 100
df_envios.loc[(df_envios['q_estimates_alg'] != 0) & (df_envios['q_real_rel'] == 0), 'q_dif_alg_pct'] = 100

df_alg = df_envios.groupby(['date_week', 'family_desc']).agg({'q_dif_alg_pct': 'sum',
                                                              'q_estimates_alg': 'sum',
                                                              'q_real_rel': 'sum',
                                                              'info_type': 'count'
                                                              }).reset_index()

df_alg = df_alg.rename(columns={'info_type': 'count'})

df_alg.loc[df_alg['family_desc'].isin(['BOLSO', 'BUFANDA', 'FULAR']), 'count'] = 8

df_alg['q_dif_alg_pct_fam'] = df_alg['q_dif_alg_pct'] / df_alg['count']


df_alg_date = df_alg.groupby(['date_week']).agg({'q_dif_alg_pct_fam': 'mean'})



###############################
# OKR - 3 - devos


df_devos = df_eval_real[df_eval_real['info_type']=='devos']


df_devos['q_dif_pct'] = df_devos['q_dif'] / df_devos['q_real'] * 100


df_devos.loc[(df_devos['q_estimates'] == 0) & (df_devos['q_real'] == 0), 'q_dif_pct'] = 0
df_devos.loc[(df_devos['q_estimates'] == 0) & (df_devos['q_real'] != 0), 'q_dif_pct'] = 100
df_devos.loc[(df_devos['q_estimates'] != 0) & (df_devos['q_real'] == 0), 'q_dif_pct'] = 100



df_devos_date_fam = df_devos.groupby(['date_week', 'family_desc']).agg({'q_dif_pct': 'sum',
                                                                         'info_type': 'count'}).reset_index()

df_devos_date_fam = df_devos_date_fam.rename(columns={'info_type': 'count'})

df_devos_date_fam.loc[df_devos_date_fam['family_desc'].isin(['BOLSO', 'BUFANDA', 'FULAR']), 'count'] = 8

df_devos_date_fam['q_dif_pct_fam'] = df_devos_date_fam['q_dif_pct'] / df_devos_date_fam['count']


df_devos_date = df_devos_date_fam.groupby(['date_week']).agg({'q_dif_pct_fam': 'mean'})


#
# test = df_eval_real[(df_eval_real['date_week']=='2020-07-27')
#                     & (df_eval_real['family_desc']=='BOLSO')
#                     & (df_eval_real['info_type']=='envios')]

