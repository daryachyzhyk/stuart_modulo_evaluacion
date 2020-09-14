'''OKR of the Stuart:
- Difference Shopping and Stuart
- Envios
- Devos
- Roturas

'''


import os
import pandas as pd
import numpy as np
import datetime


#######################################################################################################################
# path
file_eval_real = ('/var/lib/lookiero/stock/stock_tool/eval_estimates_real.csv.gz')

file_eval = ('/var/lib/lookiero/stock/stock_tool/eval_estimates.csv.gz')

file_eval_compra = ('/var/lib/lookiero/stock/stock_tool/eval_estimates_real_compra.csv.gz')

day_today = datetime.datetime.now()


date_monday = day_today - datetime.timedelta(days=7 + day_today.weekday())

date_str = datetime.datetime.strftime(date_monday, '%Y-%m-%d')

result_folder = '/var/lib/lookiero/stock/stock_tool/okr'

backup_folder = os.path.join(result_folder, date_str)

if not os.path.exists(backup_folder):
    os.makedirs(backup_folder)


#######################################################################################################################
# parameters

# threshold which we consider important as a difference in % between Compra Real and Stuart recommendation
threshold = 20

#######################################################################################################################
# OKR - 1- Difference Shopping and Stuart


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

df_compra_date = df_compra_date_fam.groupby(['date_shopping']).agg({'q_dif_div': 'mean'})

# Conclusion: 65% de familias estan por encima del umbral 20% de diferencia entre compra y recomendacion.
# Hay 69% de la diferencia de unidades a nivel familia

# TODO: save
# file_save_shopping = os.path.join(result_folder, 'okr-compra')
# df_compra_date .to_csv(file_save_shopping, mode='a', index=False, header=True)
#
# if not os.path.isfile(file_save):
#     print('Creating a new file ' + file_save)
#     df.to_csv(file_save, mode='a', index=False, header=True)
# else:
#     print('Appending to existing file ' + file_save)
#     df.to_csv(file_save, mode='a', index=False, header=False)


#######################################################################################################################
# okr 2- envios


df_eval_real = pd.read_csv(file_eval_real)

df_envios = df_eval_real[df_eval_real['info_type']=='envios']

df_envios['q_dif_alg_abs'] = np.abs(df_envios['q_dif_alg'])

# df_envios['q_dif_alg_abs_pct'] = df_envios['q_dif_alg_abs'] / df_envios['q_real_rel'] * 100
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

df_alg_date_fam_size = df_envios.groupby(['date_week', 'family_desc', 'size_desc']).agg({'q_dif_alg_pct': 'sum',
                                                              'q_estimates_alg': 'sum',
                                                              'q_real_rel': 'sum',
                                                              'info_type': 'count'
                                                              }).reset_index()




test = df_envios[(df_envios['date_week']=='2020-07-27')
                    & (df_envios['family_desc']=='BLUSA')]


# conclusion en df_alg_date

# date_week              q_dif_alg_pct_fam
# 2020-07-27          47.567220
# 2020-08-03          66.684366
# 2020-08-10          53.054112
# 2020-08-17          67.792720
# 2020-08-24          68.177977
# 2020-08-31         168.985304

#######################################################################################################################
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

#######################################################################################################################
# gather all the data