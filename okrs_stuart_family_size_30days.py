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

file_eval_settings = ('/var/lib/lookiero/stock/stock_tool/eval_settings.csv.gz')

result_folder = '/var/lib/lookiero/stock/stock_tool/okr'


date_start_str = '2020-08-23' # we analyze whole week to which the indicated day belongs
number_weeks = 4

date_start = datetime.datetime.strptime(date_start_str, '%Y-%m-%d')
date_monday_start = date_start + datetime.timedelta(days=0 - date_start.weekday())

# date_monday_end = date_monday_start + datetime.timedelta(days=7 * number_weeks)

all_mondays = []

for i in range(number_weeks):
    i = i
    print(i)
    all_mondays.append(date_monday_start + datetime.timedelta(days=7 * i))


all_mondays_str = [datetime.datetime.strftime(i, '%Y-%m-%d') for i in all_mondays]
all_date_week_str = [datetime.datetime.strftime(i + datetime.timedelta(days=7), '%Y-%m-%d') for i in all_mondays]

df_all_mondays = pd.DataFrame({'date_monday': all_mondays,
                               'date_week': all_date_week_str})


df_settings = pd.read_csv(file_eval_settings, usecols=['id_stuart', 'date_shopping', 'date_order_in_stock',
                                                       'date_next_order_in_stock'])

df_settings = df_settings.drop_duplicates(subset=['date_shopping'], keep='last')

# df_settings = pd.to_datetime(df_settings, format='%Y-%m-%d')
# next mondays
dic_settings = {}

for index, row in df_settings.iterrows():

    dic_settings[row['date_shopping']] = pd.date_range(start=pd.to_datetime(row['date_order_in_stock']) ,
                                                       end=pd.to_datetime(row['date_next_order_in_stock']) -
                                                           datetime.timedelta(days=1), freq='W-MON')

    # pd.date_range(start='2020-08-24', end='2020-09-14', freq='W-MON')

# df_dates_settings = pd.DataFrame.from_dict(dic_settings, orient='index')
df_dates_settings = pd.DataFrame.from_dict(dic_settings)

df_date_shopping_week = pd.melt(df_dates_settings, value_vars=df_dates_settings.columns,
        var_name='date_shopping', value_name='date_monday')


df_shopping_mondays = pd.merge(df_all_mondays, df_date_shopping_week, on=['date_monday'], how='left')

df_shopping_week_numbers = df_shopping_mondays['date_shopping'].value_counts().rename_axis('date_shopping').reset_index(name='n_weeks')


list_shopping_date = list(set(df_shopping_mondays['date_shopping'].to_list()))





# day_today = datetime.datetime.now()
# date_monday = day_today - datetime.timedelta(days=7 + day_today.weekday())
# date_str = datetime.datetime.strftime(date_monday, '%Y-%m-%d')





#######################################################################################################################
# parameters

# threshold which we consider important as a difference in % between Compra Real and Stuart recommendation
threshold = 0.2

#######################################################################################################################
# OKR - 1- Difference Shopping and Stuart


df_eval_compra_all = pd.read_csv(file_eval_compra)


if all(elem in list(set(df_eval_compra_all['date_shopping'].to_list())) for elem in list_shopping_date):
    print('Selected dates are ok')
else:
    shopping_not_in_list = [elem for elem in list_shopping_date if elem not in list(set(df_eval_compra_all['date_shopping'].to_list()))]

    dates_not_covered = []
    for d in shopping_not_in_list:

        dates_not_covered.append(list(np.datetime_as_string(df_shopping_mondays[df_shopping_mondays['date_shopping'] == d]['date_week'].values, unit='D')))

    print('We do NOT have shopping information ' + str(shopping_not_in_list) + ' about selected dates:'
          + str(dates_not_covered) + '. Please, select another dates.')

# df_okr_compra = pd.DataFrame([], columns=['date_shopping', 'dif_fam_size_pct'])
# dic_shop_fam_size = {}
df_family_size_dif_binary = pd.DataFrame([])
list_okr_shopping_pct = []
for date_s in list_shopping_date:
    # list_okr_shopping.append(date_s)
    df_eval_compra = df_eval_compra_all[df_eval_compra_all['date_shopping'] == date_s]

    df_eval_compra['q_dif_abs'] = np.abs(df_eval_compra['q_dif'])
    df_compra_date_fam = df_eval_compra.groupby(['date_shopping', 'family_desc', 'size']).agg({'q_dif_abs': 'sum',
                                                                                               'q_estimate': 'sum',
                                                                                               'q_real': 'sum'}).reset_index()


    # df_compra_date_fam['q_dif_abs_pct'] = df_compra_date_fam['q_dif_abs'] / df_compra_date_fam['q_estimate']


    df_compra_date_fam['q_dif_div'] = np.abs(df_compra_date_fam['q_real'] / df_compra_date_fam['q_estimate'] - 1)


    df_compra_date_fam.loc[(df_compra_date_fam['q_estimate'] == 0) & (df_compra_date_fam['q_real'] == 0), 'q_dif_div'] = 0

    df_compra_date_fam.loc[(df_compra_date_fam['q_estimate'] == 0) & (df_compra_date_fam['q_real'] != 0), 'q_dif_div'] = 1

    df_compra_date_fam.loc[(df_compra_date_fam['q_estimate'] != 0) & (df_compra_date_fam['q_real'] == 0), 'q_dif_div'] = 1


    df_compra_date_fam.loc[df_compra_date_fam['q_dif_div'] > threshold, 'q_dif_thresh'] = 1

    df_compra_date_fam['q_dif_thresh'] = df_compra_date_fam['q_dif_thresh'].fillna(0)


    df_date_number_fam = df_compra_date_fam.groupby(['date_shopping']).agg({'q_dif_thresh': 'sum',
                                                                            'family_desc': 'count'}).reset_index()

    df_date_number_fam['q_dif_family_pct'] = np.round(df_date_number_fam['q_dif_thresh'] / df_date_number_fam['family_desc'], 2)

    list_okr_shopping_pct.append(df_date_number_fam['q_dif_family_pct'].values[0])
    # df_similar.append(df_compra_date_fam.loc[df_compra_date_fam['q_dif_thresh']==0][['date_shopping', 'family_desc', 'size', 'q_dif_thresh']])

    df_family_size_dif_binary = df_family_size_dif_binary.append(df_compra_date_fam[['date_shopping', 'family_desc', 'size', 'q_dif_thresh']])

    # df_compra_date_fam.plot.bar(x='family_desc', y='q_dif_div', rot=90)


    # df_compra_date = df_compra_date_fam.groupby(['date_shopping']).agg({'q_dif_div': 'mean'})

df_okr_shopping = pd.DataFrame({#'date_start': date_start_str,
                                # 'n_weeks': number_weeks,
                                'date_shopping': list_shopping_date,
                                'dif_recommend_shopping_pct': list_okr_shopping_pct})
df_okr_shopping['date_start'] = date_start_str
df_okr_shopping['date_monday_start'] = datetime.datetime.strftime(date_monday_start, '%Y-%m-%d')
# df_okr_shopping['n_weeks'] = number_weeks

df_okr_shopping = pd.merge(df_okr_shopping, df_shopping_week_numbers, on=['date_shopping'], how='left')


df_family_size_dif_binary = df_family_size_dif_binary.rename(columns={'q_dif_thresh': 'different'})
# df_family_size_dif_binary['date_start'] = date_start_str
# df_family_size_dif_binary['date_monday_start'] = datetime.datetime.strftime(date_monday_start, '%Y-%m-%d')
# df_family_size_dif_binary['n_weeks'] = number_weeks


# Conclusion: 65% de familias estan por encima del umbral 20% de diferencia entre compra y recomendacion.
# Hay 69% de la diferencia de unidades a nivel familia




backup_folder = os.path.join(result_folder, all_mondays_str[0])

if not os.path.exists(backup_folder):
    os.makedirs(backup_folder)


df_okr_shopping.to_csv(os.path.join(backup_folder, 'okr_shopping.csv'), index=False, header=True)

df_family_size_dif_binary.to_csv(os.path.join(backup_folder, 'recommend_shopping_dif_binary.csv'), index=False, header=True)



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

df_envios_raw = df_eval_real[df_eval_real['info_type'] == 'envios']

df_envios = df_envios_raw[df_envios_raw['date_week'].isin(df_shopping_mondays['date_week'].to_list())]

# add date_shopping
df_envios = pd.merge(df_envios, df_shopping_mondays[['date_week', 'date_shopping']],
                     on=['date_week'],
                     how='left')


df_envios['q_dif_alg_abs'] = np.abs(df_envios['q_dif_alg'])

# df_envios['q_dif_alg_abs_pct'] = df_envios['q_dif_alg_abs'] / df_envios['q_real_rel'] * 100
# df_envios['q_dif_alg_pct'] = df_envios['q_dif_alg'] / df_envios['q_real_rel'] * 100

    #

df_envios.loc[(df_envios['q_estimates_alg'] == 0) & (df_envios['q_real_rel'] == 0), 'q_dif_alg_abs'] = 0
df_envios.loc[(df_envios['q_estimates_alg'] == 0) & (df_envios['q_real_rel'] != 0), 'q_dif_alg_abs'] = 1
df_envios.loc[(df_envios['q_estimates_alg'] != 0) & (df_envios['q_real_rel'] == 0), 'q_dif_alg_abs'] = 1

df_alg = df_envios.groupby(['date_week', 'family_desc', 'size']).agg({'q_dif_alg_abs': 'sum',
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