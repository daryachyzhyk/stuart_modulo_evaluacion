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
# Path
file_eval_real = ('/var/lib/lookiero/stock/stock_tool/eval_estimates_real.csv.gz')
file_eval = ('/var/lib/lookiero/stock/stock_tool/eval_estimates.csv.gz')
file_eval_compra = ('/var/lib/lookiero/stock/stock_tool/eval_estimates_real_compra.csv.gz')
file_eval_settings = ('/var/lib/lookiero/stock/stock_tool/eval_settings.csv.gz')
result_folder = '/var/lib/lookiero/stock/stock_tool/okr'

#######################################################################################################################
# Select dates
date_select = datetime.datetime(2020, 9, 15)
number_weeks = 4

# TODO: check difference with and without threshold
only_similar_shopping = True

#####################################################
print('Date when the calculation is run ', date_select)
if date_select == 'today':
    day_today = datetime.datetime.now()
    date_start = day_today - datetime.timedelta(days=7 * number_weeks + day_today.weekday())
elif isinstance(date_select, datetime.datetime):
    print('datetime correct')
    date_start = date_select - datetime.timedelta(days=7 * number_weeks + date_select.weekday())
    pass
else:
    print('Error: date_start should be datetime')
# date_start_str = '2020-09-14' # we analyze whole week to which the indicated day belongs


date_end = date_start + datetime.timedelta(days=7 * number_weeks - 1)
# date_week = date_start + datetime.timedelta(days=7 * number_weeks)

date_start_str = datetime.datetime.strftime(date_start, '%Y-%m-%d')

date_end_str = datetime.datetime.strftime(date_end, '%Y-%m-%d')
# date_week_str = datetime.datetime.strftime(date_week, '%Y-%m-%d')

print('OKR of the last ' + str(number_weeks) + ' for the days: ' + date_start_str + ' - ' + date_end_str)
# print('Date week: ')


#######################################################################################################################
# Calculate weeks to be analysed for selected dates

# date_start = datetime.datetime.strptime(date_start_str, '%Y-%m-%d')
# date_monday_start = date_start + datetime.timedelta(days=0 - date_start.weekday())


list_selected_date_week = [date_start + datetime.timedelta(days=7 * (i + 1)) for i in range(number_weeks)]

print('Date week: ', list_selected_date_week[-1])
#
# all_date_week = []
#
# for i in range(number_weeks):
#     i = i
#     all_date_week.append(date_start + datetime.timedelta(days=7 * i))
#
# all_mondays_str = [datetime.datetime.strftime(i, '%Y-%m-%d') for i in all_mondays]
# all_date_week_str = [datetime.datetime.strftime(i + datetime.timedelta(days=7), '%Y-%m-%d') for i in all_mondays]
#
# df_all_mondays = pd.DataFrame({'date_monday': all_mondays,
#                                'date_week': all_date_week_str})

df_settings = pd.read_csv(file_eval_settings, usecols=['id_stuart', 'date_shopping', 'date_order_in_stock',
                                                       'date_next_order_in_stock'])
df_settings = df_settings.drop_duplicates(subset=['date_shopping'], keep='last')

dic_settings = {}

for index, row in df_settings.iterrows():
    dic_settings[row['date_shopping']] = pd.date_range(start=pd.to_datetime(row['date_order_in_stock']),
                                                       end=pd.to_datetime(row['date_next_order_in_stock']) -
                                                           datetime.timedelta(days=1), freq='W-MON')

df_dates_settings = pd.DataFrame.from_dict(dic_settings)

df_all_shopping_week = pd.melt(df_dates_settings, value_vars=df_dates_settings.columns,
                               var_name='date_shopping', value_name='date_monday')

df_all_shopping_week['date_week'] = df_all_shopping_week['date_monday'] + datetime.timedelta(days=7)

df_selected_shopping_week = df_all_shopping_week[df_all_shopping_week['date_week'].isin(list_selected_date_week)]
# df_shopping_date_week = pd.merge(df_all_mondays, df_date_shopping_week, on=['date_monday'], how='left')
#
#
# df_shopping_mondays = pd.merge(df_all_mondays, df_date_shopping_week, on=['date_monday'], how='left')
df_shopping_week_numbers = df_selected_shopping_week['date_shopping'].value_counts().rename_axis(
    'date_shopping').reset_index(name='n_weeks')

list_shopping_date = list(set(df_shopping_week_numbers['date_shopping'].to_list()))

#######################################################################################################################
# parameters

# threshold which we consider important as a difference in % between Compra Real and Stuart recommendation

if only_similar_shopping:
    threshold = 0.2
else:
    threshold = 0.001

#######################################################################################################################
# OKR - 1- Difference Shopping and Stuart

df_eval_compra_all = pd.read_csv(file_eval_compra)

if all(elem in list(set(df_eval_compra_all['date_shopping'].to_list())) for elem in list_shopping_date):
    print('Selected dates are ok')
else:
    shopping_not_in_list = [elem for elem in list_shopping_date if
                            elem not in list(set(df_eval_compra_all['date_shopping'].to_list()))]

    dates_not_covered = []
    for d in shopping_not_in_list:
        dates_not_covered.append(list(np.datetime_as_string(
            df_selected_shopping_week[df_selected_shopping_week['date_shopping'] == d]['date_week'].values, unit='D')))

    print('We do NOT have shopping information ' + str(shopping_not_in_list) + ' about selected dates:'
          + str(dates_not_covered) + '. Please, select another dates.')

df_family_size_dif_binary = pd.DataFrame([])

print('Calculate for which family-size there is significant difference in recommendation and shopping')

list_okr_shopping_pct = []
for date_s in list_shopping_date:
    df_eval_compra = df_eval_compra_all[df_eval_compra_all['date_shopping'] == date_s]

    df_eval_compra['q_dif_abs'] = np.abs(df_eval_compra['q_dif'])
    df_compra_date_fam = df_eval_compra.groupby(['date_shopping', 'family_desc', 'size']).agg({'q_estimate': 'sum',
                                                                                               'q_real': 'sum'}).reset_index()

    df_compra_date_fam['q_real_div_estimate'] = np.abs(
        df_compra_date_fam['q_real'] / df_compra_date_fam['q_estimate'] - 1)
    df_compra_date_fam.loc[
        (df_compra_date_fam['q_estimate'] == 0) & (df_compra_date_fam['q_real'] == 0), 'q_real_div_estimate'] = 0
    df_compra_date_fam.loc[
        (df_compra_date_fam['q_estimate'] == 0) & (df_compra_date_fam['q_real'] != 0), 'q_real_div_estimate'] = 1
    df_compra_date_fam.loc[
        (df_compra_date_fam['q_estimate'] != 0) & (df_compra_date_fam['q_real'] == 0), 'q_real_div_estimate'] = 1

    df_compra_date_fam.loc[df_compra_date_fam['q_real_div_estimate'] > threshold, 'q_dif_thresh'] = 1
    df_compra_date_fam['q_dif_thresh'] = df_compra_date_fam['q_dif_thresh'].fillna(0)

    df_date_number_fam = df_compra_date_fam.groupby(['date_shopping']).agg({'q_dif_thresh': 'sum',
                                                                            'family_desc': 'count'}).reset_index()

    df_date_number_fam['q_dif_family_pct'] = np.round(df_date_number_fam['q_dif_thresh'] /
                                                      df_date_number_fam['family_desc'], 2)

    list_okr_shopping_pct.append(df_date_number_fam['q_dif_family_pct'].values[0])
    df_family_size_dif_binary = df_family_size_dif_binary.append(df_compra_date_fam[['date_shopping', 'family_desc',
                                                                                     'size', 'q_dif_thresh']])

df_family_size_dif_binary = df_family_size_dif_binary.rename(columns={'q_dif_thresh': 'different'})

date_week_last_str = datetime.datetime.strftime(list_selected_date_week[-1], '%Y-%m-%d')
df_okr_shopping = pd.DataFrame({'date_shopping': list_shopping_date,
                                'dif_recommend_shopping_pct': list_okr_shopping_pct})
df_okr_shopping['date_week'] = date_week_last_str
df_okr_shopping['threshold_shopping_difference'] = threshold
# df_okr_shopping['date_monday_start'] = datetime.datetime.strftime(date_monday_start, '%Y-%m-%d')
df_okr_shopping = pd.merge(df_okr_shopping, df_shopping_week_numbers, on=['date_shopping'], how='left')


# df_family_size_dif_binary['date_start'] = date_start_str
# df_family_size_dif_binary['date_monday_start'] = datetime.datetime.strftime(date_monday_start, '%Y-%m-%d')
# df_family_size_dif_binary['n_weeks'] = number_weeks


# Conclusion: 65% de familias estan por encima del umbral 20% de diferencia entre compra y recomendacion.
# Hay 69% de la diferencia de unidades a nivel familia

# 43% de familias tallas estan por encima del umbral 20% de diferencia entre compra y recomendacion.

backup_folder = os.path.join(result_folder, date_week_last_str)

if not os.path.exists(backup_folder):
    os.makedirs(backup_folder)

df_okr_shopping.to_csv(os.path.join(backup_folder, 'okr_shopping.csv'), index=False, header=True)
df_family_size_dif_binary.to_csv(os.path.join(backup_folder,
                                              'recommend_shopping_dif_binary_thr' + str(threshold) + '.csv'),
                                 index=False,
                                 header=True)
print('Saving OKR shopping to: ' + os.path.join(backup_folder, 'okr_shopping.csv'))

#######################################################################################################################
# okr 2- envios  and devos

# for okr_type in ['envios', 'devos']:

df_eval_real_raw = pd.read_csv(file_eval_real)

# df_envios_raw = df_eval_real[df_eval_real['info_type'] == okr_type]
# datetime.datetime.strftime(date_end, '%Y-%m-%d')
list_selected_date_week_str = [datetime.datetime.strftime(i, '%Y-%m-%d') for i in list_selected_date_week]
df_eval_real = df_eval_real_raw[df_eval_real_raw['date_week'].isin(list_selected_date_week_str)]

df_selected_shopping_week['date_week'] = df_selected_shopping_week['date_week'].dt.strftime('%Y-%m-%d')

# add date_shopping
df_eval_real = pd.merge(df_eval_real,
                     df_selected_shopping_week[['date_week', 'date_shopping']],
                     on=['date_week'],
                     how='left')

# add information about difference
df_eval_real = pd.merge(df_eval_real, df_family_size_dif_binary,
                     on=['date_shopping', 'family_desc', 'size'],
                     how='left')

# remove family-size where shopping and recommendation are different
df_eval_real = df_eval_real[df_eval_real['different'] == 0]

# drop families
list_family_drop = ['GORRO', 'SNEAKERS', 'BOTAS', 'BOTINES']
df_eval_real = df_eval_real.drop(df_eval_real[df_eval_real['family_desc'].isin(list_family_drop)].index)

# drop sizes for accessories
list_family_acces = ['BOLSO', 'BUFANDA', 'FULAR']

# test = df_envios[(df_envios['family_desc'].isin(list_family_acces)) & (df_envios['size'] != 'UNQ')]

df_eval_real = df_eval_real.drop(df_eval_real[(df_eval_real['family_desc'].isin(list_family_acces)) &
                                     (df_eval_real['size'] != 'UNQ')].index)

for okr_type in ['envios']: # , 'devos'
    print('Calculating OKR of ', okr_type)
    df_okr = df_eval_real[df_eval_real['info_type'] == okr_type]

    if okr_type == 'envios':

        df_okr = df_okr.groupby(['family_desc', 'size']).agg({'q_estimates_alg': 'sum',
                                                                'q_real_rel': 'sum'}).reset_index()

        # df_envios['q_dif_alg_abs'] = np.abs(df_envios['q_dif_alg'])
        df_okr['q_dif_abs'] = np.abs(df_okr['q_estimates_alg'] - df_okr['q_real_rel'])
        df_okr['okr_value'] = df_okr['q_dif_abs'] * df_okr['q_real_rel'] / df_okr['q_real_rel'].sum()
        print('df_okr[q_real_rel].sum()   ', df_okr['q_real_rel'].sum())

    elif okr_type == 'devos':

        df_okr = df_okr.groupby(['family_desc', 'size']).agg({'q_estimates': 'sum',
                                                              'q_real': 'sum',
                                                              'q_real_rel': 'sum'}).reset_index()
        df_okr['q_dif_abs'] = np.abs(df_okr['q_estimates'] - df_okr['q_real'])
        df_okr['okr_value'] = (df_okr['q_dif_abs'] / df_okr['q_real'])

        df_okr.loc[(df_okr['q_estimates'] == 0) & (df_okr['q_real'] == 0), 'okr_value'] = 0
        df_okr.loc[(df_okr['q_estimates'] == 0) & (df_okr['q_real'] != 0), 'okr_value'] = 1
        df_okr.loc[(df_okr['q_estimates'] != 0) & (df_okr['q_real'] == 0), 'okr_value'] = 1

        df_okr['okr_value'] = df_okr['okr_value'] * df_okr['q_real_rel']
    df_okr['okr_type'] = okr_type
    df_okr['date_week'] = date_week_last_str
    df_okr['n_week'] = number_weeks

    df_okr_mean = df_okr.groupby(['okr_type']).agg({'date_week': 'first',
                                                    'okr_value': 'mean',
                                                    'n_week': 'first'
                                                    }).reset_index()
    file_name_okr_family_size = os.path.join(backup_folder, 'okr_' + okr_type + '_family_size.csv')
    file_name_okr_mean = os.path.join(backup_folder, 'okr_' + okr_type + '.csv')
    df_okr.to_csv(file_name_okr_family_size, index=False, header=True)

    df_okr_mean.to_csv(file_name_okr_mean, index=False, header=True)

    print('Saving OKR ' + okr_type + ' detailed to: ' + file_name_okr_family_size)
    print('Saving OKR ' + okr_type + ' mean to: ' + file_name_okr_mean)




# df_envios['q_dif_alg_pct'] = df_envios['q_dif_alg'] / df_envios['q_real_rel'] * 100
# df_envios.loc[(df_envios['q_estimates_alg'] == 0) & (df_envios['q_real_rel'] == 0), 'q_dif_alg_abs'] = 0
# df_envios.loc[(df_envios['q_estimates_alg'] == 0) & (df_envios['q_real_rel'] != 0), 'q_dif_alg_abs'] = 1
# df_envios.loc[(df_envios['q_estimates_alg'] != 0) & (df_envios['q_real_rel'] == 0), 'q_dif_alg_abs'] = 1

# df_alg_fam_size = df_envios.groupby(['date_week', 'family_desc', 'size']).agg({'q_dif_alg_abs_pct': 'sum'}).reset_index()

# df_alg_fam_size = df_envios.groupby(['family_desc', 'size']).agg({'q_dif_alg_abs_pct': 'mean'}).reset_index()

#
# df_alg_fam_size = df_envios.rename(columns={'q_dif_alg_abs_pct': 'okr_value'})
# df_alg_fam_size['date_monday_start'] = datetime.datetime.strftime(date_monday_start, '%Y-%m-%d')
# df_alg_fam_size['n_week'] = number_weeks
# df_alg_fam_size['date_week'] = df_shopping_mondays.loc[
# df_shopping_mondays['date_monday'] == datetime.datetime.strftime(date_monday_start,
#                                                                      '%Y-%m-%d'), 'date_week'].values[0]
# df_alg_fam_size['okr_type'] = 'envios'
#
# df_alg = df_alg_fam_size.groupby(['okr_type']).agg({'okr_value': 'mean',
#                                                     'date_monday_start': 'first',
#                                                     'n_week': 'first',
#                                                     'date_week': 'first'}).reset_index()
#
# df_alg_fam_size.to_csv(os.path.join(backup_folder, 'okr_envios_family_size.csv'), index=False, header=True)
#
# df_alg.to_csv(os.path.join(backup_folder, 'okr_envios.csv'), index=False, header=True)
#
# print('Saving OKR envios to: ' + os.path.join(backup_folder, 'okr_envios.csv'))
#
# #######################################################################################################################
# # OKR - 3 - devos
#
#
# df_devos_raw = df_eval_real[df_eval_real['info_type'] == 'devos']
#
# df_devos = df_devos_raw[df_devos_raw['date_week'].isin(df_shopping_mondays['date_week'].to_list())]
#
# df_devos = df_devos_raw.groupby(['date_week', 'family_desc', 'size']).agg({'q_estimates': 'sum',
#                                                                            'q_real': 'sum',
#                                                                            'q_dif': 'sum'}).reset_index()
#
# # df_devos['q_dif1'] = np.abs(df_devos['q_estimates'] - df_devos['q_real'])
# # add date_shopping
# df_devos = pd.merge(df_devos, df_shopping_mondays[['date_week', 'date_shopping']],
#                     on=['date_week'],
#                     how='left')
#
# # add information about difference
# df_devos = pd.merge(df_devos, df_family_size_dif_binary,
#                     on=['date_shopping', 'family_desc', 'size'],
#                     how='left')
#
# # remove family-size where shopping and recommendation are different
# df_devos = df_devos[df_devos['different'] == 0]
#
# # drop families
# list_family_drop = ['GORRO', 'SNEAKERS', 'BOTAS', 'BOTINES']
# df_devos = df_devos.drop(df_devos[df_devos['family_desc'].isin(list_family_drop)].index)
#
# # drop sizes for accessories
# list_family_acces = ['BOLSO', 'BUFANDA', 'FULAR']
#
# df_devos = df_devos.drop(df_devos[(df_devos['family_desc'].isin(list_family_acces)) &
#                                   (df_devos['size'] != 'UNQ')].index)
#
# df_devos['q_dif_pct'] = df_devos['q_dif'] / df_devos['q_real']
#
# df_devos.loc[(df_devos['q_estimates'] == 0) & (df_devos['q_real'] == 0), 'q_dif_pct'] = 0
# df_devos.loc[(df_devos['q_estimates'] == 0) & (df_devos['q_real'] != 0), 'q_dif_pct'] = 1
# df_devos.loc[(df_devos['q_estimates'] != 0) & (df_devos['q_real'] == 0), 'q_dif_pct'] = 1
#
# df_alg_fam_size = df_devos.groupby(['family_desc', 'size']).agg({'q_dif_pct': 'mean'}).reset_index()
# df_alg_fam_size = df_alg_fam_size.rename(columns={'q_dif_pct': 'okr_value'})
# df_alg_fam_size['date_monday_start'] = datetime.datetime.strftime(date_monday_start, '%Y-%m-%d')
# df_alg_fam_size['n_week'] = number_weeks
# df_alg_fam_size['date_week'] = df_shopping_mondays.loc[
#     df_shopping_mondays['date_monday'] == datetime.datetime.strftime(date_monday_start,
#                                                                      '%Y-%m-%d'), 'date_week'].values[0]
# df_alg_fam_size['okr_type'] = 'devos'
#
# df_alg = df_alg_fam_size.groupby(['date_week']).agg({'okr_value': 'mean',
#                                                      'date_monday_start': 'first',
#                                                      'n_week': 'first',
#                                                      'okr_type': 'first'}).reset_index()
#
# df_alg_fam_size.to_csv(os.path.join(backup_folder, 'okr_devos_family_size.csv'), index=False, header=True)
# df_alg.to_csv(os.path.join(backup_folder, 'okr_devos.csv'), index=False, header=True)
#
# print('Saving OKR envios to: ' + os.path.join(backup_folder, 'okr_devos.csv'))
#
#
#














#
#
#
# df_devos = df_eval_real[df_eval_real['info_type']=='devos']
#
#
# df_devos['q_dif_pct'] = df_devos['q_dif'] / df_devos['q_real'] * 100
#
#
# df_devos.loc[(df_devos['q_estimates'] == 0) & (df_devos['q_real'] == 0), 'q_dif_pct'] = 0
# df_devos.loc[(df_devos['q_estimates'] == 0) & (df_devos['q_real'] != 0), 'q_dif_pct'] = 100
# df_devos.loc[(df_devos['q_estimates'] != 0) & (df_devos['q_real'] == 0), 'q_dif_pct'] = 100
#
#
#
# df_devos_date_fam = df_devos.groupby(['date_week', 'family_desc']).agg({'q_dif_pct': 'sum',
#                                                                          'info_type': 'count'}).reset_index()
#
# df_devos_date_fam = df_devos_date_fam.rename(columns={'info_type': 'count'})
#
# df_devos_date_fam.loc[df_devos_date_fam['family_desc'].isin(['BOLSO', 'BUFANDA', 'FULAR']), 'count'] = 8
#
# df_devos_date_fam['q_dif_pct_fam'] = df_devos_date_fam['q_dif_pct'] / df_devos_date_fam['count']
#
#
# df_devos_date = df_devos_date_fam.groupby(['date_week']).agg({'q_dif_pct_fam': 'mean'})
#

#
# test = df_eval_real[(df_eval_real['date_week']=='2020-07-27')
#                     & (df_eval_real['family_desc']=='BOLSO')
#                     & (df_eval_real['info_type']=='envios')]

#######################################################################################################################
# gather all the data
