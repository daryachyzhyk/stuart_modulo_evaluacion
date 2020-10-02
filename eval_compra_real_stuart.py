"""
Script to create comparable extraction of shopping from Stuart projection and real data

@author: dchyzhyk
"""

import os
import pandas as pd
import numpy as np
import datetime


def get_fam_size_clima(references, file=None, drop_duplicates=True, family=True, size=True, clima=True):
    """

    Get 'family_desc', 'size', 'clima' for given list of references

    :param references: list
        list of references
    :param file: str
        path to csv file, '/var/lib/lookiero/stock/stock_tool/productos_preprocessed.csv.gz'
    :param drop_duplicates: binary
        drop duplicates in product file, if False it could increase the number of items in the output
    :param family: binary
        include family description
    :param size: binary
        include size description
    :param clima: binary
        include clima description
    :return: pandas.DataFrame()

    """

    if file is None:
        file = '/var/lib/lookiero/stock/stock_tool/productos_preprocessed.csv.gz'
    query_product_text = 'reference in @references'

    usecols = ['reference']
    if family is True:
        usecols.append('family_desc')
    if size is True:
        usecols.append('size')
    if clima is True:
        usecols.append('clima')

    df = pd.read_csv(file, usecols=usecols).query(query_product_text)
    if drop_duplicates:
        df = df.drop_duplicates('reference', keep='last')
    return df


def get_compra_real(compra_file, productos_file):
    """
    :param compra_file: str
        path to csv file, '/var/lib/lookiero/stock/stock_tool/kpi/compra/compra_reference_quantity - Sheet1.csv',
        that should be downloaded from the GoogleDrive
    :param productos_file: str
        path to the product file
    :return: pandas.Dataframe
        Columns name: ['date_shopping', 'family_desc', 'clima', 'size', 'cantidad_pedida']
    """

    df_compra_raw = pd.read_csv(compra_file, usecols=['date_shopping', 'reference', 'cantidad_pedida'])

    list_references_compra = df_compra_raw["reference"].to_list()
    df_compra_products = get_fam_size_clima(list_references_compra, productos_file, drop_duplicates=True)

    df_compra_reference = pd.merge(df_compra_raw, df_compra_products, on='reference', how='left')
    df_compra = df_compra_reference.groupby(['date_shopping', 'family_desc', 'clima', 'size']).agg({'cantidad_pedida': 'sum'}).reset_index()

    df_compra = df_compra.rename(columns={'cantidad_pedida': 'q_real'})

    return df_compra


def get_stuart_recommendation(eval_settings, eval_estimates):
    """
    Get data from eval_estimates about Stuart projections
    :param eval_settings:
    :param eval_estimates:
    :return:
    """

    df_settings_raw = pd.read_csv(eval_settings)

    df_settings = df_settings_raw.groupby(['date_shopping']).agg({'id_stuart': 'max',
                                                                  'date_order_in_stock': 'last',
                                                                  'n_weeks_recommendation': 'last'}).reset_index()
    # extract weeks that covered by specific shopping
    df_id_dates = pd.DataFrame([])
    for index, row in df_settings.iterrows():
        df_id = pd.DataFrame([])

        df_id['date_week'] = pd.date_range(start=pd.to_datetime(row['date_order_in_stock']),
                                           end=pd.to_datetime(row['date_order_in_stock']) +
                                               datetime.timedelta(days=6) * row['n_weeks_recommendation'],
                                           freq='W-MON')

        df_id['id_stuart'] = row['id_stuart']
        df_id['date_shopping'] = row['date_shopping']
        df_id_dates = df_id_dates.append(df_id)

    df_raw = pd.read_csv(eval_estimates)
    df_id_dates['date_week'] = df_id_dates['date_week'].dt.strftime('%Y-%m-%d')

    df_stuart = df_raw.merge(df_id_dates, on=['date_week', 'id_stuart'], how='inner')
    df_stuart = df_stuart[df_stuart['info_type'] == 'pedido']

    df_stuart = df_stuart.rename(columns={'clase': 'size', 'q': 'q_estimate'})
    df_stuart = df_stuart[['date_shopping', 'family_desc', 'clima', 'size', 'info_type', 'q_estimate']]

    df_stuart_merged = df_stuart.groupby(['date_shopping', 'family_desc', 'clima',
                                          'size', 'info_type']).agg({'q_estimate': 'sum'}).reset_index()

    return df_stuart_merged


def merge_compra_real_stuart(df_compra_real, df_compra_stuart, file_save=None):
    """
    Merge real data and Stuar

    :param df_compra_real: pandas.DataFrame
    :param df_compra_stuart: pandas.DataFrame
    :param file_save: str
        path to csv file
    :return: pandas.DataFrame
    """

    # drop 'sin_clase'
    df_compra_stuart = df_compra_stuart[df_compra_stuart['clima'] != 'sin_clase']
    df_compra_stuart = df_compra_stuart[df_compra_stuart['size'] != 'sin_clase']

    # remove clima not in the list such as 1.66
    list_clima = [0., 0.5, 1., 1.5, 2., 2.5, 3.]
    df_compra_stuart = df_compra_stuart[~df_compra_stuart['clima'].isin(list_clima)]

    dic_clima = {'0.0': '0',
                 '1.0': '1',
                 '2.0': '2',
                 '3.0': '3'}

    df_compra_real['clima'] = df_compra_real['clima'].astype('str').replace(dic_clima)

    # keep just shopping date that apperar in both
    intersection_date_shopping = list(set(df_compra_stuart['date_shopping'].to_list())
                                      & set(df_compra_real['date_shopping'].to_list()))
    df_compra_stuart = df_compra_stuart[df_compra_stuart['date_shopping'].isin(intersection_date_shopping)]
    df_compra_real = df_compra_real[df_compra_real['date_shopping'].isin(intersection_date_shopping)]

    df = pd.merge(df_compra_stuart,
                  df_compra_real,
                  on=['date_shopping', 'family_desc', 'clima', 'size'],
                  how='outer')

    df['q_estimate'] = df['q_estimate'].fillna(0)
    df['q_real'] = df['q_real'].fillna(0)
    df['q_dif'] = np.round(df['q_estimate'] - df['q_real'], 0)

    size_dic = {'XXS': '0-XXS',
                'XS': '1-XS',
                'S': '2-S',
                'M': '3-M',
                'L': '4-L',
                'XL': '5-XL',
                'XXL': '6-XXL',
                'XXXL': '7-XXXL',
                'X4XL': '8-X4XL'}

    df['size_desc'] = df['size'].replace(size_dic)

    if file_save is not None:
        path_save = ('/var/lib/lookiero/stock/stock_tool')
        file_save = os.path.join(path_save, 'eval_estimates_real_compra.csv.gz')

    print('Creating a new file ' + file_save)
    df.to_csv(file_save, index=False, header=True)

    return df

##################################################################################################
# psth
eval_settings = '/var/lib/lookiero/stock/stock_tool/eval_settings.csv.gz'
eval_estimates = '/var/lib/lookiero/stock/stock_tool/eval_estimates.csv.gz'
compra_file = ('/var/lib/lookiero/stock/stock_tool/kpi/compra/compra_reference_quantity - Sheet1.csv')
productos_file = ('/var/lib/lookiero/stock/stock_tool/productos_preprocessed.csv.gz')

path_save = ('/var/lib/lookiero/stock/stock_tool')
path_save_date = ('/var/lib/lookiero/stock/stock_tool/kpi/eval_real_history')

file_save = os.path.join(path_save, 'eval_estimates_real_compra.csv.gz')

##################################################################################################
# run

df_compra_real = get_compra_real(compra_file, productos_file)

df_compra_stuart = get_stuart_recommendation(eval_settings, eval_estimates)

df = merge_compra_real_stuart(df_compra_real, df_compra_stuart, file_save)

print('Total difference: ',  df['q_dif'].abs().sum())



df[df['date_shopping'] == '2020-08-03']['q_dif'].sum()
df[df['date_shopping'] == '2020-08-03']['q_dif'].abs().sum()

df[df['date_shopping'] == '2020-08-31']['q_dif'].sum()
df[df['date_shopping'] == '2020-08-31']['q_dif'].abs().sum()

