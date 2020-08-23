import os
import glob
import pandas as pd
import numpy as np
import datetime
from google_drive_downloader import GoogleDriveDownloader as gdd

# TODO: make a google file download

#
# file_id = "1cFS436nfXP1XV5aBeauPbTe6WZF_jFZ3fbyi5H3Qd7M"
# gdd.download_file_from_google_drive(file_id=file_id,
#                                     dest_path='/var/lib/lookiero/stock/stock_tool/kpi',
#                                     unzip=True)




def get_fam_size_clima(references, file=None, drop_duplicates=True, family=True, size=True, clima=True):
    '''

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

    '''

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


def get_compra_real(date_compra_str, compra_file, productos_file):
    # TODO: download compra file from Google Drive

    # compra realizada
    # compra_file = ('/var/lib/lookiero/stock/stock_tool/kpi/compra/compra_referemce_quantity - Sheet1.csv')

    # link between week and compra date
    # compra_dates_file = ('/var/lib/lookiero/stock/stock_tool/kpi/compra/compra_fechas - Sheet1.csv')

    # query_compra_date_text = 'week == @date_compra_str'
    # df_date_compra = pd.read_csv(compra_dates_file).query(query_compra_date_text)
    # if df_date_compra.empty == False:
    #     date_compra_str = df_date_compra['date_compra'].values[0]

    query_compra_reference_text = 'date_compra == @date_compra_str'
    df_compra_raw = pd.read_csv(compra_file,
                                usecols=['date_compra', 'reference', 'cantidad_pedida']
                                ).query(query_compra_reference_text)

    list_references_compra = df_compra_raw["reference"].to_list()
    df_compra_products = get_fam_size_clima(list_references_compra, productos_file, drop_duplicates=True)

    df_compra_reference = pd.merge(df_compra_raw, df_compra_products, on='reference', how='left')
    df_compra = df_compra_reference.groupby(['family_desc', 'clima', 'size']).agg({'cantidad_pedida': 'sum'}).reset_index()

    # df_compra['info_type'] = 'pedido'
    df_compra = df_compra.rename(columns={'cantidad_pedida': 'compra_real'})

    # else:
    #     print('There is no date ' + date_compra_str + '. Please update the data here: ' + compra_dates_file)

    return df_compra


def get_stuart_recommendation(date_compra_str, compra_date_stuart_id_file):
    df_date_id = pd.read_csv(compra_date_stuart_id_file)
    date_stuart_str = df_date_id[df_date_id['date_compra']==date_compra_str]['date_stuart'].values[0]
    date_stuart_datetime = datetime.datetime.strptime(date_stuart_str, '%Y-%m-%d')
    path_stuart = ('/var/lib/lookiero/stock/stock_tool/stuart')
    stuart_folder = date_stuart_datetime.strftime('%Y%m%d')
    stuart_file = os.path.join(path_stuart, stuart_folder, 'stuart_output_todos.csv.gz')

    df_raw = pd.read_csv(stuart_file)
    df_raw[['family_desc', 'clima']] = df_raw['family_desc-clima'].str.split("-", expand=True)
    df_raw[['temp', 'size']] = df_raw['clase'].str.split("-", expand=True)
    df_raw = df_raw.rename(columns={'clase': 'size_desc'})
    df_stuart = df_raw[['family_desc', 'clima', 'size', 'recomendacion', 'size_desc']]
    return df_stuart


def merge_compra_real_stuart(df_compra_real, df_compra_stuart, file_save=None, file_save_date=None):

    # drop 'sin_clase'
    df_compra_stuart = df_compra_stuart[df_compra_stuart['clima'] != 'sin_clase']
    df_compra_stuart = df_compra_stuart[df_compra_stuart['size'] != 'sin_clase']



    # TODO: remove clima not in [] such as 1.66
    list_clima = [0., 0.5, 1., 1.5, 2., 2.5, 3.]
    df_compra_stuart = df_compra_stuart[~df_compra_stuart['clima'].isin(list_clima)]


    dic_clima = {'0.0': '0',
                 '1.0': '1',
                 '2.0': '2',
                 '3.0': '3'}

    df_compra_real['clima'] = df_compra_real['clima'].astype('str').replace(dic_clima)

    df = pd.merge(df_compra_stuart,
                  df_compra_real,
                  on=['family_desc', 'clima', 'size'],
                  how='outer')
    df['recomendacion'] = df['recomendacion'].fillna(0)
    df['compra_real'] = df['compra_real'].fillna(0)

    df['date_compra'] = date_compra_str

    if file_save is not None:

        if not os.path.isfile(file_save):
            print('Creating a new file ' + file_save)
            df.to_csv(file_save, mode='a', index=False, header=True)
        else:
            print('Appending to existing file ' + file_save)
            df.to_csv(file_save, mode='a', index=False, header=False)

    if file_save_date is not None:
        df.to_csv(file_save_date, index=False, header=True)



    return df


# run
date_compra_str = '2020-07-22'
date_stuart_str = '2020-08-03'
compra_file = ('/var/lib/lookiero/stock/stock_tool/kpi/compra/compra_reference_quantity - Sheet1.csv')
productos_file = ('/var/lib/lookiero/stock/stock_tool/productos_preprocessed.csv.gz')

path_save = ('/var/lib/lookiero/stock/stock_tool')
path_save_date = ('/var/lib/lookiero/stock/stock_tool/kpi/eval_real_history')

file_save = os.path.join(path_save, 'eval_estimates_real_compra.csv.gz')
file_save_date = os.path.join(path_save_date, 'eval_estimates_real_compra_' + date_compra_str + '.csv.gz')

# TODO: correct  stuart date
compra_date_stuart_id_file = ('/var/lib/lookiero/stock/stock_tool/kpi/compra/compra-date-stuart-id - Sheet1.csv')

df_compra_real = get_compra_real(date_compra_str, compra_file, productos_file)

df_compra_stuart = get_stuart_recommendation(date_compra_str, compra_date_stuart_id_file)

df = merge_compra_real_stuart(df_compra_real, df_compra_stuart, file_save, file_save_date)






# aa = df_compra_stuart[df_compra_stuart['clima'].isin(['sin_clase','no_definido'])]


#
# def get_compra_real(date_compra_str, compra_file, compra_dates_file, productos_file):
#     # TODO: download compra file from Google Drive
#
#     # compra realizada
#     # compra_file = ('/var/lib/lookiero/stock/stock_tool/kpi/compra/compra_referemce_quantity - Sheet1.csv')
#
#     # link between week and compra date
#     # compra_dates_file = ('/var/lib/lookiero/stock/stock_tool/kpi/compra/compra_fechas - Sheet1.csv')
#
#     query_compra_date_text = 'week == @date_compra_str'
#     df_date_compra = pd.read_csv(compra_dates_file).query(query_compra_date_text)
#     if df_date_compra.empty == False:
#         date_compra_str = df_date_compra['date_compra'].values[0]
#
#         query_compra_reference_text = 'date_compra == @date_compra_str'
#         df_compra_raw = pd.read_csv(compra_file,
#                                     usecols=['date_compra', 'reference', 'cantidad_pedida']
#                                     ).query(query_compra_reference_text)
#
#         list_references_compra = df_compra_raw["reference"].to_list()
#         df_compra_products = get_fam_size_clima(list_references_compra, productos_file, drop_duplicates=True)
#
#         df_compra_reference = pd.merge(df_compra_raw, df_compra_products, on='reference', how='left')
#         df_compra = df_compra_reference.groupby(['family_desc', 'clima', 'size']).agg({'cantidad_pedida': 'sum'}).reset_index()
#
#         df_compra['info_type'] = 'pedido'
#         df_compra = df_compra.rename(columns={'cantidad_pedida': 'q'})
#
#     else:
#         print('There is no date ' + date_compra_str + '. Please update the data here: ' + compra_dates_file)
#
#     return df_compra



#
# # run
#
# # compra realizada
# compra_file = ('/var/lib/lookiero/stock/stock_tool/kpi/compra/compra_referemce_quantity - Sheet1.csv')
#
# # link between week and compra date
# compra_dates_file = ('/var/lib/lookiero/stock/stock_tool/kpi/compra/compra_fechas - Sheet1.csv')
# compra_date_stuart_id = ('/var/lib/lookiero/stock/stock_tool/kpi/compra/compra-date-stuart-id - Sheet1.csv')
# productos_file = ('/var/lib/lookiero/stock/stock_tool/productos_preprocessed.csv.gz')
#
#
# date_compra_str = '2020-07-22'
# get_compra_real(date_compra_str, compra_file, compra_dates_file, productos_file)
#
#
# # date_compra	id_stuart	date_stuart