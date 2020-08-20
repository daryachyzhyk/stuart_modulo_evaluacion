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



def get_compra_real(date_start_str, compra_file, compra_dates_file, productos_file):
    # TODO: download compra file from Google Drive

    # compra realizada
    # compra_file = ('/var/lib/lookiero/stock/stock_tool/kpi/compra/compra_referemce_quantity - Sheet1.csv')

    # link between week and compra date
    # compra_dates_file = ('/var/lib/lookiero/stock/stock_tool/kpi/compra/compra_fechas - Sheet1.csv')

    query_compra_date_text = 'week == @date_start_str'
    df_date_compra = pd.read_csv(compra_dates_file).query(query_compra_date_text)
    if df_date_compra.empty == False:
        date_compra_str = df_date_compra['date_compra'].values[0]

        query_compra_reference_text = 'date_compra == @date_compra_str'
        df_compra_raw = pd.read_csv(compra_file,
                                    usecols=['date_compra', 'reference', 'cantidad_pedida']
                                    ).query(query_compra_reference_text)

        list_references_compra = df_compra_raw["reference"].to_list()
        df_compra_products = get_fam_size_clima(list_references_compra, productos_file, drop_duplicates=True)

        df_compra_reference = pd.merge(df_compra_raw, df_compra_products, on='reference', how='left')
        df_compra = df_compra_reference.groupby(['family_desc', 'clima', 'size']).agg({'cantidad_pedida': 'sum'}).reset_index()

        df_compra['info_type'] = 'pedido'
        df_compra = df_compra.rename(columns={'cantidad_pedida': 'q'})

    else:
        print('There is no date ' + date_start_str + '. Please update the data here: ' + compra_dates_file)

    return df_compra




# run

# compra realizada
compra_file = ('/var/lib/lookiero/stock/stock_tool/kpi/compra/compra_referemce_quantity - Sheet1.csv')

# link between week and compra date
compra_dates_file = ('/var/lib/lookiero/stock/stock_tool/kpi/compra/compra_fechas - Sheet1.csv')
compra_date_stuart_id = ('/var/lib/lookiero/stock/stock_tool/kpi/compra/compra-date-stuart-id - Sheet1.csv')
productos_file = ('/var/lib/lookiero/stock/stock_tool/productos_preprocessed.csv.gz')


date_start_str = '2020'
get_compra_real(date_start_str, compra_file, compra_dates_file, productos_file)