'''
Script to create the table of the real data last week:
* stock
* compra
* sent
* returns
*

The scrip take the date for analyze as a previous Monday to the day of run the code
The format of output table (columns): 'date_week', 'family_desc', 'clima', 'size', 'info_type', 'q'

'''




import os
import glob
import pandas as pd
import seaborn as sns
import numpy as np
import matplotlib.pyplot as plt

import datetime


def get_fam_size_clima(references, file, drop_duplicates=True):
    '''
    Get 'family_desc', 'size', 'clima' for given list of references
    :param references: list
        list of references
    :param file: str
        path to csv file, '/var/lib/lookiero/stock/stock_tool/productos_preprocessed.csv.gz'
    :return:
    '''

    query_product_text = 'reference in @references'

    df = pd.read_csv(file, usecols=['reference', 'family_desc', 'size', 'clima']).query(query_product_text)
    if drop_duplicates:
        df = df.drop_duplicates('reference', keep='last')
    return df
####################################################################################################################
# Date to analyze
day_today = datetime.datetime.now()
date_start = day_today - datetime.timedelta(days = 7 + day_today.weekday())

date_start_str = datetime.datetime.strftime(date_start, '%Y-%m-%d')
date_end = date_start + datetime.timedelta(days = 6)

date_end_str = datetime.datetime.strftime(date_end, '%Y-%m-%d')


# fecha_stock_actual_start_str = '2020-07-13'


######################################################################################3

# path
stock_path = ('/var/lib/lookiero/stock/snapshots')

productos_file = ('/var/lib/lookiero/stock/stock_tool/productos_preprocessed.csv.gz')

# TODO: change to stock server
path_save = ('/home/darya/Documents/Reports/2020-07-17-kpi-roturas-particular-case')


#########################################################
########################################
# stock actual
def get_stock_real(date_start, date_end, stock_path, how='monday'):
    # how='week_mean'
    df_stock_all = pd.DataFrame([])

    if how == 'week_mean':
        delta_date_stock = date_end - date_start



        for i in range(delta_date_stock.days + 1):
            day = date_start + datetime.timedelta(days=i)
            print(day)
            stock_fecha = day.strftime('%Y%m%d')
            stock_file = sorted(glob.glob(os.path.join(stock_path, stock_fecha + '*')))[0]
            print(stock_file)
            query_stock_text = 'real_stock > 0 and active > 0'

            df_stock_day = pd.read_csv(stock_file,
                                       usecols=['reference', 'family', 'real_stock', 'active']
                                       ).query(query_stock_text)
            df_stock_day['date'] = day

            df_stock_all = df_stock_all.append(df_stock_day)
    elif how == 'monday':
        print(date_start)
        stock_fecha = date_start.strftime('%Y%m%d')
        stock_file = sorted(glob.glob(os.path.join(stock_path, stock_fecha + '*')))[0]
        print(stock_file)
        query_stock_text = 'real_stock > 0 and active > 0'

        df_stock_all = pd.read_csv(stock_file,
                                   usecols=['reference', 'family', 'real_stock', 'active']
                                   ).query(query_stock_text)
        df_stock_all['date'] = date_start




    # add clima, family_desc and size
    list_references_stock = df_stock_all["reference"].to_list()
    df_stock_products = get_fam_size_clima(list_references_stock, productos_file, drop_duplicates=True)

    df_stock_reference = pd.merge(df_stock_all, df_stock_products, on='reference', how='left')
    df_stock = df_stock_reference.groupby(['family_desc', 'clima', 'size']).agg({'real_stock': 'sum'}).reset_index()
    if how == 'week_mean':
        df_stock['real_stock'] = df_stock['real_stock'] / 7

    df_stock['info_type'] = 'stock'
    df_stock = df_stock.rename(columns={'real_stock': 'q'})
    return df_stock

df_stock = get_stock_real(date_start, date_end, stock_path, how='monday')



################################################################################################################
# load Compra
def get_compra_real(date_start_str):
    # TODO: download compra file from Google Drive

    # compra realizada
    compra_file = ('/var/lib/lookiero/stock/stock_tool/kpi/compra/compra_referemce_quantity - Sheet1.csv')

    # link between week and compra date
    compra_dates_file = ('/var/lib/lookiero/stock/stock_tool/kpi/compra/compra_fechas - Sheet1.csv')

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

        df_compra['info_type'] = 'compra'
        df_compra = df_compra.rename(columns={'cantidad_pedida': 'q'})

    else:
        print('There is no date ' + date_start_str + '. Please update the data here: ' + compra_dates_file)

    return df_compra

df_compra = get_compra_real(date_start_str)

#################################################################3
# Pendientes





# campos fichero PENDIENTES.txt
# "reference","pendiente","date","family","family_desc","color","temporada","size","brand","precio_compra","precio_compra_iva","precio_compra_libras","precio_compra_libras_iva","NA"
# date es fecha prevista de recibir
# campos fichero PEDIDOS_RECIBIDOS.txt
# "date","reference","pedidos","family","family_desc","date2","brand","precio_compra","precio_compra_iva","precio_compra_libras","precio_compra_libras_iva","NA"
# date es fecha de recepción

# datetime.

# pendientes_file = ('/var/lib/lookiero/stock/stock_tool/stuart2/20200702/pedidos.csv.gz')

pendientes_folder = ('/var/lib/lookiero/stock/Pendiente_llegar')



fecha_pendientes_anterior = fecha_stock_actual_start - datetime.timedelta(days=7)

# date_datetime = fecha_stock_actual_start
# date_str = pendientes_fecha_start




# delta_date_stock = date_end - date_start
#

#
# df_stock_all = pd.DataFrame([])
#
# for i in range(delta_date_stock.days + 1):
#     day = date_start + datetime.timedelta(days=i)
#     print(day)
#     stock_fecha = day.strftime('%Y%m%d')
#     stock_file = sorted(glob.glob(os.path.join(stock_path, stock_fecha + '*')))[0]
#     print(stock_file)
#     query_stock_text = 'real_stock > 0 and active > 0'
#
#     df_stock_day = pd.read_csv(stock_file,
#                                usecols=['reference', 'family', 'real_stock']
#                                ).query(query_stock_text)
#     df_stock_day['date'] = day
#
#     df_stock_all = df_stock_all.append(df_stock_day)
#
# # add clima, family_desc and size
# list_references_stock = df_stock_all["reference"].to_list()
# df_stock_products = get_fam_size_clima(list_references_stock, productos_file, drop_duplicates=True)
#
#
# df_stock_reference = pd.merge(df_stock_all, df_stock_products, on='reference', how='left')
#
# df_stock = df_stock_reference.groupby(['family_desc', 'clima', 'size']).agg({'real_stock': 'sum'}).reset_index()
#
# df_stock['info_type'] = 'stock'
# df_stock = df_stock.rename(columns={'real_stock': 'q'})
#
#


#######################################################
# info de cada prenda
list_reference_stock = df_stock_all["reference"].to_list()
query_product_text = 'reference in @list_reference_stock'


df_productos = pd.read_csv(productos_file,
                           usecols=['reference', 'family_desc', 'size', 'clima'] # , 'clima_grupo'
                           ).query(query_product_text)





#########################################################
# stock actual añadir familia, talla, clima

df_stock_reference = pd.merge(df_stock_all, df_productos, on='reference', how='left')



df_stock_familia_talla = df_stock_reference.groupby(['family_desc', 'size']).agg({'stock_real_week': 'sum',
                                                                                  'date': 'last'}).reset_index()

df_stock_familia_clima = df_stock_reference.groupby(['family_desc', 'clima']).agg({'stock_real_week': 'sum',
                                                                                   'date': 'last'}).reset_index()

df_stock_familia_clima['clima_desc'] = df_stock_familia_clima['clima'].replace(dic_clima)
df_stock_familia_clima['clima'] = df_stock_familia_clima['clima'].astype(str)

df_stock_familia_talla['stock_real'] = (df_stock_familia_talla['stock_real_week'] / 7)

df_stock_familia_clima['stock_real'] = (df_stock_familia_clima['stock_real_week'] / 7)



# df_stock_familia_talla['stock_mean'] = (df_stock_familia_talla['real_stock'] / 7).round(0)
# df_stock_familia_talla['stock_mean1'] = (df_stock_familia_talla['real_stock'] / 7).apply(np.ceil)


######
# fechas
date_stock_start
fecha_stock_actual_start = datetime.strptime(fecha_stock_actual_start_str, '%Y-%m-%d')
fecha_stock_actual_end = fecha_stock_actual_start + timedelta(days=6)

delta_fecha_stock_actual = fecha_stock_actual_end - fecha_stock_actual_start



fecha_compra_str = dict_fechas[fecha_stock_actual_start_str][0]

fecha_stuart_str = dict_fechas[fecha_stock_actual_start_str][1]
############################
# files
stock_proyeccion_file = (os.path.join'/var/lib/lookiero/stock/stock_tool/stuart/20200616/proyeccion_stock_todos.csv.gz')




def get_current_season(_date):
    if isinstance(_date, datetime.datetime):
        _date = _date.date()
    _date = _date.replace(year=2000)
    return [season for season, (start, end) in seasons if start <= _date <= end][0]