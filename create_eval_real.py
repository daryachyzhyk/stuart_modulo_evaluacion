'''
Script to create the table of the real data last week:
* stock
* buing
* sent
* returns
*

The scrip take the date for analyze as a previous Monday to the day of run the code
The format of output table (columns): date_week	family_desc	clima	size	info_type	q



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
def get_stock_real(date_start, date_end, stock_path):

    delta_date_stock = date_end - date_start

    df_stock_all = pd.DataFrame([])

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


    # add clima, family_desc and size
    list_references_stock = df_stock_all["reference"].to_list()
    df_stock_products = get_fam_size_clima(list_references_stock, productos_file, drop_duplicates=True)

    df_stock_reference = pd.merge(df_stock_all, df_stock_products, on='reference', how='left')

    df_stock = df_stock_reference.groupby(['family_desc', 'clima', 'size']).agg({'real_stock': 'sum'}).reset_index()

    df_stock['info_type'] = 'stock'
    df_stock = df_stock.rename(columns={'real_stock': 'q'})
    return df_stock

df_stock1 = get_stock_real(date_start, date_end, stock_path)





#
# delta_date_stock = date_end - date_start
#
# # TODO: create df['reference', 'clima']
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