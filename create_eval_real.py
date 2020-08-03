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
# stock real
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

# df_compra = get_compra_real(date_start_str)

df_real = pd.DataFrame([])
try:
    print('Getting stock real for the dates: ' + date_start + ' - ' + date_end)
    df_stock = get_stock_real(date_start, date_end, stock_path, how='monday')
    df_real.append(df_stock)
except:
    print('Error in getting rel stock')
    pass

try:
    print('Getting compra real')
    df_stock = get_compra_real(date_start_str)
    df_real.append(df_stock)
except:
    print('Error in getting real compra')
    pass


#################################################################3
# Pendientes


def get_pendientes_real():
    pendientes_file = ('/var/lib/lookiero/stock/Pendiente_llegar')
    fecha_pendientes_anterior = fecha_stock_actual_start - datetime.timedelta(days=7)

    # date_datetime = fecha_stock_actual_start
    # date_str = pendientes_fecha_start

    def load_pendientes(date_datetime):
        '''
        Load the PENDIENTES_XXX.txt from stock server based on the date in datetime format for the seasons not older then
        previous to the actual season.

        :param date_datetime: datetime.datetime
            The date of the day
        :return: pandas.DataFrame
        '''

        folder = ('/var/lib/lookiero/stock/Pendiente_llegar')
        date_str = date_datetime.strftime('%d%m%Y')
        file = os.path.join(folder, 'PENDIENTES_' + date_str + '.txt')
        # pendientes_anteriro_file = os.path.join(pendientes_folder, 'PENDIENTES_' + pendientes_fecha_anterior + '.txt')

        df_raw = pd.read_csv(file, sep=";", header=None, error_bad_lines=False, encoding="ISO-8859-1")

        df_raw = df_raw.drop(df_raw.columns[-1], axis=1)

        df_raw.columns = ["reference", "pendiente", "date", "family", "family_desc", "color", "temporada", "size",
                          "brand", "precio_compra", "precio_compra_iva", "precio_compra_libras",
                          "precio_compra_libras_iva"]

        df_raw['season'] = df_raw['reference'].str.extract('(^[0-9]+)')

        df_raw['season'] = df_raw['season'].fillna('0')
        df_raw['season'] = df_raw['season'].astype(int)

        season_actual = get_current_season(date_datetime)
        df = df_raw[df_raw['season'] >= season_actual - 1]

        return df

    df_pendientes_actual_all = load_pendientes(fecha_stock_actual_start)

    df_pendientes_anterior_all = load_pendientes(fecha_pendientes_anterior)

    # add info about climate
    # list_reference_pendientes = df_stock_all["reference"].to_list()
    # query_product_text = 'reference in @list_reference_stock'

    df_productos_all_ref_cl = pd.read_csv(productos_file,
                                          usecols=['reference', 'clima'])

    df_pendientes_actual = pd.merge(df_pendientes_actual_all,
                                    df_productos_all_ref_cl.drop_duplicates('reference', keep='last'),
                                    on='reference',
                                    how='left')

    df_pendientes_anterior = pd.merge(df_pendientes_anterior_all,
                                      df_productos_all_ref_cl.drop_duplicates('reference', keep='last'),
                                      on='reference',
                                      how='left')

    df_pendientes_actual['clima'] = df_pendientes_actual['clima'].fillna('no_informado')
    df_pendientes_anterior['clima'] = df_pendientes_anterior['clima'].fillna('no_informado')

    # TODO: group and restar
    df_pendientes_actual_ft = df_pendientes_actual.groupby(['family_desc', 'size']).agg(
        {'pendiente': 'sum'}).reset_index()

    df_pendientes_anterior_ft = df_pendientes_anterior.groupby(['family_desc', 'size']).agg(
        {'pendiente': 'sum'}).reset_index()

    df_pendientes_actual_ft = df_pendientes_actual_ft.rename(columns={'pendiente': 'pendiente_actual'})

    df_pendientes_anterior_ft = df_pendientes_anterior_ft.rename(columns={'pendiente': 'pendiente_anterior'})

    df_pendientes_ft = pd.merge(df_pendientes_actual_ft,
                                df_pendientes_anterior_ft,
                                on=['family_desc', 'size'])

    df_pendientes_ft['pendiente_real'] = np.abs(
        df_pendientes_ft['pendiente_anterior'] - df_pendientes_ft['pendiente_actual'])

    df_pendientes_stuart_realidad_ft = pd.merge(df_pendientes_ft,
                                                df_proyeccion_familia_talla[
                                                    ['family_desc', 'size', 'pendientes', 'size_ord']],
                                                on=['family_desc', 'size'])

    df_pendientes_stuart_realidad_ft = df_pendientes_stuart_realidad_ft.rename(
        columns={'pendientes': 'pendiente_proyeccion'})

    df_pendientes_stuart_real_ft_merge = pd.melt(df_pendientes_stuart_realidad_ft,
                                                 id_vars=['family_desc', 'size', 'size_ord'],
                                                 value_vars=['pendiente_real', 'pendiente_proyeccion'],
                                                 var_name='pendiente_type',
                                                 value_name='pendiente'
                                                 )

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




###########################################
# Devos



query_devos_text = 'date_terminated >= @fecha_stock_actual_start_str and date_terminated <= @fecha_stock_actual_end_str and purchased == 0'

df_devos_real = pd.read_csv(venta_file,
                           usecols=['reference', 'family_desc', 'size', 'date_ps_done', 'date_co',
                                    'date_terminated', 'purchased']
                           ).query(query_devos_text)

# TODO: add reference climate

df_devos_real_ft = df_devos_real.groupby(['family_desc', 'size']).agg({'reference': 'count'}).reset_index()
df_devos_real_ft = df_devos_real_ft.rename(columns={'reference': 'devos_real'})

df_devos_stuart_real_ft = pd.merge(df_devos_real_ft,
                                            df_proyeccion_familia_talla[['family_desc', 'size', 'devos', 'size_ord']],
                                            on=['family_desc', 'size'])

df_devos_stuart_real_ft = df_devos_stuart_real_ft.rename(columns={'devos': 'devos_proyeccion'})

df_devos_stuart_real_ft_melt = pd.melt(df_devos_stuart_real_ft,
                                             id_vars=['family_desc', 'size', 'size_ord'],
                                             value_vars=['devos_real', 'devos_proyeccion'],
                                             var_name='devos_type',
                                             value_name='devos')

# df_devos_stuart_real_ft_melt = df_devos_stuart_real_ft_melt.sort_values(by=['family_desc', 'size_ord'])

size_order = ['XXS', 'XS', 'S', 'M', 'L', 'XL', 'XXL', 'XXXL', 'X4XL']

plot_catplot_stuart_real(df_devos_stuart_real_ft_melt,
                         x="size",
                         y="devos",
                         hue='devos_type',
                         col="family_desc",
                         col_wrap=4, title_name='Devos proyeccion vs real, familia - talla',
                         path_results=path_results,
                         file_name='plot_devos_stuart_real_familia_talla.png',
                         order=size_order)