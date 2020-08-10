'''
Script to create the table of the real data last week:
* stock
* compra
* sent, envios
* returns, devos
* pendientes recibidos

The scrip take the date for analyze as a previous Monday to the day of run the code
The format of output table (columns): 'date_week', 'family_desc', 'clima', 'size', 'info_type', 'q'

'''

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


def get_current_season(date_):
    if isinstance(date_, datetime.datetime):
        date_fisrt_season = datetime.datetime(2016, 1, 1)

        # delta_month = (date_.year - date_fisrt_season.year) * 12 + date_.month - date_fisrt_season.month

        delta_season = (date_.year - date_fisrt_season.year) * 2
        if date_.month <= 6:
            season = delta_season + 1
        else:
            season = delta_season + 2
    else:
        print('Shoud be datetime')
        season = np.nan()
    return season


########################################
# stock real
def get_stock_real(date_start, date_end, stock_path=None, how='monday'):
    # how='week_mean'

    if stock_path is None:
        stock_path = ('/var/lib/lookiero/stock/snapshots')
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

    list_references_stock = df_stock_all["reference"].to_list()
    df_stock_products = get_fam_size_clima(list_references_stock, productos_file, drop_duplicates=True)

    df_stock_reference = pd.merge(df_stock_all, df_stock_products, on='reference', how='left')
    df_stock = df_stock_reference.groupby(['family_desc', 'clima', 'size']).agg({'real_stock': 'sum'}).reset_index()
    if how == 'week_mean':
        df_stock['real_stock'] = df_stock['real_stock'] / 7

    df_stock['info_type'] = 'stock'
    df_stock = df_stock.rename(columns={'real_stock': 'q'})
    return df_stock


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

        df_compra['info_type'] = 'pedido'
        df_compra = df_compra.rename(columns={'cantidad_pedida': 'q'})

    else:
        print('There is no date ' + date_start_str + '. Please update the data here: ' + compra_dates_file)

    return df_compra


def get_pendientes_real(date_start, pedidos_file=None, productos_file=None):
    if pedidos_file is None:
        pedidos_file = ('/var/lib/lookiero/stock/stock_tool/stuart/pedidos.csv.gz')
    if productos_file is None:
        productos_file = ('/var/lib/lookiero/stock/stock_tool/productos_preprocessed.csv.gz')

    df_pedidos = pd.read_csv(pedidos_file, encoding="ISO-8859-1")

    df_pedidos['date'] = pd.to_datetime(df_pedidos['date'])


    df_pedidos['date_week'] = df_pedidos['date'] - pd.TimedeltaIndex(df_pedidos['date'].dt.dayofweek, unit='d')
    df_pedidos['date_week'] = df_pedidos['date_week'].dt.date

    df_pedidos = df_pedidos[df_pedidos['date_week'] == date_start.date()]

    references_list = df_pedidos['reference'].to_list()
    df_productos = get_fam_size_clima(references_list, file=productos_file, drop_duplicates=True,
                                                 family=False, size=True, clima=True)

    df_pedidos_fct = pd.merge(df_pedidos[['reference', 'family_desc', 'recibido']],
                              df_productos,
                              on='reference',
                              how='left')



    df_pendientes = df_pedidos_fct.groupby(['family_desc', 'clima', 'size']).agg({'recibido': 'sum'}).reset_index()



    df_pendientes['info_type'] = 'pendientes'
    df_pendientes = df_pendientes.rename(columns={'recibido': 'q'})


    return df_pendientes


#
# def get_pendientes_real(date_actual, productos_file=None):
#     # pendientes_file = ('/var/lib/lookiero/stock/Pendiente_llegar')
#     if productos_file is None:
#         productos_file = ('/var/lib/lookiero/stock/stock_tool/productos_preprocessed.csv.gz')
#
#     # date_datetime = fecha_stock_actual_start
#     # date_str = pendientes_fecha_start
#
#     def load_pendientes(date_datetime):
#         '''
#         Load the PENDIENTES_XXX.txt from stock server based on the date in datetime format for the seasons not older then
#         previous to the actual season.
#
#         :param date_datetime: datetime.datetime
#             The date of the day
#         :return: pandas.DataFrame
#         '''
#
#         folder = ('/var/lib/lookiero/stock/Pendiente_llegar')
#         date_str = date_datetime.strftime('%d%m%Y')
#         file = os.path.join(folder, 'PENDIENTES_' + date_str + '.txt')
#         # pendientes_anteriro_file = os.path.join(pendientes_folder, 'PENDIENTES_' + pendientes_fecha_anterior + '.txt')
#
#         df_raw = pd.read_csv(file, sep=";", header=None, error_bad_lines=False, encoding="ISO-8859-1")
#
#         df_raw = df_raw.drop(df_raw.columns[-1], axis=1)
#
#         df_raw.columns = ["reference", "pendiente", "date", "family", "family_desc", "color", "temporada", "size",
#                           "brand", "precio_compra", "precio_compra_iva", "precio_compra_libras",
#                           "precio_compra_libras_iva"]
#
#         # calculate the season o use from .txt
#         # df_raw['season'] = df_raw['reference'].str.extract('(^[0-9]+)')
#         # df_raw['season'] = df_raw['season'].fillna('0')
#         # df_raw['season'] = df_raw['season'].astype(int)
#
#         season_actual = get_current_season(date_datetime)
#         df = df_raw[df_raw['temporada'] >= season_actual - 1]
#
#         return df
#
#     date_prior = date_actual - datetime.timedelta(days=7)
#     # fecha_pendientes_anterior = fecha_stock_actual_start - datetime.timedelta(days=7)
#     df_pendientes_actual_all = load_pendientes(date_actual)
#
#     df_pendientes_prior_all = load_pendientes(date_prior)
#
#     # add info about climate
#     # list_reference_pendientes = df_stock_all["reference"].to_list()
#     # query_product_text = 'reference in @list_reference_stock'
#
#
#     references_list = set(list(df_pendientes_actual_all.reference.to_list() +  df_pendientes_prior_all.reference.to_list()))
#
#     df_productos_all_ref_cl = get_fam_size_clima(references_list, file=productos_file, drop_duplicates=True,
#                                                  family=False, size=False, clima=True)
#
#
#
#
#     # df_productos_all_ref_cl = pd.read_csv(productos_file,
#     #                                       usecols=['reference', 'clima'])
#
#     df_pendientes_actual = pd.merge(df_pendientes_actual_all,
#                                     df_productos_all_ref_cl,
#                                     on='reference',
#                                     how='left')
#
#     df_pendientes_prior = pd.merge(df_pendientes_prior_all,
#                                       df_productos_all_ref_cl,
#                                       on='reference',
#                                       how='left')
#
#     df_pendientes_actual['clima'] = df_pendientes_actual['clima'].fillna('no_definido')
#     df_pendientes_prior['clima'] = df_pendientes_prior['clima'].fillna('no_definido')
#
#     df_pendientes_actual_fct = df_pendientes_actual.groupby(['family_desc', 'clima', 'size']).agg(
#         {'pendiente': 'sum'}).reset_index()
#
#     df_pendientes_anterior_fct = df_pendientes_prior.groupby(['family_desc', 'clima', 'size']).agg(
#         {'pendiente': 'sum'}).reset_index()
#
#     df_pendientes_actual_fct = df_pendientes_actual_fct.rename(columns={'pendiente': 'pendiente_actual'})
#     df_pendientes_anterior_fct = df_pendientes_anterior_fct.rename(columns={'pendiente': 'pendiente_anterior'})
#
#
#     df_pendientes_fct = pd.merge(df_pendientes_actual_fct,
#                                 df_pendientes_anterior_fct,
#                                 on=['family_desc', 'clima', 'size'])
#
#     df_pendientes_fct['pendiente_real'] = np.abs(
#         df_pendientes_fct['pendiente_anterior'] - df_pendientes_fct['pendiente_actual'])
#
#     df_pendientes_fct['info_type'] = 'pendientes'
#     df_pendientes_fct = df_pendientes_fct.rename(columns={'pendiente_real': 'q'})
#
#
#     df_pendientes_fct  = df_pendientes_fct.drop(columns=['pendiente_actual', 'pendiente_anterior'])
#
#     return df_pendientes_fct


def get_devos_real(date_start_str, date_end_str, venta_file=None, productos_file=None):

    if venta_file is None:
        venta_file = ('/var/lib/lookiero/stock/stock_tool/demanda_preprocessed.csv.gz')

    query_devos_text = 'date_terminated >= @date_start_str and date_terminated <= @date_end_str and purchased == 0'

    df_devos_raw = pd.read_csv(venta_file,
                                    usecols=['reference', 'family_desc', 'size', 'date_terminated', 'purchased']
                                    # 'date_co',    'date_ps_done'
                                    ).query(query_devos_text)

    df_products_ref_cl = get_fam_size_clima(df_devos_raw.reference.to_list(), file=productos_file,
                                            drop_duplicates=True, family=False, size=False, clima=True)


    df_devos_ref_cl = pd.merge(df_devos_raw,
                               df_products_ref_cl,
                               on='reference',
                               how='left')

    df_devos = df_devos_ref_cl.groupby(['family_desc', 'clima', 'size']).agg({'reference': 'count'}).reset_index()

    df_devos['info_type'] = 'devos'
    df_devos = df_devos.rename(columns={'reference': 'q'})


    return df_devos


def get_envios_real(date_start_str, date_end_str, venta_file=None, productos_file=None):

    if venta_file is None:
        venta_file = ('/var/lib/lookiero/stock/stock_tool/demanda_preprocessed.csv.gz')

    query_envios_text = 'date_ps_done >= @date_start_str and date_ps_done <= @date_end_str'

    df_envios_raw = pd.read_csv(venta_file,
                               usecols=['reference', 'family_desc', 'size', 'date_ps_done']
                                # 'date_co',, 'date_terminated', 'purchased'
                               ).query(query_envios_text)

    df_products_ref_cl = get_fam_size_clima(df_envios_raw.reference.to_list(), file=productos_file,
                                            drop_duplicates=True, family=False, size=False, clima=True)

    df_envios_ref_cl = pd.merge(df_envios_raw,
                               df_products_ref_cl,
                               on='reference',
                               how='left')

    df_envios = df_envios_ref_cl.groupby(['family_desc', 'clima', 'size']).agg({'reference': 'count'}).reset_index()

    df_envios['info_type'] = 'envios'
    df_envios = df_envios.rename(columns={'reference': 'q'})

    return df_envios


def merge_eval_estimates_real(date_start_str, file_estimates=None, file_real=None, file_save=None):
    print('Merging output of Stuart and real data')
    if file_estimates is None:
        file_estimates = ('/var/lib/lookiero/stock/stock_tool/eval_estimates.csv.gz')

    if file_real is None:
        file_real = ('/var/lib/lookiero/stock/stock_tool/eval_real_data.csv.gz')

    if file_save is None:
        file_save = ('/var/lib/lookiero/stock/stock_tool/eval_estimates_real.csv.gz')

    df_estimates_raw = pd.read_csv(file_estimates)

    df_estimates_raw = df_estimates_raw[df_estimates_raw['date_week'] == date_start_str]

    df_estimates_raw = df_estimates_raw[df_estimates_raw['id_stuart'] == df_estimates_raw['id_stuart'].max()]


    df_estimates = df_estimates_raw[df_estimates_raw['caracteristica'] == 'size']


    df_estimates = df_estimates.rename(columns={'clase': 'size',
                                                'q': 'q_estimates'})

    # drop 'sin_clase'
    df_estimates = df_estimates[df_estimates['clima'] != 'sin_clase']
    df_estimates = df_estimates[df_estimates['size'] != 'sin_clase']

    # drop pedido
    df_estimates = df_estimates[df_estimates['info_type'] != 'pedido']

    # TODO: remove clima not in [] such as 1.66
    list_clima = [0., 0.5, 1., 1.5, 2., 2.5, 3.]
    df_estimates = df_estimates[~df_estimates['clima'].isin(list_clima)]


    df_real = pd.read_csv(file_real)

    df_real = df_real.rename(columns={'q': 'q_real'})

    dic_clima = {'0.0': '0',
                 '1.0': '1',
                 '2.0': '2',
                 '3.0': '3'}

    df_real['clima'] = df_real['clima'].astype('str').replace(dic_clima)

    df = pd.merge(df_estimates, df_real,
                  on=['date_week', 'family_desc', 'clima', 'size', 'info_type'],
                  how='outer')
    df['q_real'] = df['q_real'].fillna(0)

    # TODO: as type integer or np.round
    df['q_estimates'] = df['q_estimates'].fillna(0).astype(int)

    df['q_dif'] = df['q_estimates'] - df['q_real']

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


    if not os.path.isfile(file_save):
        print('Creating a new file ' + file_save)
        df.to_csv(file_save, mode='a', index=False, header=True)
    else:
        print('Appending to existing file ' + file_save)
        df.to_csv(file_save, mode='a', index=False, header=False)

    return df



def apply_distribution_unq(df, file_distribution=None):
    print('Applying size distribution to the UNQ size for not accessories')
    if file_distribution is None:

        file_distribution = ('/var/lib/lookiero/stock/stock_tool/stuart/distribucion_osfa.csv.gz')
    df = df.reset_index(drop=True)
    distr_osfa = pd.read_csv(file_distribution)
    distr_osfa = distr_osfa.fillna(0)

    list_family_unq = ['ABRIGO', 'BLUSA', 'CAMISETA', 'CARDIGAN', 'CHAQUETA', 'DENIM', 'FALDA', 'JERSEY', 'JUMPSUIT',
                       'PANTALON', 'PARKA', 'SHORT', 'SUDADERA', 'TOP', 'TRENCH', 'VESTIDO']

    df_unq = df[(df['family_desc'].isin(list_family_unq)) & (df['size'] == 'UNQ')]


    df_unq = df_unq.rename(columns={'size': 'size_unq',
                                                'q': 'q_unq'})
    df_osfa = pd.merge(df_unq, distr_osfa, on='family_desc')

    df_osfa['q_osfa'] = df_osfa['q_unq'] * df_osfa['osfa']
    # df_osfa['q'] = df_osfa['q_unq'] + df_osfa['q_osfa']

    # df_real[(df_real['family_desc'].isin(list_family_unq)) & (df_real['size'] == 'UNQ')] = df_real_osfa

    df = df.drop(df_unq.index)
    # df2 = df[~df.index.isin(df_unq.index)]


    df = pd.merge(df, df_osfa, on=['family_desc', 'clima', 'size', 'info_type'], how='left')

    df['q_osfa'] = df['q_osfa'].fillna(0)
    df['q'] = df['q'] + df['q_osfa']

    # df = df.append(df_osfa)
    df = df.drop(columns=['size_unq', 'q_unq', 'osfa', 'q_osfa'])
    return df

####################################################################
# run

####################################################################################################################
# Date to analyze
day_today = datetime.datetime.now()
# TODO: eliminate test

# day_today = day_today - datetime.timedelta(days = 21) ######### !!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!




date_start = day_today - datetime.timedelta(days = 7 + day_today.weekday())

date_start_str = datetime.datetime.strftime(date_start, '%Y-%m-%d')
date_end = date_start + datetime.timedelta(days=6)

date_end_str = datetime.datetime.strftime(date_end, '%Y-%m-%d')


# fecha_stock_actual_start_str = '2020-07-13'


######################################################################################3

# path
stock_path = ('/var/lib/lookiero/stock/snapshots')

productos_file = ('/var/lib/lookiero/stock/stock_tool/productos_preprocessed.csv.gz')


path_save = ('/var/lib/lookiero/stock/stock_tool/kpi/eval_real_history')


#########################################################


df_real = pd.DataFrame([])
try:
    print('Getting stock real for the dates: ' + date_start_str + ' - ' + date_end_str)
    df_stock = get_stock_real(date_start, date_end, stock_path, how='monday')
    df_real = df_real.append(df_stock)
except:
    print('Error in getting real stock')
    pass

# try:
#     print('Getting compra real')
#     df_real = df_real.append(get_compra_real(date_start_str))
# except:
#     print('Error in getting real compra. Check if data is updated on the Google drive')
#     pass

try:
    print('Getting pendientes real')

    df_real = df_real.append(get_pendientes_real(date_start))
except:
    print('Error in getting pendientes real')
    pass

try:
    print('Getting devos real')

    df_real = df_real.append(get_devos_real(date_start_str, date_end_str, venta_file=None, productos_file=None))
except:
    print('Error in getting devos compra')
    pass


try:
    print('Getting envios real')

    df_real = df_real.append(get_envios_real(date_start_str, date_end_str, venta_file=None, productos_file=None))
except:
    print('Error in getting envios compra')
    pass

# df_real1 = df_real.copy()




df_real = apply_distribution_unq(df_real)


# df_real3 = df_real2.groupby(['family_desc', 'clima', 'size', 'info_type']).agg({'q': 'sum'}).reset_index()



# add dates
df_real['date_week'] = date_start.date()
# save
name_save = 'eval_real_data.csv.gz'
name_save_date = 'eval_real_data_' + date_start_str + '.csv.gz'

path_save_date = ('/var/lib/lookiero/stock/stock_tool/kpi/eval_real_history')
# df_real.to_csv(os.path.join(path_save, name_save), mode='a', index=False) # , header=False

if not os.path.isfile(os.path.join(path_save_date, name_save_date)):
   df_real.to_csv(os.path.join(path_save_date, name_save_date), mode='a', index=False, header=True)
else: # else it exists so append without writing the header
   df_real.to_csv(os.path.join(path_save_date, name_save_date), mode='a', index=False, header=False)


# # if file does not exist write header
# if not os.path.isfile(os.path.join(path_save, name_save)):
#    df_real.to_csv(os.path.join(path_save, name_save), mode='a', index=False, header=True)
# else: # else it exists so append without writing the header
#    df_real.to_csv(os.path.join(path_save, name_save), mode='a', index=False, header=False)






merge_eval_estimates_real(date_start_str, file_estimates=None, file_real=None, file_save=None)





# df_pendientes_1 = get_pendientes_real(date_start, pedidos_file=None, productos_file=None)

# df_devos_real = get_devos_real(date_start_str, date_end_str, venta_file=None, productos_file=None)