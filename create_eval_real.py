"""
Script to create the table that contains estimations by Stuart and real data for the last week.
We run this script avery Monday.
Data to join
* stock
* shoppments or envios
* returns or devos
* pendientes recibidos

The scrip take the start date for analyze as a previous Monday to the Monday of run the code
The format of output table (columns): 'date_week', 'family_desc', 'clima', 'size', 'info_type', 'q'

Data extraction as pandas DataFrame is appended to the historic data at the stock server

@author: dchyzhyk
"""

import os
import glob
import pandas as pd
import numpy as np
import datetime


def get_fam_size_clima(references, file=None, drop_duplicates=True, family=True, size=True, clima=True):
    """
    Get 'family_desc', 'size', 'clima' for given list of references

    :param references: list
        list of references, what we use in the query
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


def get_current_season(date_):
    """
    Return the season of the indicates date
    :param date_: date.time
        date
    :return: int
        number of the season
    """

    if isinstance(date_, datetime.datetime):
        date_fisrt_season = datetime.datetime(2016, 1, 1)
        delta_season = (date_.year - date_fisrt_season.year) * 2
        if date_.month <= 6:
            season = delta_season + 1
        else:
            season = delta_season + 2
    else:
        print('Shoud be datetime')
        season = np.nan()
    return season


def get_stock_real(date_start, date_end, stock_path, productos_file, how='monday'):
    """
    Extract stock for the indicated window  of time (in case of 'week_mean') and
    point of time in case of "monday" from the snapshots.
    Since there are several snapshors for the same day, we select the very first

    :param date_start: datetime
        start of the period of interest
    :param date_end: datetime
        end of the period of interest
    :param stock_path: str
        path to the snapshots folder
    :param productos_file: str
        path to the product file
    :param how: str, default 'monday'
        if 'monday', take snapshots just for the one day, if 'week_mean': take mean for the week
    :return: pandas.DataFrame
        real stock
    """

    df_stock_all = pd.DataFrame([])

    if how == 'week_mean':

        #  should be checked
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

            print('Loading stock: ', stock_file)
            df_stock_day['date'] = day

            df_stock_all = df_stock_all.append(df_stock_day)
    elif how == 'monday':
        stock_fecha = date_start.strftime('%Y%m%d')

        stock_file = sorted(glob.glob(os.path.join(stock_path, stock_fecha + '*')))[0]
        print('Loading stock snapshot ' + stock_file)
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


def get_pendientes_real(date_start, pedidos_file, productos_file):
    """
    Get state of pendientes at the specific day start_day
    """

    df_pedidos = pd.read_csv(pedidos_file, encoding="ISO-8859-1")
    print('Loading pedidos: ' + pedidos_file + ' filtrando por la fecha ' + date_start.strftime('%d%m%Y'))
    df_pedidos['date'] = pd.to_datetime(df_pedidos['date'])

    # Monday of the week
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


def get_devos_real(date_start_str, date_end_str, venta_file, productos_file):
    """
    Geting all the devos arriving in the time window
    """
    print('Getting devos real for the dates: ' + date_start_str + ' - ' + date_end_str)

    query_devos_text = 'date_terminated >= @date_start_str and date_terminated <= @date_end_str and purchased == 0'

    df_devos_raw = pd.read_csv(venta_file,
                               usecols=['reference', 'family_desc', 'size', 'date_terminated', 'purchased']
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


def get_envios_real(date_start_str, date_end_str, venta_file, productos_file):
    """
    Geting all the envios (shipments) arriving in the time window
    """
    print('Getting envios real for the dates: ' + date_start_str + ' - ' + date_end_str)

    query_envios_text = 'date_ps_done >= @date_start_str and date_ps_done <= @date_end_str'

    df_envios_raw = pd.read_csv(venta_file,
                                usecols=['reference', 'family_desc', 'size', 'date_ps_done']
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


def apply_distribution_unq(df, file_distribution):
    """
    Function that allow to apply distribution of sizes to UNQ size in case of not accessories families.
    For example, Stuart does not predict PANTALON size UNQ, but we have it in the stock
    and in order to compare we apply apriory calculated distribution.
    """
    print('Applying size distribution to the UNQ size for not accessories')
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
    df = df.drop(df_unq.index)
    df = pd.merge(df, df_osfa, on=['family_desc', 'clima', 'size', 'info_type'], how='left')

    df['q_osfa'] = df['q_osfa'].fillna(0)
    df['q'] = df['q'] + df['q_osfa']
    df = df.drop(columns=['size_unq', 'q_unq', 'osfa', 'q_osfa'])
    return df


def merge_eval_estimates_real(date_start, file_estimates, file_eval_settings, file_real, file_save):
    """
    Merge real data with Stuart projections
    :param date_start: date.time
        date
    :param file_estimates: str
        path
    :param file_eval_settings: str
        path to the file, that contains id_stuart for the specific shoppinf, date when the units of this shopping
        appear at stock and number of weeks that are covered fot this shopping
    :param file_real:
    :param file_save:
    :return:
    """
    print('Merging output of Stuart and real data by the date ', date_start)

    df_estimates_raw = pd.read_csv(file_estimates)
    df_settings_raw = pd.read_csv(file_eval_settings)

    df_settings = df_settings_raw.groupby(['date_shopping']).agg({'id_stuart': 'max',
                                                                  'date_order_in_stock': 'last',
                                                                  'n_weeks_recommendation': 'last'}).reset_index()

    df_id_dates = pd.DataFrame([])
    for index, row in df_settings.iterrows():
        df_id = pd.DataFrame([])

        df_id['date_week'] = pd.date_range(start=pd.to_datetime(row['date_order_in_stock']),
                                           end=pd.to_datetime(row['date_order_in_stock']) +
                                               datetime.timedelta(days=6) * row['n_weeks_recommendation'],
                                           freq='W-MON')

        df_id['id_stuart'] = row['id_stuart']
        df_id_dates = df_id_dates.append(df_id)

    df_id_dates['date_week'] = df_id_dates['date_week'].dt.strftime('%Y-%m-%d')
    df_estimates_raw = df_estimates_raw.merge(df_id_dates, on=['date_week', 'id_stuart'], how='inner')

    # clean overlapping dates for different recommendations

    # for testing
    # test = df_estimates_raw.groupby(['date_week', 'family_desc', 'clima', 'clase',
    #                                   'caracteristica', 'info_type']).agg({'id_stuart': 'count'}).reset_index()


    date_week_str = datetime.datetime.strftime(date_start, '%Y-%m-%d')
    df_estimates_raw = df_estimates_raw[df_estimates_raw['date_week'] == date_week_str]

    print('Print Stuart ID ', df_estimates_raw['id_stuart'].unique())
    df_estimates = df_estimates_raw[df_estimates_raw['caracteristica'] == 'size']

    df_estimates = df_estimates.rename(columns={'clase': 'size',
                                                'q': 'q_estimates'})

    # drop 'sin_clase'
    df_estimates = df_estimates[df_estimates['clima'] != 'sin_clase']
    df_estimates = df_estimates[df_estimates['size'] != 'sin_clase']

    # drop pedido
    df_estimates = df_estimates[df_estimates['info_type'] != 'pedido']

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

    # some id_stuart are empty, its related to the case where there is no Stuart recommendation, but we have real data
    # for particular family-clima-size
    df['id_stuart'] = df['id_stuart'].fillna(method='ffill')

    df['q_estimates'] = df['q_estimates'].fillna(0).astype(int)

    df['q_dif'] = df['q_estimates'] - df['q_real']

    # Calculate part of envios related to Stuar algorithms, without business plan

    df['q_estimates_alg'] = df['q_estimates'] / df.groupby(['id_stuart', 'date_week', 'info_type'])['q_estimates'].transform('sum')
    df['q_real_rel'] = df['q_real'] / df.groupby(['id_stuart', 'date_week', 'info_type'])['q_real'].transform('sum')

    df.loc[df['info_type'] != 'envios', 'q_estimates_alg'] = np.nan

    df['q_dif_alg'] = df['q_estimates_alg'] - df['q_real_rel']

    df['q_real'] = np.round(df['q_real'], 0)

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


####################################################################################################################
# run

def run_eval_estimates_real(date_run='today', stock_path=None, productos_file=None, pedidos_file=None,
                            venta_file=None, file_distribution_osfa=None, file_estimates=None, file_eval_settings=None,
                            path_save=None, path_save_date=None):
    ######################################################################################
    # Dates
    print('------------------------------------------------------------------------------------------------')
    if date_run == 'today':
        date_run = datetime.datetime.now()
    elif isinstance(date_run, datetime.datetime):
        print('Selected datetime is correct')
        pass
    else:
        print('Error: date_start should be datetime')

    date_start = date_run - datetime.timedelta(days=7 + date_run.weekday())
    date_end = date_start + datetime.timedelta(days=6)

    date_run_str = datetime.datetime.strftime(date_run, '%Y-%m-%d')
    date_start_str = datetime.datetime.strftime(date_start, '%Y-%m-%d')
    date_end_str = datetime.datetime.strftime(date_end, '%Y-%m-%d')


    print('Getting real data for ' + date_start_str + ' - ' + date_end_str + ' computed on ' + date_run_str)

    ######################################################################################
    # path
    if stock_path is None:
        stock_path = ('/var/lib/lookiero/stock/snapshots')
    if productos_file is None:
        productos_file = ('/var/lib/lookiero/stock/stock_tool/productos_preprocessed.csv.gz')
    if venta_file is None:
        # venta_file = ('/var/lib/lookiero/stock/stock_tool/demanda_preprocessed.csv.gz')
        venta_file = ('/var/lib/lookiero/stock/stock_tool/demanda.csv.gz')
    if pedidos_file is None:
        pedidos_file = ('/var/lib/lookiero/stock/stock_tool/stuart/pedidos.csv.gz')
    if file_distribution_osfa is None:
        file_distribution_osfa = ('/var/lib/lookiero/stock/stock_tool/stuart/distribucion_osfa.csv.gz')
    if file_estimates is None:
        file_estimates = ('/var/lib/lookiero/stock/stock_tool/eval_estimates.csv.gz')
    if file_eval_settings is None:
        file_eval_settings = ('/var/lib/lookiero/stock/stock_tool/eval_settings.csv.gz')
    if path_save is None:
        path_save = ('/var/lib/lookiero/stock/stock_tool')
    if path_save_date is None:
        path_save_date = ('/var/lib/lookiero/stock/stock_tool/kpi/eval_real_history')

    #########################################################



    df_real = pd.DataFrame([])
    try:
        # stock
        df_stock = get_stock_real(date_start, date_end, stock_path, productos_file, how='monday')
        df_real = df_real.append(df_stock)
    except:
        print('Error in getting real stock')
        pass

    try:
        # pendientes
        df_real = df_real.append(get_pendientes_real(date_start, pedidos_file, productos_file))
    except:
        print('Error in getting pendientes real')
        pass

    try:
        # devos
        df_real = df_real.append(get_devos_real(date_start_str, date_end_str, venta_file, productos_file))
    except:
        print('Error in getting devos compra')
        pass

    try:
        # real
        df_real = df_real.append(get_envios_real(date_start_str, date_end_str, venta_file, productos_file))
    except:
        print('Error in getting envios compra')
        pass

    df_real = apply_distribution_unq(df_real, file_distribution_osfa)

    df_real['date_week'] = date_start.date()

    test = df_real[df_real['size'] == 'UNQ']

    # save
    name_save = 'eval_real_data.csv.gz'
    name_save_date = 'eval_real_data_' + date_start_str + '.csv.gz'
    name_save_merged = 'eval_estimates_real.csv.gz'

    # save historical data with date in the file name

    df_real.to_csv(os.path.join(path_save_date, name_save_date), index=False, header=True)

    # save appended data
    if not os.path.isfile(os.path.join(path_save, name_save)):
       df_real.to_csv(os.path.join(path_save, name_save), mode='a', index=False, header=True)
       print('Creating new file ' + os.path.join(path_save, name_save))
    else:
       df_real.to_csv(os.path.join(path_save, name_save), mode='a', index=False, header=False)
       print('Appending to already exist file ' + os.path.join(path_save, name_save))

    file_real = os.path.join(path_save_date, name_save_date)
    file_merged_save = os.path.join(path_save, name_save_merged)


    df_merged = merge_eval_estimates_real(date_start,
                                          file_estimates,
                                          file_eval_settings,
                                          file_real=file_real,
                                          file_save=file_merged_save)


if __name__ == "__main__":

    # path
    path_save = ('/var/lib/lookiero/stock/stock_tool')
    path_save_date = ('/var/lib/lookiero/stock/stock_tool/kpi/eval_real_history')

    date_to_run = 'today'                           # usual case, we run it every Monday
    # date_to_run = 'all_days'                      # uncomment when run all the history from beginning
    # date_to_run = datetime.datetime(2020, 8, 24)  # uncomment if want run for some specific date

    if date_to_run == 'today':
        run_eval_estimates_real(date_run='today', path_save=path_save, path_save_date=path_save_date)

    elif date_to_run == 'all_days':
        print('You are run all the history from 2020-8-24 until today')
        print('...Deleting the previous history files in ' + os.path.join(path_save, 'eval_real_data.csv.gz')
              + ' and ' + os.path.join(path_save, 'eval_estimates_real.csv.gz'))

        if os.path.join(path_save, 'eval_real_data.csv.gz'):
            os.remove(os.path.join(path_save, 'eval_real_data.csv.gz'))
        if os.path.join(path_save, 'eval_estimates_real.csv.gz'):
            os.remove(os.path.join(path_save, 'eval_estimates_real.csv.gz'))

        for date in pd.date_range(start=datetime.datetime(2020, 8, 24),
                                  end=datetime.datetime.now(),
                                  freq='W-MON'):
            print(date)
            run_eval_estimates_real(date_run=date, path_save=path_save, path_save_date=path_save_date)
    else:
        run_eval_estimates_real(date_run=date_to_run, path_save=path_save, path_save_date=path_save_date)


