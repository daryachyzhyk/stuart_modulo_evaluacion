

import os
import glob
import pandas as pd
import numpy as np
import datetime

path_save = ('/var/lib/lookiero/stock/stock_tool')
path_save_date = ('/var/lib/lookiero/stock/stock_tool/kpi/eval_real_history')

stock_path = ('/var/lib/lookiero/stock/snapshots')
productos_file = ('/var/lib/lookiero/stock/stock_tool/productos_preprocessed.csv.gz')
venta_file = ('/var/lib/lookiero/stock/stock_tool/demanda.csv.gz')
pedidos_file = ('/var/lib/lookiero/stock/stock_tool/stuart/pedidos.csv.gz')
file_distribution_osfa = ('/var/lib/lookiero/stock/stock_tool/stuart/distribucion_osfa.csv.gz')
file_estimates = ('/var/lib/lookiero/stock/stock_tool/eval_estimates.csv.gz')
file_eval_settings = ('/var/lib/lookiero/stock/stock_tool/eval_settings.csv.gz')
path_save = ('/var/lib/lookiero/stock/stock_tool')
path_save_date = ('/var/lib/lookiero/stock/stock_tool/kpi/eval_real_history')


date_run = datetime.datetime(2020, 9, 14)

































import os
import glob
import pandas as pd
import numpy as np
import datetime
from google_drive_downloader import GoogleDriveDownloader as gdd



########################################
# stock real
def get_stock_real(date_start, date_end, stock_path, productos_file, how='monday'):
    # how='week_mean'

    # if stock_path is None:
    #     stock_path = ('/var/lib/lookiero/stock/snapshots')
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


file_stock = ('/var/lib/lookiero/stock/snapshots/20200803000004.csv.gz')
productos_file = ('/var/lib/lookiero/stock/stock_tool/productos_preprocessed.csv.gz')

df_stock_raw = pd.read_csv(file_stock)

# df_stock = df_stock_raw[(df_stock_raw['real_stock'] > 0) & (df_stock_raw['active'] > 0)]

df_stock = df_stock_raw[(df_stock_raw['es'] + df_stock_raw['fr'] + df_stock_raw['gb'] +
                         df_stock_raw['pt'] + df_stock_raw['be']+ df_stock_raw['lu'] +
                         df_stock_raw['it']) > 0]
# (es+fr+gb+pt+be+lu+it > 0)
df_products = pd.read_csv(productos_file, encoding="ISO-8859-1")
df_products = df_products[['family_desc', 'size', 'reference', 'clima']]
df_products = df_products.drop_duplicates(subset=['reference'])
df_stock_data = pd.merge(df_stock, df_products, on='reference', how='left')

df_stock_fct = df_stock_data.groupby(['family_desc', 'size', 'clima']).agg({'reference': 'count',
                                                                            'real_stock': 'sum'}).reset_index()

df_stock_fct.to_csv('/home/darya/Documents/stuart/data/kpi/eval_pruebas/test_stock/stock_test.csv')


# pendientes
pedidos_file = ('/var/lib/lookiero/stock/stock_tool/stuart/pedidos.csv.gz')

date_start = datetime.datetime(2020, 8, 3)


df_pedidos = pd.read_csv(pedidos_file, encoding="ISO-8859-1")

df_pedidos['date'] = pd.to_datetime(df_pedidos['date'])


df_pedidos['date_week'] = df_pedidos['date'] - pd.TimedeltaIndex(df_pedidos['date'].dt.dayofweek, unit='d')
df_pedidos['date_week'] = df_pedidos['date_week'].dt.date

df_pedidos = df_pedidos[df_pedidos['date_week'] == date_start.date()]

# references_list = df_pedidos['reference'].to_list()
# df_productos = get_fam_size_clima(references_list, file=productos_file, drop_duplicates=True,
#                                              family=False, size=True, clima=True)


# df_pedidos_recibido = df_products[['reference', 'recibido']]
df_pedidos_fct = pd.merge(df_pedidos[['reference', 'recibido']],
                          df_products,
                          on='reference',
                          how='left')



df_pendientes = df_pedidos_fct.groupby(['family_desc', 'clima', 'size']).agg({'recibido': 'sum'}).reset_index()

df_pendientes.to_csv('/home/darya/Documents/stuart/data/kpi/eval_pruebas/test_stock/pendientes_test.csv')







# test stock
df_real = pd.DataFrame([])
try:
    print('Getting stock real for the dates: ' + date_start_str + ' - ' + date_end_str)
    df_stock = get_stock_real(date_start, date_end, stock_path, productos_file, how='monday')
    df_real = df_real.append(df_stock)
except:
    print('Error in getting real stock')
    pass

try:
    print('Getting pendientes real')

    df_real = df_real.append(get_pendientes_real(date_start, pedidos_file, productos_file))
except:
    print('Error in getting pendientes real')
    pass






file_estimates = ('/var/lib/lookiero/stock/stock_tool/eval_estimates.csv.gz')

file_real = ('/var/lib/lookiero/stock/stock_tool/eval_real_data.csv.gz')

file_settings = ('/var/lib/lookiero/stock/stock_tool/eval_settings.csv.gz')

df_estimates_raw = pd.read_csv(file_estimates)

df_estimates_date = df_estimates_raw[df_estimates_raw['date_week'] == date_start_str]

df_estimates_date = df_estimates_date[df_estimates_date['id_stuart'] == df_estimates_date['id_stuart'].max()]

df_estimates_date = df_estimates_date[df_estimates_date['caracteristica'] == 'size']


df_estimates_date = df_estimates_date.rename(columns={'clase': 'size',
                                            'q': 'q_estimates'})

df_estimates_date = df_estimates_date[df_estimates_date['clima'] != 'sin_clase']

df_real = pd.read_csv(file_real)

df_real = df_real.rename(columns={'q': 'q_real'})

dic_clima = {'0.0': '0',
             '1.0': '1',

             '2.0': '2',
             '3.0':  '3'}

df_real['clima'] = df_real['clima'].replace(dic_clima)

df = pd.merge(df_estimates_date, df_real,
              on=['date_week', 'family_desc', 'clima', 'size', 'info_type'],
              how='outer')

df['q_real'] = df['q_real'].fillna(0)





df_estimates = df_estimates.rename(columns={'clase': 'size',
                                            'q': 'q_estimates'})
df_real = pd.read_csv(file_real)

df_real = df_real.rename(columns={'q': 'q_real'})






# plot
import seaborn as sns

df = pd.read_csv('/var/lib/lookiero/stock/stock_tool/eval_estimates_real.csv.gz')

df_stock_est = df[df['info_type'] == 'stock'][['family_desc', 'size', 'q_estimates']]
# flights = flights.pivot("month", "year", "passengers")
# df_plot = df_stock_est.pivot('family_desc', 'size', 'q_estimates')

df_plot = df_stock_est.groupby(['family_desc', 'size'])['q_estimates'].sum().unstack(-1)

# df_plot['q_estimates'] = df_plot['q_estimates'].astype(int)

df_plot = df_plot.fillna(0)


ax = sns.heatmap(df_plot, annot=True)


flights = sns.load_dataset("flights")
flights_piv = flights.pivot("month", "year", "passengers")
ax = sns.heatmap(flights)



# Update stock


compra_file = ('/var/lib/lookiero/stock/stock_tool/kpi/compra/compra_referemce_quantity - Sheet1.csv')

stock_path = ('/var/lib/lookiero/stock/snapshots')


productos_file = ('/var/lib/lookiero/stock/stock_tool/productos_preprocessed.csv.gz')

venta_file = ('/var/lib/lookiero/stock/stock_tool/demanda_preprocessed.csv.gz')

file_estimates = ('/var/lib/lookiero/stock/stock_tool/eval_estimates.csv.gz')

file_real = ('/var/lib/lookiero/stock/stock_tool/eval_real_data.csv.gz')

file_save = ('/var/lib/lookiero/stock/stock_tool/eval_estimates_real.csv.gz')

pedidos_file = ('/var/lib/lookiero/stock/stock_tool/stuart/pedidos.csv.gz')



