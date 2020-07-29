'''
Script to create the table of the real data last week:
* stock
* buing
* sent
* returns
*

The scrip take the date for analyze as a previous Monday to the day of run the code
'''
import os
import glob
import pandas as pd
import seaborn as sns
import numpy as np
import matplotlib.pyplot as plt

import datetime

####################################################################################################################
# Date to analyze
day_today = datetime.date.today()
date_actual = day_today - datetime.timedelta(days = 7 + day_today.weekday())

fecha_stock_actual_start_str = '2020-07-13'
# fecha_stock_actual_end_str = '2020-07-19'
#
# fecha_compra = '2020-06-17'
#
# fecha_pedido_start = '2020-06-17'
# fecha_pedido_end = '2020-06-30'
#
#
# familias_interes = ['BLUSA', 'CAMISETA', 'FALDA', 'JUMPSUIT', 'SHORT', 'TOP', 'VESTIDO']

###################################################################################################################
# settings

dict_fechas = {'2020-07-13': ['2020-06-17', '2020-06-16'],
               '2020-07-13': ['2020-06-17', '2020-06-16'],
               '2020-07-20': ['2020-06-17', '2020-06-16'],
               '2020-07-27': ['2020-06-17', '2020-06-16'],
               '2020-08-03': ['2020-07-03', '2020-07-02'],
               '2020-08-10': ['2020-07-03', '2020-07-02'],
               '2020-08-17': ['2020-07-03', '2020-07-02'],
               '2020-08-24': ['2020-07-03', '2020-07-02']}
dic_clima = {0.: 'cold',
             0.5: 'cold_soft_cold',
             1.: 'soft_cold',
             1.5: 'soft_cold_soft_warm',
             2.: 'soft_warm',
             2.5: 'soft_warm_warm',
             3.:  'warm'}

dic_clima = {0.: '0_invierno',
             0.5: '0.5_invierno',
             1.: '1_invierno',
             1.5: '1.5_entretiempo',
             2.: '2_verano',
             2.5: '2.5_verano',
             3.:  '3_verano'}

dic_clima = {0.: '0_inv',
             0.5: '0.5_inv',
             1.: '1_inv',
             1.5: '1.5_ent',
             2.: '2_ver',
             2.5: '2.5_ver',
             3.:  '3_ver'}

# path
stock_folder = ('/var/lib/lookiero/stock/snapshots')

productos_file = ('/var/lib/lookiero/stock/stock_tool/productos_preprocessed.csv.gz')

stuart_folder = ('/var/lib/lookiero/stock/stock_tool/stuart')


path_results = ('/home/darya/Documents/Reports/2020-07-17-kpi-roturas-particular-case')

######
# fechas
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