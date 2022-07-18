import requests
import pandas as pd
import xlrd
import psycopg2
import db_connect
import re
import numpy as np
from datetime import datetime

auto_bcra_mon_fin_framework_runs = open('C:\\Users\\48575\\Documents\\GitHub\\macro_uni\\auto_bcra_mon_fin_framework_runs.txt', 'a')

###CURRENT DATE

run_time = f'Run on: {datetime.now().strftime("%d %b %Y: %H:%M:%S")} CET\n'
auto_bcra_mon_fin_framework_runs.write(f'{run_time}File Extraction and Data Transformation...\n')
print(f'{run_time}File Extraction and Data Transformation...\n')

### FILE EXTRACTION AND DATA TRANSFORMATION
df_bcra = pd.read_excel(
    'http://www.bcra.gov.ar/Pdfs/PublicacionesEstadisticas/panhis.xls',
    sheet_name = 'Cuadro',
    engine = 'xlrd',
    header=[25],
    dtype = {'bcrafec':str})

#    engine : str, default None
#        If io is not a buffer or path, this must be set to identify io.
#        Supported engines: "xlrd", "openpyxl", "odf", "pyxlsb", default "xlrd".
#        Engine compatibility :
#        - "xlrd" supports most old/new Excel file formats.
#        - "openpyxl" supports newer Excel file formats.
#        - "odf" supports OpenDocument file formats (.odf, .ods, .odt).
#        - "pyxlsb" supports Binary Excel files.

#     header : int, list of int, default 0
#        Row (0-indexed) to use for the column labels of the parsed
#        DataFrame. If a list of integers is passed those row positions will
#        be combined into a ``MultiIndex``. Use None if there is no header.

#    dtype : Type name or dict of column -> type, default None
#        Data type for data or columns. E.g. {'a': np.float64, 'b': np.int32}
#        Use `object` to preserve data as stored in Excel and not interpret dtype.
#        If converters are specified, they will be applied INSTEAD
#        of dtype conversion.

df_bcra = df_bcra.dropna(subset = ['sismes'])

#Dropping rows where year.month >>13th month
df_bcra = df_bcra[df_bcra['bcrafec'] != df_bcra['bcrafec'].str.match(r'(\d{4}.13)')]

#Fixing dates where year.1 not year.10 for Oct
df_bcra.insert(0,'Year',df_bcra['bcrafec'].str.extract(r'(\d{4})(.*)')[0])
df_bcra.insert(0,'Month',df_bcra['bcrafec'].str.extract(r'(\d{4}.)(.*)')[1])
df_bcra['Month'].replace('(^1$)', '10' , regex = True, inplace = True)

df_bcra.insert(0, 'Date', df_bcra['Year']+ df_bcra['Month'])

df_bcra = df_bcra.drop(['Unnamed: 4', 'Unnamed: 5', 'sisobs', 'sismes' , 'Unnamed: 3', 'bcrafec', 'Month', 'Year'], axis =1)

#for the full history quote the line below
df_bcra = df_bcra[df_bcra['Date']>'202100']

df_bcra['Date'] = pd.to_datetime(df_bcra['Date'], format = '%Y%m', errors='coerce')

df_bcra.replace('...', np.nan, inplace =True)
df_bcra.replace('^.$', np.nan, regex = True, inplace =True)

source_vs_db = {'pan30' : 'bcra_mff_001' ,
                'pan31' : 'bcra_mff_002' ,
                'pan32' : 'bcra_mff_003' ,
                'pan33' : 'bcra_mff_004' ,
                'pan34' : 'bcra_mff_005' ,
                'pan45' : 'bcra_mff_006' ,
                'pan46' : 'bcra_mff_007' ,
                'pan37' : 'bcra_mff_008' ,
                'pan38' : 'bcra_mff_009' ,
                'pan39' : 'bcra_mff_010' ,
                'pan41' : 'bcra_mff_011' ,
                'pan40' : 'bcra_mff_012' ,
                'pan1' : 'bcra_mff_013' ,
                'pan2' : 'bcra_mff_014' ,
                'pan5' : 'bcra_mff_015' ,
                'pan11' : 'bcra_mff_016' ,
                'pan12' : 'bcra_mff_017' ,
                'pan13' : 'bcra_mff_018' ,
                'pan14' : 'bcra_mff_019' ,
                'pan6' : 'bcra_mff_020' ,
                'pan7' : 'bcra_mff_021' ,
                'pan8' : 'bcra_mff_022' ,
                'pan9' : 'bcra_mff_023' ,
                'pan10' : 'bcra_mff_024'

}
###DB CONNECTION
try:
    conn = psycopg2.connect(
        host = db_connect.UniProd.host,
        database = db_connect.UniProd.database,
        user = db_connect.UniProd.user,
        password = db_connect.UniProd.password

    )

    cur = conn.cursor()

    auto_bcra_mon_fin_framework_runs.write('Connected to db\n')
    print('Connected to db')
except:
    auto_bcra_mon_fin_framework_runs.write('Error in connection\n')
    print('Error in connection')



auto_bcra_mon_fin_framework_runs.write('Cursor created\nInserting Data...\n')
print('Cursor created\nInserting Data...')

series_count = 0

for i in range(df_bcra.shape[0]):

    series_date_py = df_bcra['Date'].iloc[i]

    for source_id_py, db_id in list(source_vs_db.items()):

        sql_statement = '''INSERT INTO series_values(series_id, series_date, series_values)
        VALUES (%s, %s, %s)
        ON CONFLICT(series_id, series_date) DO UPDATE SET series_values = %s
        '''
        sql_values = (db_id, series_date_py, df_bcra[source_id_py].iloc[i], df_bcra[source_id_py].iloc[i])

        cur.execute(sql_statement, sql_values)
        conn.commit()


    series_count = series_count+1

print(f'series updated: {series_count}')

#close the cursor
cur.close()
#close the connection
conn.close()

auto_bcra_mon_fin_framework_runs.write('Disconnected from db\n')
print('Disconnected from db')