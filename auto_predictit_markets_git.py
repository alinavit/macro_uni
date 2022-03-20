import requests
import json
import psycopg2 #POSTGRES
from datetime import datetime
import db_connect

print('Date:', datetime.now())

try:
    conn = psycopg2.connect(
        host = db_connect.UniProd.host,
        database = db_connect.UniProd.database,
        user = db_connect.UniProd.user,
        password = db_connect.UniProd.password

    )
    print('Connected to db')
except:
    print('Error in connection')



cur = conn.cursor()
print('Cursor created')


url = 'https://www.predictit.org/api/marketdata/all/'
data_json = requests.get(url, 'json').text

full_json_file = json.loads(data_json)
print('Files extracted')

json_markets = full_json_file['markets'] #markets are basically questions that should be voted by categrory

## DB insert series
new_series = []
for i in range(len(json_markets)):
    for k in range(len(json_markets[i]['contracts'])):  # contracts are what someone can vote for [DETAILED INFORMATION]

        series_id = 'predictit_' + str(json_markets[i]['id']) + '_' + str(json_markets[i]['contracts'][k]['id'])

        series_name = str(json_markets[i]['name']) + ', ' + str(json_markets[i]['contracts'][k]['name'])

        sql_statement = '''
        insert into 
            series(series_id, series_name, dataset_id, series_frequency_id, series_unit_id, series_state_id) 
            values(%s,%s,%s,%s,%s,%s)
        ON CONFLICT (series_id) DO NOTHING;
        '''

        sql_values = (series_id, series_name, 'predictit_markets', 'daily', 'percent', 'total_us')

        cur.execute(sql_statement, sql_values)
        conn.commit()

        #print(cur.rowcount, f"record inserted : {series_id}")

        if cur.rowcount == 1:
            new_series.append(series_id)

print(f'{len(new_series)} new series have been created: {new_series}')

cur.execute('''select count(distinct series_id) from series_values where series_id like 'predictit%'  ''')
total_series = cur.fetchall()

## DB insert series_values
series_updated = []
for i in range(len(json_markets)):
    for k in range(len(json_markets[i]['contracts'])):
        series_id = 'predictit_' + str(json_markets[i]['id']) + '_' + str(json_markets[i]['contracts'][k]['id'])

        sql_statement_values = '''
            insert into 
                series_values (series_id, series_date, series_values) 
                values(%s,%s,%s)
            ON CONFLICT(series_id, series_date) DO UPDATE SET series_values = %s;
            '''

        sql_values_values = (series_id,
                             json_markets[i]['timeStamp'],
                             json_markets[i]['contracts'][k]['lastClosePrice'],
                             json_markets[i]['contracts'][k]['lastClosePrice'])

        cur.execute(sql_statement_values, sql_values_values)
        conn.commit()

        #print(cur.rowcount, f"inserted: {series_id} for : {json_markets[i]['timeStamp']}")

        if cur.rowcount == 1:
            series_updated.append(series_id)

print(f'{len(series_updated)} out of {total_series[0][0]}  series have been updated')

#close the cursor
cur.close()
#close the connection
conn.close()

print('Disconnected from db')