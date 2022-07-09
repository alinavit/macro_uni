## This pipeline has to create new series when they appear in the source,
## update existing series
## write the run info into log file
## discontinue series that have last series_date < 3 months from the run

import requests
import json
import psycopg2 #POSTGRES
from datetime import datetime
import db_connect

auto_predictit_markets_git_runs = open('C:\\Users\\48575\\Documents\\GitHub\\macro_uni\\auto_predictit_markets_git_runs.txt', 'a')

###CURRENT DATE

run_time = f'Run on: {datetime.now().strftime("%d %b %Y: %H:%M:%S")} CET\n'
auto_predictit_markets_git_runs.write(run_time)
print(run_time)

###DB CONNECTION
try:
    conn = psycopg2.connect(
        host = db_connect.UniProd.host,
        database = db_connect.UniProd.database,
        user = db_connect.UniProd.user,
        password = db_connect.UniProd.password

    )

    cur = conn.cursor()

    auto_predictit_markets_git_runs.write('Connected to db\n')
    print('Connected to db')
except:
    auto_predictit_markets_git_runs.write('Error in connection\n')
    print('Error in connection')



auto_predictit_markets_git_runs.write('Cursor created\nExtracting Data...\n')
print('Cursor created\nExtracting Data...')


###DATA EXTRACTION
try:
    url = 'https://www.predictit.org/api/marketdata/all/'
    data_json = requests.get(url, 'json').text

    full_json_file = json.loads(data_json)

    json_markets = full_json_file['markets'] #markets are basically questions that should be voted by categrory

    auto_predictit_markets_git_runs.write('Files extracted\n')
    print('Files extracted')
except:
    auto_predictit_markets_git_runs.write('Error extracting files\n')
    print('Error extracting files')


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

auto_predictit_markets_git_runs.write(f'{len(new_series)} new series have been created: {new_series}\n')
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

auto_predictit_markets_git_runs.write(f'{len(series_updated)} out of {total_series[0][0]}  series have been updated\n')
print(f'{len(series_updated)} out of {total_series[0][0]}  series have been updated')

#close the cursor
cur.close()
#close the connection
conn.close()

auto_predictit_markets_git_runs.write('Disconnected from db\n')
print('Disconnected from db')