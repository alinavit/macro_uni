import json
import requests

states_dict = {
    'AL': 'al_us', 'AK': 'ak_us', 'AZ': 'az_us', 'AR': 'ar_us', 'CA': 'ca_us', 'CO': 'co_us', 'CT': 'ct_us',
    'DE': 'de_us', 'DC': 'dc_us',
    'FL': 'fl_us', 'GA': 'ga_us', 'HI': 'hi_us', 'ID': 'id_us', 'IL': 'il_us', 'IN': 'in_us', 'IA': 'ia_us',
    'KS': 'ks_us', 'KY': 'ky_us', 'LA': 'la_us', 'ME': 'me_us',
    'MD': 'md_us', 'MA': 'ma_us', 'MI': 'mi_us', 'MN': 'mn_us', 'MS': 'ms_us', 'MO': 'mo_us', 'MT': 'mt_us',
    'NE': 'ne_us', 'NV': 'nv_us', 'NH': 'nh_us', 'NJ': 'nj_us',
    'NM': 'nm_us', 'NY': 'ny_us', 'NC': 'nc_us', 'ND': 'nd_us', 'OH': 'oh_us', 'OK': 'ok_us', 'OR': 'or_us',
    'PA': 'pa_us', 'RI': 'ri_us', 'SC': 'sc_us', 'SD': 'sd_us',
    'TN': 'tn_us', 'TX': 'tx_us', 'UT': 'ut_us', 'VT': 'vt_us', 'VA': 'va_us', 'WA': 'wa_us', 'WV': 'wv_us',
    'WI': 'wi_us', 'WY': 'wy_us', 'US': 'us_us'
}

import psycopg2
import db_connect

conn = psycopg2.connect(
    host = db_connect.UniProd.host,
    database = db_connect.UniProd.database,
    user = db_connect.UniProd.user,
    password = db_connect.UniProd.password

)

cur = conn.cursor()

url_static = 'https://api.eia.gov/series/?api_key=5yd7cxa6ftTNdZuFjDGtYDh4actG0uiiIVKxjLgC&series_id=EMISS.CO2-TOTV-TT-TO-'

for state_source, state_db in list(states_dict.items()):

    ###series

    series_request = requests.get(url_static + state_source + '.A', 'json').text
    json_file = json.loads(series_request)

    series_id = 'eia_emm_co2_' + state_db

    ###SERIES CREATION DISABLED, ALL SERIES ARE CREATED###

    ##series_name = json_file['series'][0]['name']

    ##sql_statement = '''insert into
    ##    series(series_id, series_name, dataset_id, series_frequency_id, series_unit_id, series_state_id)
    ##    values(%s,%s,%s,%s,%s,%s)
    ##ON CONFLICT DO NOTHING
    ##'''
    ##sql_values = (series_id, series_name, 'eia_co2_emission', 'annual', 'mmt_co2', str(state_db))

    ##cur.execute(sql_statement, sql_values)
    ##conn.commit()

    ##print(cur.rowcount, f" series inserted : {series_id}")

    # series_values
    for i in range(len(json_file['series'][0]['data'])):
        sql_statement_values = '''insert into series_values (series_id, series_date, series_values)
                            values (%s,TO_DATE(%s,'YYYY'),%s)
                            ON CONFLICT (series_id,series_date) DO UPDATE SET series_values = %s
        '''

        sql_values_values = (series_id, json_file['series'][0]['data'][i][0], json_file['series'][0]['data'][i][1],
                             json_file['series'][0]['data'][i][1])

        cur.execute(sql_statement_values, sql_values_values)
        conn.commit()

        print(cur.rowcount,
              f"series values inserted : {series_id} year: {json_file['series'][0]['data'][i][0]}, value: {json_file['series'][0]['data'][i][1]}")

# close the cursor
cur.close()
# close the connection
conn.close()
