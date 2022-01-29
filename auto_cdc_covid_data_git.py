import requests
import json
import psycopg2
import db_connect

re_js = requests.get('https://data.cdc.gov/api/views/9mfq-cb36/rows.json?accessType=DOWNLOAD').text
js = json.loads(re_js)

#submission_date -- Date of counts
#state -- Jurisdiction
#tot_cases -- Total number of cases
#conf_cases -- Total confirmed cases
#prob_cases -- Total probable cases
#new_case -- Number of new cases
#pnew_case -- Number of new probable cases
#tot_death -- Total number of deaths
#conf_death -- Total number of confirmed deaths
#prob_death -- Total number of probable deaths
#new_death -- Number of new deaths
#pnew_death -- Number of new probable deaths
#created_at -- Date and time record was created
#consent_cases -- If Agree, then confirmed and probable cases are included. If Not Agree, then only total cases are included.
#consent_deaths -- f Agree, then confirmed and probable deaths are included. If Not Agree, then only total deaths are included.

#js['data'][8] -> series_date
#js['data'][9] -> series_state_id

#js['data'][13] -> new_case
#js['data'][18] -> new_death

js_data = js['data']

try:
    conn = psycopg2.connect(
        host=db_connect.UniProd.host,
        database=db_connect.UniProd.database,
        user=db_connect.UniProd.user,
        password=db_connect.UniProd.password
    )

    cur = conn.cursor()

    print('Connected to the database')
except:
    print('Problems with connection to  the database')

variables = {'new_case': 'Number of new cases',
             'new_death': 'Number of new deaths'
            }

for i in range(len(js_data)):

    if js_data[i][9] not in ['FSM', 'PW', 'RMI']:  ##EXCEPTIONS FROM THE SOURCE

        for idx, desc in list(variables.items()):

            try:
                series_id = 'cdc_covid_' + str(js_data[i][9].lower()) + '_' + str(idx)
                series_name = str(desc)
                series_state_id = str(js_data[i][9].lower()) + '_us'


            except:
                print(f'Problem in defining variables, source: {i}')
                pass

            try:
                sql_statement = '''INSERT INTO SERIES 
                                (series_id, series_name, dataset_id, series_frequency_id, series_unit_id, series_state_id )
                                VALUES(%s, %s, 'cdc_covid_data', 'daily', 'persons', %s)
                                ON CONFLICT DO NOTHING
                '''
                sql_values = (series_id, series_name, series_state_id)

                cur.execute(sql_statement, sql_values)
                conn.commit()


            except:
                print(f'INSERT INTO SERIES FAILED, source: {i}')
                pass

            try:

                sql_statement_sv = '''INSERT INTO SERIES_VALUES 
                                (series_id, series_date, series_values)
                                VALUES(%s,  TO_DATE(%s, 'YYYY-MM-DDTHH24:MI:SS:SSSS'),  %s)
                                ON CONFLICT(series_id, series_date) DO UPDATE SET series_values = %s
                '''

                if 'new_case' in series_id:

                    sql_values_sv = (series_id, js_data[i][8], js_data[i][13], js_data[i][13])

                    cur.execute(sql_statement_sv, sql_values_sv)
                    conn.commit()

                elif 'new_death' in series_id:

                    sql_values_sv = (series_id, js_data[i][8], js_data[i][18], js_data[i][18])

                    cur.execute(sql_statement_sv, sql_values_sv)
                    conn.commit()


            except:
                print(f'Error inserting into database, source: {i}')
                pass

print('Execution finished')

cur.close()
conn.close()