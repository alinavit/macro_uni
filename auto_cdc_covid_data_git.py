import requests
import json
import psycopg2

re_js = requests.get('https://data.cdc.gov/resource/9mfq-cb36.json').text
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

try:
    conn = psycopg2.connect(
        host='localhost',
        database='m_uni',
        user='postgres',
        password='111'
    )

    cur = conn.cursor()

    print('Connected to the database')
except:
    print('Problems with connection to  the database')

variables = {'tot_cases': 'Total number of cases' ,
             'new_case': 'Number of new cases',
             'tot_death': 'Total number of deaths',
             'new_death': 'Number of new deaths '
            }

for i in range(len(js)):
    for idx, desc in list(variables.items()):

        try:
            series_id = 'cdc_covid_' + str(js[i]['state'].lower()) + '_' + str(idx)
            series_name = str(desc)
            series_state_id = str(js[i]['state'].lower()) + '_us'


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

            sql_values_sv = (series_id, js[i]['submission_date'], js[i][idx], js[i][idx])

            cur.execute(sql_statement_sv, sql_values_sv)
            conn.commit()


        except:
            print(f'Foreign Key violation {i}')
            pass

print('Execution finished')

cur.close()
conn.close()