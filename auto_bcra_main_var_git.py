from selenium import webdriver
import requests
import lxml
from bs4 import BeautifulSoup
from datetime import datetime


import db_connect
import psycopg2 as psql
auto_bcra_main_var_runs = open('C:\\Users\\48575\\Documents\\GitHub\\macro_uni\\auto_bcra_main_var_runs.txt', 'a')

###CURRENT DATE
run_time = f'Run on: {datetime.now().strftime("%d %b %Y: %H:%M:%S")} CET\n'
auto_bcra_main_var_runs.write(f'{run_time}Data Extraction:\n')
print(f'{run_time}Data Extraction:\n')

url = 'http://www.bcra.gob.ar/PublicacionesEstadisticas/Principales_variables_i.asp'
html_main_page = requests.get(url).text

soup_main_page = BeautifulSoup(html_main_page, 'lxml')

table_links = soup_main_page.tbody
whole_table_in_tr = table_links.find_all('tr')

table_links = soup_main_page.tbody
whole_table_in_tr = table_links.find_all('tr')
#table_links = soup_main_page.tbody
#whole_table_in_tr = table_links.find_all('tr')


###Links to variables and max dates
links_to_variables = []
max_dates = []
for idx, tr_row in enumerate(whole_table_in_tr):

    try:
        row_link = tr_row.find('td').a['href']
        links_to_variables.append('http://www.bcra.gob.ar/' + row_link)

        for idx2, tr_cell in enumerate(tr_row.find_all('td')):
            if idx2 == 1:
                max_dates.append(tr_cell.text)
        auto_bcra_main_var_runs.write('Max dates extracted\n')
        print('Max dates extracted\n')
    except:
        ###SKIPPING THE ROWS IN THE TABLE WHERE NO DATA. THEY ARE USED ONLY AS A VISUALISATION PART
        auto_bcra_main_var_runs.write('Max dates have not been extracted\n')
        print('Max dates have not been extracted\n')
        pass

link_series_date = {
    'http://www.bcra.gob.ar//PublicacionesEstadisticas/Principales_variables_datos_i.asp?serie=246&detalle=BCRA International Reserves (in million dollars - provisional figures subject to valuation change)': 'bcra_reserves_246',
    'http://www.bcra.gob.ar//PublicacionesEstadisticas/Principales_variables_datos_i.asp?serie=7927&detalle=RRetail Foreign Exchange Rate (ARS/USD) Communication B 9791 - Selling Rate Average': 'bcra_rr_7927',
    'http://www.bcra.gob.ar//PublicacionesEstadisticas/Principales_variables_datos_i.asp?serie=272&detalle=Wholesale Foreign Exchange Rate (ARS/USD) Communication A 3500 - Benchmark Rate': 'bcra_wer_272',
    'http://www.bcra.gob.ar//PublicacionesEstadisticas/Principales_variables_datos_i.asp?serie=7935&detalle=Monetary Policy Rate (APR %)': 'mon_pol_rate_apr_7935',
    'http://www.bcra.gob.ar//PublicacionesEstadisticas/Principales_variables_datos_i.asp?serie=7936&detalle=Monetary Policy Rate (EAR %)': 'mon_pol_rate_ear_7936',
    'http://www.bcra.gob.ar//PublicacionesEstadisticas/Principales_variables_datos_i.asp?serie=7934&detalle=Early-payment fixed interest rate for UVA early-payment deposits (APR %)': 'bcra_main_7934',
    'http://www.bcra.gob.ar//PublicacionesEstadisticas/Principales_variables_datos_i.asp?serie=7938&detalle=Minimum Interest Rate for Natural Persons\x92 Time Deposits up to $1 Million (APR %)': 'bcra_main_7938',
    'http://www.bcra.gob.ar//PublicacionesEstadisticas/Principales_variables_datos_i.asp?serie=7939&detalle=Minimum Interest Rate for Natural Persons\x92 Time Deposits up to $1 Million (EAR % for 30-day deposits)': 'bcra_main_7939',
    'http://www.bcra.gob.ar//PublicacionesEstadisticas/Principales_variables_datos_i.asp?serie=1222&detalle=BADLAR Rate in Pesos at Private Banks (APR %)': 'bcra_main_1222',
    'http://www.bcra.gob.ar//PublicacionesEstadisticas/Principales_variables_datos_i.asp?serie=7937&detalle=BADLAR Rate in Pesos at Private Banks (EAR %)': 'bcra_main_7937',
    'http://www.bcra.gob.ar//PublicacionesEstadisticas/Principales_variables_datos_i.asp?serie=7922&detalle=Interest Rate on Deposits above 20 Million Pesos (TM20) at Private Banks (APR %)': 'bcra_main_7922',
    'http://www.bcra.gob.ar//PublicacionesEstadisticas/Principales_variables_datos_i.asp?serie=7920&detalle=Interest Rates on Overnight Repo Transactions for the BCRA (APR %)': 'bcra_main_7920',
    'http://www.bcra.gob.ar//PublicacionesEstadisticas/Principales_variables_datos_i.asp?serie=7921&detalle=Interest Rates on Overnight Reverse Repo Transactions for the BCRA (APR %)': 'bcra_main_7921',
    'http://www.bcra.gob.ar//PublicacionesEstadisticas/Principales_variables_datos_i.asp?serie=7990&detalle=Interest Rates on Overnight Reverse Repo Transactions for the BCRA (EAR %)': 'bcra_main_7990',

    #'http://www.bcra.gob.ar//PublicacionesEstadisticas/Principales_variables_datos_i.asp?serie=7989&detalle=Interest Rates on 7-Day Reverse Repo Transactions for the BCRA (APR %)': 'bcra_main_7989',
    #'http://www.bcra.gob.ar//PublicacionesEstadisticas/Principales_variables_datos_i.asp?serie=7991&detalle=Interest Rates on 7-Day Reverse Repo Transactions for the BCRA (EAR %)': 'bcra_main_7991',
    'http://www.bcra.gob.ar//PublicacionesEstadisticas/Principales_variables_datos_i.asp?serie=3139&detalle=Interest Rates on Loans between Private Financial Institutions (BAIBAR) (APR %)': 'bcra_main_3139',
    'http://www.bcra.gob.ar//PublicacionesEstadisticas/Principales_variables_datos_i.asp?serie=1212&detalle=Interest Rates on 30-Day Deposits with Financial Institutions (APR %)': 'bcra_main_1212',
    'http://www.bcra.gob.ar//PublicacionesEstadisticas/Principales_variables_datos_i.asp?serie=7924&detalle=Interest Rates on Loans for Current Account Overdrafts': 'bcra_main_7924',
    'http://www.bcra.gob.ar//PublicacionesEstadisticas/Principales_variables_datos_i.asp?serie=7925&detalle=Interest Rates on Personal Loans': 'bcra_main_7925',
    'http://www.bcra.gob.ar//PublicacionesEstadisticas/Principales_variables_datos_i.asp?serie=250&detalle=Monetary Base - Total (in million pesos)': 'bcra_main_250',
    'http://www.bcra.gob.ar//PublicacionesEstadisticas/Principales_variables_datos_i.asp?serie=251&detalle=Money Circulation (in million pesos)': 'bcra_main_251',
    'http://www.bcra.gob.ar//PublicacionesEstadisticas/Principales_variables_datos_i.asp?serie=251&serie1=296&detalle=Banknotes and Coins Held by the Public (in million pesos)': 'bcra_main_251_296',
    'http://www.bcra.gob.ar//PublicacionesEstadisticas/Principales_variables_datos_i.asp?serie=296&detalle=Cash at Financial Institutions (in million pesos)': 'bcra_main_296',
    'http://www.bcra.gob.ar//PublicacionesEstadisticas/Principales_variables_datos_i.asp?serie=252&detalle=Banks Deposits in Current Accounts in Pesos with the BCRA (in million pesos)': 'bcra_main_252',
    'http://www.bcra.gob.ar//PublicacionesEstadisticas/Principales_variables_datos_i.asp?serie=7926&detalle=LELIQ Balance (in billion ARS)': 'bcra_main_7926',
    'http://www.bcra.gob.ar//PublicacionesEstadisticas/Principales_variables_datos_i.asp?serie=444&serie1=459&serie2=3540&detalle=Cash Deposits with Financial Institutions - Total (in million pesos)': 'bcra_main_444_459',
    'http://www.bcra.gob.ar//PublicacionesEstadisticas/Principales_variables_datos_i.asp?serie=446&serie1=477&detalle=In Current Accounts (net of the use of Official Accounts Unified Fund (FUCO)) (in million pesos)': 'bcra_main_446_477',
    'http://www.bcra.gob.ar//PublicacionesEstadisticas/Principales_variables_datos_i.asp?serie=450&detalle=In Savings Accounts (in million pesos)': 'bcra_main_450',
    'http://www.bcra.gob.ar//PublicacionesEstadisticas/Principales_variables_datos_i.asp?serie=452&serie1=459&serie2=463&serie3=464&serie4=465&detalle=Term (including investments and excluding Certificates of Rescheduled Bank Deposit (CEDROS)) (in million pesos)': 'bcra_main_452_459',
    'http://www.bcra.gob.ar//PublicacionesEstadisticas/Principales_variables_datos_i.asp?serie=7919&detalle=Private M2, 30-Day Rolling Average, Year-on-Year Change (%)': 'bcra_main_7919',
    'http://www.bcra.gob.ar//PublicacionesEstadisticas/Principales_variables_datos_i.asp?serie=392&detalle=Financial Institutions\x92 Loans to the Private Sector (in million pesos)': 'bcra_main_392',
    'http://www.bcra.gob.ar//PublicacionesEstadisticas/Principales_variables_datos_i.asp?serie=7931&detalle=Monthly Inflation\xa0(% change)': 'bcra_main_7931',
    'http://www.bcra.gob.ar//PublicacionesEstadisticas/Principales_variables_datos_i.asp?serie=7932&detalle=Year-on-Year Inflation (y.o.y. % change)': 'bcra_main_7932',
    'http://www.bcra.gob.ar//PublicacionesEstadisticas/Principales_variables_datos_i.asp?serie=7933&detalle=Expected Inflation - REM 12 Months - MEDIAN (y.o.y. % change)': 'bcra_main_7933',
    ###SPANISH VERSION IS USED
    'http://www.bcra.gob.ar/PublicacionesEstadisticas/Principales_variables_datos.asp?serie=7987&detalle=Inflaci%F3n%20esperada%20-%20REM%20pr%F3ximos%2012%20meses%A0-%20Promedio%20de%20mejores%2010%20pronosticadores%20(variaci%F3n%20en%20%%20i.a)': 'bcra_main_7987',
    'http://www.bcra.gob.ar//PublicacionesEstadisticas/Principales_variables_datos_i.asp?serie=3540&detalle=Reference Stabilization Coefficient (CER) (2.2.2002=1 Basis)': 'bcra_main_3540',
    'http://www.bcra.gob.ar//PublicacionesEstadisticas/Principales_variables_datos_i.asp?serie=7913&detalle=Unit of Purchasing Power (UVA) (in pesos -with two decimals- 3.31.2016=14.05 basis)': 'bcra_main_7913',
    'http://www.bcra.gob.ar//PublicacionesEstadisticas/Principales_variables_datos_i.asp?serie=7914&detalle=Housing Unit (UVI) (in pesos -with two decimals-, 3.31.2016=14.05 basis)': 'bcra_main_7914',
    'http://www.bcra.gob.ar//PublicacionesEstadisticas/Principales_variables_datos_i.asp?serie=7988&detalle=Rent Index (ICL-Law No. 27.551, with two decimal places, basis 06.30.20=1)': 'bcra_main_7988'
}



###DB CONNECTION
try:
    conn = psql.connect(
        host = db_connect.UniProd.host,
        database = db_connect.UniProd.database,
        user = db_connect.UniProd.user,
        password = db_connect.UniProd.password

    )

    cur = conn.cursor()

    auto_bcra_main_var_runs.write('Connected to db\n')
    print('Connected to db')
except:
    auto_bcra_main_var_runs.write('Error in connection\n')
    print('Error in connection')

for link, series_id in list(link_series_date.items()):
    try:
        html_data = requests.get(link).text
        soup_direct_link = BeautifulSoup(html_data, 'lxml')

        # will need for scraping
        index = int(list(link_series_date.keys()).index(link))
        max_start_date = max_dates[index]

        to_date = datetime.strptime(max_start_date, '%m/%d/%Y')
        max_date = to_date.strftime('%m/%d/%Y')

    except:
        print(f'error in max_value, dictionary index {index}')
        auto_bcra_main_var_runs.write(f'error in max_value, dictionary index {index}')
        pass

    try:

        driver = webdriver.Chrome('C:\\Users\\48575\\Documents\\GitHub\\macro_uni\\CHROMEDRIVER FOLDER\\chromedriver.exe')

        driver.get(link)

        start_date = driver.find_element_by_name('fecha_desde')
        end_date = driver.find_element_by_name('fecha_hasta')
        start_date.send_keys('09/10/2021')
        end_date.send_keys(max_date)

        click_see = driver.find_element_by_css_selector(
            'body > div > div.contenido > div > div > div > div > form > button')
        click_see.click()

        #################EXCEPTION OF SPANISH VERSION LINK##################
        if series_id in ['bcra_main_7987']:
            table_string = driver.find_elements_by_css_selector(
                'body > div > div.contenido > div > div.col-xs-12.col-md-6 > div > div > table')

        else:
            table_string = driver.find_elements_by_css_selector(
                'body > div > div.contenido > div > div > div > div > table')
        ###################################################################

        data_string = table_string[0].text
        data = data_string.split('\n')
    except:
        print(f'error running driver {series_id}')
        auto_bcra_main_var_runs.write(f'error running driver {series_id}')
        pass

    try:

        ##DATA
        for i in range(len(data)):
            if i > 0:

                value = data[i].split(' ')[1]

                ############VALUE EXCEPTION: thousand separator '.'######################
                if series_id in ['bcra_main_251', 'bcra_main_251_296', 'bcra_main_444_459', 'bcra_main_446_477',
                                 'bcra_main_452_459']:
                    value = value.replace('.', '')

                    date = data[i].split(' ')[0]
                    to_date_py = datetime.strptime(date, '%m/%d/%Y')

                    #################EXCEPTION OF SPANISH VERSION LINK##################
                elif series_id in ['bcra_main_7987']:
                    value = value.replace(',', '.')

                    date = data[i].split(' ')[0]
                    to_date_py = datetime.strptime(date, '%d/%m/%Y')

                ###########REST OF VALUES#####################
                else:
                    value = value.replace(',', '')

                    date = data[i].split(' ')[0]
                    to_date_py = datetime.strptime(date, '%m/%d/%Y')

                sql_statement = '''
                        insert into 
                            series_values (series_id, series_date, series_values) 
                            values(%s,%s,%s)
                        ON CONFLICT(series_id, series_date) DO UPDATE SET series_values = %s;
                        '''

                sql_values = (series_id, to_date_py, value, value)

                cur.execute(sql_statement, sql_values)
                conn.commit()

        print(f'work on {series_id} done')
        auto_bcra_main_var_runs.write(f'work on {series_id} done\n')

    except:
        print(f'error related to sql {series_id}')
        auto_bcra_main_var_runs.write(f'error related to sql {series_id}')
        pass



#close the cursor
cur.close()
#close the connection
conn.close()

auto_bcra_main_var_runs.write('Disconnected from db\n')
print('Disconnected from db')
