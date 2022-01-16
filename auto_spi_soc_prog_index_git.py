import pandas as pd
import openpyxl
import numpy as np
import psycopg2
import db_connect

####The aim of this file is to load fast all data available, skipping the series where is no data.
####It is very likely that in the future when revising the data, revising of the code below is also needed,
####because the link to the file was gotten after request, and also it has some modifications from year to year

##the URL was requested to email. This is the link that was sent by SPI
url = 'https://u1334509.ct.sendgrid.net/ls/click?upn=SmgS8jV9fcaG0CzFUh6hFHjXz6XIyh5-2BCEAzsdJxftMpKKP-2BDMVwFhugq-2BSG0yGL7StnSTtAnOS8a-2BbjB7KMnZiM-2F1-2FRn763zow97CImTdT5wTGNIaBjcVyK0BI0K4Au6eBuN-2B40rHVDSClYT6mC0Q-3D-3DHv3W_xc-2Bm0IZfMESS3QBsKVvzjl0d7y3veUayN4auGlzNLGNAZuDpoP9Y-2Fttb3zyB4DrbV7YngkNL6CtQF3q3aIayRTwFSe-2Fu-2Bzef-2B2R6i8VnTuz3Gnf6qQfRsWPB1WpZmEaM8EKAtCw6iZ5kQsXMYAT4Z-2B3yv4K-2BOBmOr0qEticrCqLXPBUx6KWamdE9d-2F-2BFg5cX3YOBaEXoCim2bDMrfyVAoQEBllfFgj-2FhUxU7o9Pwl2ljY8fDKzbJXjYfBIfHvWnP25wDs5Cpni0qUrQ5RRDJYO6h9uf3-2FhtLT0IsJcZy9-2Fxmq3FkGdxKQFx4QUv22YYC'

df_spi = pd.read_excel(url, engine = 'openpyxl', sheet_name = '2011-2021 data', header = [0] )

df_spi.drop(columns = ['SPI country code', 'Status', 'SPI Rank', 'Unnamed: 9', 'Unnamed: 22'], axis = 1, inplace = True)

########## Dictionary : source country name and DB country name ################
source_vs_db_state = {'World': 'total_wd',
 'Albania': 'total_al',
 'Algeria': 'total_dz',
 'Angola': 'total_ao',
 'Argentina': 'total_ar',
 'Armenia': 'total_am',
 'Australia': 'total_au',
 'Austria': 'total_at',
 'Azerbaijan': 'total_az',
 'Bahrain': 'total_bh',
 'Bangladesh': 'total_bd',
 'Barbados': 'total_bb',
 'Belarus': 'total_by',
 'Belgium': 'total_be',
 'Benin': 'total_bj',
 'Bhutan': 'total_bt',
 'Bolivia': 'total_bo',
 'Bosnia and Herzegovina': 'total_ba',
 'Botswana': 'total_bw',
 'Brazil': 'total_br',
 'Bulgaria': 'total_bg',
 'Burkina Faso': 'total_bf',
 'Burundi': 'total_bi',
 'Cabo Verde': 'total_cv',
 'Cambodia': 'total_kh',
 'Cameroon': 'total_cm',
 'Canada': 'total_ca',
 'Central African Republic': 'total_cf',
 'Colombia': 'total_co',
 'Comoros': 'total_km',
 'Congo, Democratic Republic of': 'total_cd',
 'Congo, Republic of': 'total_cg',
 'Costa Rica': 'total_cr',
 "Côte d'Ivoire": 'total_ci',
 'Croatia': 'total_hr',
 'Cuba': 'total_cu',
 'Cyprus': 'total_cy',
 'Czechia': 'total_cz',
 'Denmark': 'total_dk',
 'Djibouti': 'total_dj',
 'Dominican Republic': 'total_do',
 'Ecuador': 'total_ec',
 'Egypt': 'total_eg',
 'El Salvador': 'total_sv',
 'Equatorial Guinea': 'total_gq',
 'Eritrea': 'total_er',
 'Estonia': 'total_ee',
 'Eswatini': 'total_sz',
 'Ethiopia': 'total_et',
 'Fiji': 'total_fj',
 'Finland': 'total_fi',
 'France': 'total_fr',
 'Gabon': 'total_ga',
 'Gambia, The': 'total_gm',
 'Georgia': 'total_ge',
 'Germany': 'total_de',
 'Ghana': 'total_gh',
 'Greece': 'total_gr',
 'Guatemala': 'total_gt',
 'Guinea': 'total_gn',
 'Guinea-Bissau': 'total_gw',
 'Guyana': 'total_gy',
 'Haiti': 'total_ht',
 'Honduras': 'total_hn',
 'Hungary': 'total_hu',
 'Chad': 'total_td',
 'Chile': 'total_cl',
 'China': 'total_cn',
 'Iceland': 'total_ic',
 'India': 'total_in',
 'Indonesia': 'total_id',
 'Iran': 'total_ir',
 'Iraq': 'total_iq',
 'Ireland': 'total_ie',
 'Israel': 'total_il',
 'Italy': 'total_it',
 'Jamaica': 'total_jm',
 'Japan': 'total_jp',
 'Jordan': 'total_jo',
 'Kazakhstan': 'total_kz',
 'Kenya': 'total_ke',
 'Korea, Republic of': 'total_kr',
 'Kuwait': 'total_kw',
 'Kyrgyzstan': 'total_kg',
 'Laos': 'total_la',
 'Latvia': 'total_lv',
 'Lebanon': 'total_lb',
 'Lesotho': 'total_ls',
 'Liberia': 'total_lr',
 'Libya': 'total_ly',
 'Lithuania': 'total_lt',
 'Luxembourg': 'total_lu',
 'Madagascar': 'total_mg',
 'Malawi': 'total_mw',
 'Malaysia': 'total_my',
 'Maldives': 'total_mv',
 'Mali': 'total_ml',
 'Malta': 'total_mt',
 'Mauritania': 'total_mr',
 'Mauritius': 'total_mu',
 'Mexico': 'total_mx',
 'Moldova': 'total_md',
 'Mongolia': 'total_mn',
 'Montenegro': 'total_me',
 'Morocco': 'total_ma',
 'Mozambique': 'total_mz',
 'Myanmar': 'total_mm',
 'Namibia': 'total_na',
 'Nepal': 'total_np',
 'Netherlands': 'total_nl',
 'New Zealand': 'total_nz',
 'Nicaragua': 'total_ni',
 'Niger': 'total_ne',
 'Nigeria': 'total_ng',
 'Norway': 'total_no',
 'Oman': 'total_om',
 'Pakistan': 'total_pk',
 'Panama': 'total_pa',
 'Papua New Guinea': 'total_pg',
 'Paraguay': 'total_py',
 'Peru': 'total_pe',
 'Philippines': 'total_ph',
 'Poland': 'total_pl',
 'Portugal': 'total_pt',
 'Qatar': 'total_qa',
 'Republic of North Macedonia': 'total_mk',
 'Romania': 'total_ro',
 'Russia': 'total_ru',
 'Rwanda': 'total_rw',
 'Sao Tome and Principe': 'total_st',
 'Saudi Arabia': 'total_sa',
 'Senegal': 'total_sn',
 'Serbia': 'total_rs',
 'Sierra Leone': 'total_sl',
 'Singapore': 'total_sg',
 'Slovakia': 'total_sk',
 'Slovenia': 'total_si',
 'Solomon Islands': 'total_sb',
 'Somalia': 'total_so',
 'South Africa': 'total_za',
 'South Sudan': 'total_ss',
 'Spain': 'total_es',
 'Sri Lanka': 'total_lk',
 'Sudan': 'total_sd',
 'Suriname': 'total_sr',
 'Sweden': 'total_se',
 'Switzerland': 'total_ch',
 'Syria': 'total_sy',
 'Tajikistan': 'total_tj',
 'Tanzania': 'total_tz',
 'Thailand': 'total_th',
 'Timor-Leste': 'total_tl',
 'Togo': 'total_tg',
 'Trinidad and Tobago': 'total_tt',
 'Tunisia': 'total_tn',
 'Turkey': 'total_tr',
 'Turkmenistan': 'total_tm',
 'Uganda': 'total_ug',
 'Ukraine': 'total_ua',
 'United Arab Emirates': 'total_ae',
 'United Kingdom': 'total_gb',
 'United States': 'total_us',
 'Uruguay': 'total_uy',
 'Uzbekistan': 'total_uz',
 'Vietnam': 'total_vn',
 'West Bank and Gaza': 'total_gz',
 'Yemen': 'total_ye',
 'Zambia': 'total_zm',
 'Zimbabwe': 'total_zw',
 'Brunei Darussalam': 'total_bn',
 'Korea, Democratic Republic of': 'total_nk',
 'Seychelles': 'total_sc',
 'Vanuatu': 'total_vu',
 'American Samoa': 'total_as',
 'Andorra': 'total_ad',
 'Antigua and Barbuda': 'total_ag',
 'Bahamas, The': 'total_bs',
 'Belize': 'total_bz',
 'Bermuda': 'total_bm',
 'Cook Islands': 'total_ck',
 'Dominica': 'total_dm',
 'Greenland': 'total_gl',
 'Grenada': 'total_gd',
 'Guam': 'total_gu',
 'Hong Kong': 'total_hk',
 'Kiribati': 'total_ki',
 'Kosovo': 'total_xk',
 'Marshall Islands': 'total_mh',
 'Micronesia': 'total_fm',
 'Monaco': 'total_mc',
 'Nauru': 'total_nr',
 'Niue': 'total_nu',
 'Northern Mariana Islands': 'total_mp',
 'Palau': 'total_pw',
 'Puerto Rico': 'total_pr',
 'Samoa': 'total_ws',
 'San Marino': 'total_sm',
 'St Kitts and Nevis': 'total_kn',
 'St Lucia': 'total_lc',
 'St Vincent and the Grenadines': 'total_vc',
 'Taiwan': 'total_tw',
 'Tokelau': 'total_tk',
 'Tonga': 'total_to',
 'Tuvalu': 'total_tv',
 'Virgin Islands (US)': 'total_vi'}

df_spi.rename(columns={
    'Basic Human Needs': 'Basic Human Needs, Total',
    'Foundations of Wellbeing': 'Foundations of Wellbeing, Total',
    'Opportunity': 'Opportunity, Total',

    'Nutrition and Basic Medical Care': 'Basic Human Needs, Nutrition and Basic Medical Care, Total',
    'Water and Sanitation': 'Basic Human Needs, Water and Sanitation, Total',
    'Shelter': 'Basic Human Needs, Shelter, Total',
    'Personal Safety': 'Basic Human Needs, Personal Safety, Total',

    'Access to Basic Knowledge': 'Foundations of Wellbeing, Access to Basic Knowledge, Total',
    'Access to Information and Communications': 'Foundations of Wellbeing, Access to Information and Communications, Total',
    'Health and Wellness': 'Foundations of Wellbeing, Health and Wellness, Total',
    'Environmental Quality': 'Foundations of Wellbeing, Environmental Quality, Total',

    'Personal Rights': 'Opportunity, Personal Rights, Total',
    'Personal Freedom and Choice': 'Opportunity, Personal Freedom and Choice, Total',
    'Inclusiveness': 'Opportunity, Inclusiveness, Total',
    'Access to Advanced Education': 'Opportunity, Access to Advanced Education, Total',

    'Deaths from infectious diseases (deaths/100,000)': 'Basic Human Needs, Deaths from infectious diseases (deaths/100,000)',
    'Child mortality rate (deaths/1000 live births)': 'Basic Human Needs, Child mortality rate (deaths/1000 live births)',
    'Child stunting (0=low risk; 100=high risk)': 'Basic Human Needs, Child stunting (0=low risk; 100=high risk)',
    'Maternal mortality rate (deaths/100,000 live births)': 'Basic Human Needs, Maternal mortality rate (deaths/100,000 live births)',
    'Undernourishment (% of population)': 'Basic Human Needs, Undernourishment (% of population)',
    'Access to improved sanitation (proportion of population)': 'Basic Human Needs, Access to improved sanitation (proportion of population)',
    'Access to improved water source (proportion of population)': 'Basic Human Needs, Access to improved water source (proportion of population)',
    'Unsafe water, sanitation and hygiene attributable deaths (deaths/100,000)': 'Basic Human Needs, Unsafe water, sanitation and hygiene attributable deaths (deaths/100,000)',
    'Household air pollution attributable deaths (deaths/100,000)': 'Basic Human Needs, Household air pollution attributable deaths (deaths/100,000)',
    'Dissatisfaction with housing affordability (0=low; 1=high)': 'Basic Human Needs, Dissatisfaction with housing affordability (0=low; 1=high)',
    'Access to electricity (% of population)': 'Basic Human Needs, Access to electricity (% of population)',
    'Usage of clean fuels and technology for cooking (% of population)': 'Basic Human Needs, Usage of clean fuels and technology for cooking (% of population)',
    'Deaths from interpersonal violence (deaths/100,000)': 'Basic Human Needs, Deaths from interpersonal violence (deaths/100,000)',
    'Transportation related fatalities (deaths/100,000)': 'Basic Human Needs, Transportation related fatalities (deaths/100,000)',
    'Perceived criminality (1=low; 5=high)': 'Basic Human Needs, Perceived criminality (1=low; 5=high)',
    'Political killings and torture (0=low freedom; 1=high freedom)': 'Basic Human Needs, Political killings and torture (0=low freedom; 1=high freedom)',

    'Women with no schooling (proportion of females)': 'Foundations of Wellbeing, Women with no schooling (proportion of females)',
    'Equal access to quality education (0=unequal; 4=equal)': 'Foundations of Wellbeing, Equal access to quality education (0=unequal; 4=equal)',
    'Primary school enrollment (% of children)': 'Foundations of Wellbeing, Primary school enrollment (% of children)',
    'Secondary school attainment (% of population aged 25+)': 'Foundations of Wellbeing, Secondary school attainment (% of population aged 25+)',
    'Gender parity in secondary attainment (distance from parity)': 'Foundations of Wellbeing, Gender parity in secondary attainment (distance from parity)',
    'Access to online governance (0=low; 1=high)': 'Foundations of Wellbeing, Access to online governance (0=low; 1=high)',
    'Internet users (% of population)': 'Foundations of Wellbeing, Internet users (% of population)',
    'Media censorship (0=frequent; 4=rare)': 'Foundations of Wellbeing, Media censorship (0=frequent; 4=rare)',
    'Mobile telephone subscriptions (subscriptions/100 people)': 'Foundations of Wellbeing, Mobile telephone subscriptions (subscriptions/100 people)',
    'Life expectancy at 60 (years)': 'Foundations of Wellbeing, Life expectancy at 60 (years)',
    'Premature deaths from non-communicable diseases (deaths/100,000)': 'Foundations of Wellbeing, Premature deaths from non-communicable diseases (deaths/100,000)',
    'Equal access to quality healthcare (0=unequal; 4=equal)': 'Foundations of Wellbeing, Equal access to quality healthcare (0=unequal; 4=equal)',
    'Access to essential health services (0=none; 100=full coverage)': 'Foundations of Wellbeing, Access to essential health services (0=none; 100=full coverage)',
    'Outdoor air pollution attributable deaths (deaths/100,000)': 'Foundations of Wellbeing, Outdoor air pollution attributable deaths (deaths/100,000)',
    'Deaths from lead exposure (deaths/100,000)': 'Foundations of Wellbeing, Deaths from lead exposure (deaths/100,000)',
    'Particulate matter pollution (mean annual exposure, µg/m3)': 'Foundations of Wellbeing, Particulate matter pollution (mean annual exposure, µg/m3)',
    'Species protection (0=low;100=high)': 'Foundations of Wellbeing, Species protection (0=low;100=high)',

    'Access to justice (0=non-existent; 1=observed)': 'Opportunity, Access to justice (0=non-existent; 1=observed)',
    'Freedom of expression (0=no freedom; 1=full freedom)': 'Opportunity, Freedom of expression (0=no freedom; 1=full freedom)',
    'Freedom of religion (0=no freedom; 4=full freedom)': 'Opportunity, Freedom of religion (0=no freedom; 4=full freedom)',
    'Political rights (0 and lower=no rights; 40=full rights)': 'Opportunity, Political rights (0 and lower=no rights; 40=full rights)',
    'Property rights for women (0=no rights; 5= full rights)': 'Opportunity, Property rights for women (0=no rights; 5= full rights)',
    'Satisfied demand for contraception (% of satisfied demand)': 'Opportunity, Satisfied demand for contraception (% of satisfied demand)',
    'Perception of corruption (0=high corruption; 100=low corruption)': 'Opportunity, Perception of corruption (0=high corruption; 100=low corruption)',
    'Early marriage (% of married women aged 15-19)': 'Opportunity, Early marriage (% of married women aged 15-19)',
    'Young people not in education, employment or training (% of youth)': 'Opportunity, Young people not in education, employment or training (% of youth)',
    'Vulnerable employment (% of total employment)': 'Opportunity, Vulnerable employment (% of total employment)',
    'Equality of political power by gender (0=unequal power; 4=equal power)': 'Opportunity, Equality of political power by gender (0=unequal power; 4=equal power)',
    'Equality of political power by social group (0=unequal power; 4=equal power)': 'Opportunity, Equality of political power by social group (0=unequal power; 4=equal power)',
    'Equality of political power by socioeconomic position (0=unequal power; 4=equal power)': 'Opportunity, Equality of political power by socioeconomic position (0=unequal power; 4=equal power)',
    'Discrimination and violence against minorities (0=low; 10=high)': 'Opportunity, Discrimination and violence against minorities (0=low; 10=high)',
    'Acceptance of gays and lesbians (0=low; 1=high)': 'Opportunity, Acceptance of gays and lesbians (0=low; 1=high)',
    'Citable documents (documents/1000 people)': 'Opportunity, Citable documents (documents/1000 people)',
    'Academic freedom (0=low; 1=high)': 'Opportunity, Academic freedom (0=low; 1=high)',
    'Women with advanced education (proportion of females)': 'Opportunity, Women with advanced education (proportion of females)',
    'Expected years of tertiary schooling (years)': 'Opportunity, Expected years of tertiary schooling (years)',
    'Quality weighted universities (points)': 'Opportunity, Quality weighted universities (points)'

}, inplace=True)

########## Units specified for each column in the source ##############
units = {'2':'index',
         '3':'index',
         '4':'index',
         '5':'index',
         '6':'index',
         '7':'index',
         '8':'index',
         '9':'index',
         '10':'index',
         '11':'index',
         '12':'index',
         '13':'index',
         '14':'index',
         '15':'index',
         '16':'index',
         '17':'index',
         '18':'rate',
         '19':'rate',
         '20':'index',
         '21':'rate',
         '22':'percent',
         '23':'rate',
         '24':'rate',
         '25':'rate',
         '26':'rate',
         '27':'index',
         '28':'percent',
         '29':'percent',
         '30':'rate',
         '31':'rate',
         '32':'index',
         '33':'index',
         '34':'rate',
         '35':'index',
         '36':'percent',
         '37':'percent',
         '38':'index',
         '39':'index',
         '40':'percent',
         '41':'index',
         '42':'rate',
         '43':'years',
         '44':'rate',
         '45':'index',
         '46':'index',
         '47':'rate',
         '48':'rate',
         '49':'mg_m3',
         '50':'index',
         '51':'index',
         '52':'index',
         '53':'index',
         '54':'index',
         '55':'index',
         '56':'percent',
         '57':'index',
         '58':'percent',
         '59':'percent',
         '60':'percent',
         '61':'index',
         '62':'index',
         '63':'index',
         '64':'index',
         '65':'index',
         '66':'rate',
         '67':'index',
         '68':'rate',
         '69':'years',
         '70':'points'
}

try:
    conn = psycopg2.connect(
        host = db_connect.UniProd.host,
        user = db_connect.UniProd.user,
        database = db_connect.UniProd.database,
        password = db_connect.UniProd.password
    )

    cur = conn.cursor()
    print(f'Connected to the database...')
except:
    print('Unable to connect to the database...')

for i in range(df_spi.shape[0]):  ## FOR EACH COUNTRY (row)
    row_ = df_spi.iloc[i]
    country = df_spi.iloc[i]['Country']

    for k in range(len(df_spi.columns)):  # FOR EACH COLUMN

        if k > 1 and np.isnan(df_spi.iloc[i][k]) == False:

            series_state_id_py = source_vs_db_state[country]

            series_id_py = 'spi_' + series_state_id_py + str(k)
            series_name_py = df_spi.columns[k]
            series_unit_id_py = units[str(k)]

            ###SERIES CREATION#####

            sql_statement = ''' INSERT INTO SERIES(series_id, series_name, dataset_id, series_frequency_id, series_unit_id, series_state_id)
                                VALUES (%s,%s,%s,%s,%s,%s)
                                ON CONFLICT DO NOTHING
                            '''
            sql_values = (
            series_id_py, series_name_py, 'spi_soc_prog_index', 'annual', series_unit_id_py, series_state_id_py)

            cur.execute(sql_statement, sql_values)

            ####SERIES VALUES UPDATE#####

            sql_statement_v = '''INSERT INTO SERIES_VALUES(series_id, series_date, series_values)
                                VALUES (%s,TO_DATE(%s, 'YYYY'),%s)
                                ON CONFLICT(series_id,series_date) DO UPDATE SET series_values = %s
                            '''
            sql_values_v = (series_id_py,
                            str(df_spi.iloc[i]['SPI \nyear']),
                            float(df_spi.iloc[i][k]),
                            float(df_spi.iloc[i][k])
                            )

            cur.execute(sql_statement_v, sql_values_v)

            conn.commit()
            print(cur.rowcount, f" series_created {series_id_py} ")
            print(cur.rowcount, f' for series {series_id_py} , period {df_spi.iloc[i][1]}')

        else:
            pass


cur.close()
conn.close()