# Data Engineering Project

Welcome to the data engineering project. The goal of the project is to summarise the knowledge on SQL and Python as well as combination of it. 

The Project contains:  
1. [**MACRO UNI DB**](https://github.com/alinavit/macro_uni#1macro-uni-db)  
    * Description of the database Structure (ERD)  
2. [**Datasets and Data Extraction/Transformatio/Upload to PostgreSQL using Python**](https://github.com/alinavit/macro_uni#2-datasets-and-data-extractiontransformationupload-to-postgresql-using-python)
    * PredictIt, World
    * Energy-Related Carbon Dioxide Emissions by State, EIA, United States
    * Social Progress Index, Social Progress Imperative, World
    * Monetary and Financial Framework, BCRA, Argentina
    * Main Variables of the Central Bank of Argentina, BCRA, Argentina
  
3. [**Data Visualisation via Power BI (screen pictures)**](https://github.com/alinavit/macro_uni#3-data-visualisation-via-power-bi-screen-pictures)



## 1.MACRO UNI DB

**Macro Uni DB** is a relational database that is stored in PostgreSQL. 
Currently it contains 8 tables that helps to normalise data into Second Normal Form.  

The tables are as follows:
1. Countries
2. States
3. Sources
4. Units
5. Frequencies
6. Datasets
7. Series
8. Series Values

[Entity Relationship Diagram](https://github.com/alinavit/macro_uni/blob/main/01.%20ERD%20Pg%20Admin%204%20database%20m_uni.pgerd)
(created in phAdmin 4)  
[Entity Relationship Diagram (picture)](https://github.com/alinavit/macro_uni/blob/main/erd_2.png)  

[Entities created in the databse based on SQL script](https://github.com/alinavit/macro_uni/blob/main/03.%20database_entities%20(EMPTY%20TABLES).sql)  

## 2. Datasets and Data Extraction/Transformation/Upload to PostgreSQL using Python

### PredictIt
[Source](https://www.predictit.org/)  
PredictIt is a unique and exciting real money site that tests your knowledge of political events by letting you trade shares on everything from the outcome of an election to a Supreme Court decision to major world events.  
[Direct Source of data (json)](https://www.predictit.org/api/marketdata/all/)  
[Automation (Python)](https://github.com/alinavit/macro_uni/blob/main/auto_predictit_markets_git.py)  
[Log File](https://github.com/alinavit/macro_uni/blob/main/auto_predictit_markets_git_runs.txt)

### Energy-Related Carbon Dioxide Emissions by State, EIA
[Source](https://www.eia.gov/)    
This data shows the emission released at locations where fossil fuels are consumed, not generated. This is the place where basically the electricity or fuels as the end product are combusted.   
*Example for US, Total*  
[Direct Source to data (json)](http://api.eia.gov/series/?api_key=YOUR_API_KEY_HERE&series_id=EMISS.CO2-TOTV-TT-TO-US.A) *API KEY REQUIRED*  
[*Alternative View*](https://www.eia.gov/opendata/qb.php?category=2251669&sdid=EMISS.CO2-TOTV-TT-TO-US.A)  
[Automation Python](https://github.com/alinavit/macro_uni/blob/main/auto_eia_emmision_git.py)  

### Social Progress Index, Social Progress Imperative
[Source](https://www.socialprogress.org/)  
At the Social Progress Imperative, we define social progress as the capacity of a society to meet the basic human needs of its citizens, establish the building blocks that allow citizens and communities to enhance and sustain the quality of their lives, and create the conditions for all individuals to reach their full potential. Improving quality of life is a complex task and past efforts to measure progress simply haven’t created a sufficiently nuanced picture of what a successful society looks like. [@SPI](https://www.socialprogress.org/index/global)  

The Source data was sent on request to email  

Automation for this source is done mainly for speedy load into the DB. For the future, it is very likely the automation code will have to be adjusted.  
[Automation Python](https://github.com/alinavit/macro_uni/blob/main/auto_spi_soc_prog_index_git.py)

### Monetary and Financial Framework, BCRA (Argentina)
[Source](http://www.bcra.gov.ar/)  
This source presents some data of the Mpnetary Statistics of the Central Bank of Argentina such as main liabilities or interest rates.   
The database includes data from this [file](http://www.bcra.gov.ar/Pdfs/PublicacionesEstadisticas/panhis.xls) sheet: 'Cuadro'.  
[Automation Python](https://github.com/alinavit/macro_uni/blob/main/auto_bcra_mon_fin_framework_git.py)


### Main Variables of the Central Bank of Argentina, BCRA (Argentina)
[Source](http://www.bcra.gob.ar//PublicacionesEstadisticas/Principales_variables_i.asp)  
Here can be found top main variables that are presented by BCRA. All data is accessible online on the page.  
[Automation Python](https://github.com/alinavit/macro_uni/blob/main/auto_bcra_main_var_git.py)  


## 3. Data Visualisation via Power BI (screen pictures)

**COVID New Cases in the United States**  
  
![covid_new_cases_total_power_bi](https://github.com/alinavit/macro_uni/blob/main/COVID%20NEW%20CASES.png)

**COVID New Deaths in the United States, Connecticut**  
  
![covid_death_connecticut](https://github.com/alinavit/macro_uni/blob/main/COVID%20NEW%20DEATH%20CONNECTICUT.png) 

**Which party will control the Senate after 2022 election?. PredictIt**  
  
![predictit_6874_](https://github.com/alinavit/macro_uni/blob/main/predictit_6874_.png)

