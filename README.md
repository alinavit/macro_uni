# MACRO UNI DB

### Project on creation database with data from different countries|sources etc. and different formats.  

[Entity Relationship Diagram](https://github.com/alinavit/macro_uni/blob/main/01.%20ERD%20Pg%20Admin%204%20database%20m_uni.pgerd)
(created in phAdmin 4)  
[Entity Relationship Diagram (picture)](https://github.com/alinavit/macro_uni/blob/main/02.%20ERD%20Pg%20Admin%204%20database%20m_uni%20PICTURE.png)  

[Entities created in the databse based on SQL script](https://github.com/alinavit/macro_uni/blob/main/03.%20database_entities%20(EMPTY%20TABLES).sql)  

## PredictIt
[Source](https://www.predictit.org/)  
PredictIt is a unique and exciting real money site that tests your knowledge of political events by letting you trade shares on everything from the outcome of an election to a Supreme Court decision to major world events.  
[Direct Source of data (json)](https://www.predictit.org/api/marketdata/all/)  
[Automation (Python)](https://github.com/alinavit/macro_uni/blob/main/auto_predictit_markets_git.py)  

## Energy-Related Carbon Dioxide Emissions by State, EIA
[Source](https://www.eia.gov/)    
This data shows the emission released at locations where fossil fuels are consumed, not generated. This is the place where basically the electricity or fuels as the end product are combusted.   
*Example for US, Total*  
[Direct Source to data (json)](http://api.eia.gov/series/?api_key=YOUR_API_KEY_HERE&series_id=EMISS.CO2-TOTV-TT-TO-US.A) *API KEY REQUIRED*  
[*Alternative View*](https://www.eia.gov/opendata/qb.php?category=2251669&sdid=EMISS.CO2-TOTV-TT-TO-US.A)  
[Automation Python](https://github.com/alinavit/macro_uni/blob/main/auto_eia_emmision_git.py)  

## Social Progress Index, Social Progress Imperative
[Source](https://www.socialprogress.org/)  
At the Social Progress Imperative, we define social progress as the capacity of a society to meet the basic human needs of its citizens, establish the building blocks that allow citizens and communities to enhance and sustain the quality of their lives, and create the conditions for all individuals to reach their full potential. Improving quality of life is a complex task and past efforts to measure progress simply haven’t created a sufficiently nuanced picture of what a successful society looks like. [@SPI](https://www.socialprogress.org/index/global)  

The Source data was sent on request to email  

Automation for this source is done mainly for speedy load into the DB. For the future, it is very likely the automation code will have to be adjusted.  
[Automation Python](https://github.com/alinavit/macro_uni/blob/main/auto_spi_soc_prog_index_git.py)

## Monetary and Financial Framework, BCRA (Argentina)
[Source](http://www.bcra.gov.ar/)  
This source presents some data of the Mpnetary Statistics of the Central Bank of Argentina such as main liabilities or interest rates.   
The database includes data from this [file](http://www.bcra.gov.ar/Pdfs/PublicacionesEstadisticas/panhis.xls) sheet: 'Cuadro'.  
[Automation Python](https://github.com/alinavit/macro_uni/blob/main/auto_bcra_mon_fin_framework_git.py)

