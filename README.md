# spark
spark code
UK Climate Historic Station Data Analysis using Apache Spark-2.4 and Scala.

Source URLS:  
============
https://www.metoffice.gov.uk/public/weather/climate-historic/#?tab=climateHistoric
https://www.metoffice.gov.uk/pub/data/weather/uk/climate/stationdata/aberporthdata.txt [Example: aberporth station source URL]

There are 37 stations in total.

Columnn MetaData:
================
 1. yyyy 		-	Four Digit Year
 2. mm 			-   Two Digit Month   
 3. tmax (degC) -  	Mean daily maximum temperature 
 4. tmin (degC) -  	Mean daily minimum temperature    
 5. af days 	-   Days of air frost (af)
 6. rain (mm) 	-   Total rainfall (rain)
 7. sun hours 	-   Total sunshine duration

Some Testable Questions
========================
 1. Rank Stations they have been online(eg: number of measures)  
 2. Rank stations by rainfall and-or sunshine
 3. Worst rainfall and best sunshine for each station with year
 4. Averages for month of may across all stations,best and worst year

Code Description
=================
 1. Code always get's the latest day from source URL's for each station.
 2. Data cleaning is needed for variable length headers and foot note being present when station is Closed.
 3. Additionally there are special characters like * # $, and missing columnn data indicated by(---) which may or may not be consistently  present in each line.
 4. After extracting data completely, sparksession is created to create dataframe on th List of Historical comma seperated data of all stations.
 5. SQL queries are created for the above testable questions by creating temporary view on the dataframe. 
