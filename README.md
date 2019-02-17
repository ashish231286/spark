# spark
spark code
UK Climate Historic Station Data Analysis using Apache Spark-2.4 and Scala.

Source URLS:  
============
https://www.metoffice.gov.uk/public/weather/climate-historic/#?tab=climateHistoric
https://www.metoffice.gov.uk/pub/data/weather/uk/climate/stationdata/aberporthdata.txt [aberporth station source URL]

Columnn MetaData:
================
yyyy 			    -	  Four Digit Year
mm 			      -   Two Digit Month   
tmax (degC) 	-  	Mean daily maximum temperature 
tmin (degC) 	-  	Mean daily minimum temperature    
af days 		  -   Days of air frost (af)
rain (mm) 	  -   Total rainfall (rain)
sun hours 	  -   Total sunshine duration

Some Testable Questions
========================
 1. Rank Stations they have been online(eg: number of measures)  
 2. Rank stations by rainfall and-or sunshine
 3. Worst rainfall and best sunshine for each station with year
 4. Averages for month of may across all stations,best and worst year
