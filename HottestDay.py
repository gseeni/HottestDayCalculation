#!/usr/bin/env python
# coding: utf-8


#Importing the Required classes

import pyspark.sql.functions as f
import pyspark.sql.types
from pyspark.sql import Window
from pyspark.sql import SparkSession

#Creating Spark Session and Reads the csv files
sc = SparkSession.builder.master("local").appName("WeatherTest").getOrCreate()

df = sc.read.csv('C:\Govind\WeatherData\*.csv',header=True)
#df.count()


#Removing duplicate records from the input csv files

df = df.dropDuplicates()
df.count()


#Converting the necessary columns to make ready for the calculatios and check

df = df.withColumn("ObservationDate",df.ObservationDate.cast('date'))    
	.withColumn("ScreenTemperature", df.ScreenTemperature.cast('float'))    
	.withColumn("ObservationTime", df.ObservationTime.cast('int'))    
	.withColumn("SignificantWeatherCode", df.SignificantWeatherCode.cast('int'))  


#Removing the records whose ScreenTemperature is -99 (outlier) and considering only sunny hours

df = df.filter(("SignificantWeatherCode ==1") or ("ScreenTemperature != -99") )
df.count()


#Dropping the columns which are not considered for the calculation 

df = df.drop("ForecastSiteCode",'ObservationTime','WindDirection','SiteName','Country', 'WindSpeed','WindGust','Visibility', 'Pressure','SignificantWeatherCode','Latitude', 'Longitude')

#Grouping the data based on date and region to find out the average temperature for a day and write those records in parquet file

windowSpec = Window.partitionBy("ObservationDate","Region")
result = df.withColumn("AvgDayTemperature",f.avg(f.col("ScreenTemperature")).over(windowSpec)).drop("ScreenTemperature")
result = result.distinct()
result.write.parquet('C:\Govind\WeatherData\Grouped_weather_data',mode='overwrite')

#Reading the converted parquet file and create a temporary view out of that

pfile = sc.read.parquet('C:\Govind\WeatherData\Grouped_weather_data')
pfile.createOrReplaceTempView("pfile")


#Viewing the records based on the average temperature 

query = "select ObservationDate,Region,round(AvgDayTemperature,1) HighestTemperature from pfile order by AvgDayTemperature desc"
data = sc.sql(query)
data.show(truncate=False)

