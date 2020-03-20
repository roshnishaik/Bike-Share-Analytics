import sys
#assert sys.version_info >= (3, 5) # make sure we have Python 3.5+

from pyspark.sql import SparkSession, functions, types
#spark = SparkSession.builder.appName('example code').getOrCreate()
spark = SparkSession.builder.appName('weather_staion_etl') \
    .config('spark.cassandra.connection.host', '127.0.0.1').getOrCreate()
#assert spark.version >= '2.4' # make sure we have Spark 2.4+
spark.sparkContext.setLogLevel('WARN')
sc = spark.sparkContext

import json,pandas,numpy
import requests
# add more functions as necessary

def api_function(station_id):
    r = requests.get("https://api.meteostat.net/v1/stations/meta?station={}&key=FBzg5ET9".format(station_id))
    r = json.loads(r.text)['data']
    return [r]

def weather_api_function(station_id):
    time_zone = "America/New_York"
    start_date = "2013-01-01"
    end_date = "2019-11-01"
    key = "FBzg5ET9"
    r = requests.get("https://api.meteostat.net/v1/history/hourly?station={}&start={}&end={}&time_zone={}&time_format=Y-m-d%20H:i&key={}".format(station_id,start_date,end_date,time_zone,key))
    r = json.loads(r.text)['data']
    return r


def main():
    # main logic starts here

    #Schema for two tables
    weather_station_schema1 = types.StructType([
    types.StructField('id', types.StringType()),
    types.StructField('name', types.StringType()),
    types.StructField('country',types.StructType([
        types.StructField("code",types.StringType()),
        types.StructField("name",types.StringType())])),
    types.StructField('region', types.StringType()),
    types.StructField('wmo', types.StringType()),
    types.StructField('icao', types.StringType()),
    types.StructField('iata', types.StringType()),
    types.StructField('latitude', types.StringType()),
    types.StructField('longitude', types.StringType()),
    types.StructField('elevation', types.StringType()),
    types.StructField('time_zone', types.StringType())])

    weather_schema = types.StructType([
    types.StructField('time', types.StringType()),
    types.StructField('time_local', types.StringType()),
    types.StructField('temperature', types.FloatType()),
    types.StructField('dewpoint', types.FloatType()),
    types.StructField('humidity', types.IntegerType()),
    types.StructField('precipitation', types.FloatType()),
    types.StructField('precipitation_3', types.FloatType()),
    types.StructField('precipitation_6', types.FloatType()),
    types.StructField('snowdepth', types.FloatType()),
    types.StructField('windspeed', types.FloatType()),
    types.StructField('peakgust', types.FloatType()),
    types.StructField('winddirection', types.IntegerType()),
    types.StructField('pressure', types.IntegerType()),
    types.StructField('condition', types.IntegerType())])



    #Querying nearby stations

    urlString = "https://api.meteostat.net/v1/stations/nearby"
    querystring = {"lat":"40.730610","lon":"-73.935242","limit":"10","key":"FBzg5ET9"}
    rdd_query = sc.parallelize([querystring])
    df_query = spark.read.json(rdd_query)
    df_query.createOrReplaceTempView('querystring_inputs')
    prms = {'url':urlString,'input':'querystring_inputs','method':'GET'}
    df_output = spark.read.format("org.apache.dsext.spark.datasource.rest.RestDataSource").options(**prms).load()
    df_nearby_stations = df_output.select("output.data")
    df_nearby_stations = df_nearby_stations.select(functions.explode(df_nearby_stations.data).alias('data')).select('data.*')

    #fetching station details
    apiCall = functions.udf(lambda z: api_function(z), types.ArrayType(elementType=weather_station_schema1))
    df_stations = df_nearby_stations.withColumn("station_metadata",apiCall(df_nearby_stations.id))
    df_stations = df_stations.select(functions.explode(df_stations.station_metadata).alias('station_metadata')).select('station_metadata.*')
    df_stations = df_stations.withColumn("latitude", df_stations["latitude"].cast(types.FloatType()))
    df_stations = df_stations.withColumn("longitude", df_stations["longitude"].cast(types.FloatType()))
    df_stations = df_stations.select("id","name","region","wmo","icao","iata","latitude","longitude","elevation","time_zone")
        


    #Writing station data into cassandra

    df_stations.write.format("org.apache.spark.sql.cassandra") \
    .options(table='weather_station_data', keyspace='bike_share_analytics').save()


    #Fetching historical weather data 

    weather_api_call = functions.udf(lambda z: weather_api_function(z), types.ArrayType(elementType=weather_schema))
    df_weather_fetch = df_stations.withColumn("weather_data",weather_api_call(df_stations.id))
    df_weather = df_weather_fetch.select("id","name",'latitude','longitude',functions.explode(df_weather_fetch.weather_data).alias('weather_data')).select('id','name','latitude','longitude','weather_data.*')

    #Writing weather data into cassandra

    df_weather.write.format("org.apache.spark.sql.cassandra") \
    .options(table='weather_data1', keyspace='bike_share_analytics').save()


if __name__ == '__main__':
    
    main()

    