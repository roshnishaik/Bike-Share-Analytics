import sys
import json

from pyspark.sql import SparkSession, functions, types
from pyspark.sql.types import ArrayType, StringType

cluster_seeds = ['199.60.17.32', '199.60.17.65']

spark = SparkSession.builder.appName('citi stream').getOrCreate()
spark.sparkContext.setLogLevel('WARN')

def required_attributes(value):
    json_value = json.loads(value)
    stationBeanList = json_value['stationBeanList']
    reqBikeAttributes = []
    for i in stationBeanList:
        reqBikeAttributes.append({"id":i['id'], "stationName":i['stationName'], "availableBikes":i['availableBikes'],"latitude":i['latitude'], "longitude":i['longitude'], "longitude":i['longitude'], "availableDocks":i['availableDocks']})

    return reqBikeAttributes

def main():
    #station schema
    station_schema = types.StructType([
    types.StructField('id', types.IntegerType()),
    types.StructField('stationName', types.StringType()),
    types.StructField('availableBikes', types.IntegerType()),
    types.StructField('latitude', types.FloatType()),
    types.StructField('longitude', types.FloatType()),
    types.StructField('availableDocks',types.IntegerType()),
    ])

    messages = spark.readStream.format('kafka') \
            .option('kafka.bootstrap.servers', '199.60.17.210:9092,199.60.17.193:9092') \
            .option('subscribe', 'citi1').load()
    values=messages.select(messages['value'].cast('string'))

    req_attr_udf = functions.udf(required_attributes, ArrayType(elementType = station_schema))
    values = values.withColumn('value', req_attr_udf(values['value']))

    stationList = values.select('*', functions.explode(values['value']).alias('exploded_col')).select('exploded_col.*')
    stream = stationList.writeStream.format('console').format("org.apache.spark.sql.cassandra") \
            .options(table='stationinfo', keyspace='bike_share_analytics') \
            .outputMode('update').start()

    stream.awaitTermination(20)

if __name__ == '__main__':
    main()





'''CREATE table stationinfo(
stationName string,
availableBikes int,
latitude float,
longitude float,
availableDocks int
);
'''
