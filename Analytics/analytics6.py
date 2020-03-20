from pyspark.sql import SparkSession, functions, types
from pyspark.sql.functions import lit
spark = SparkSession.builder.appName('citi analytics') \
    .config('spark.cassandra.connection.host', '127.0.0.1').getOrCreate()
spark.sparkContext.setLogLevel('WARN')
sc = spark.sparkContext


def main():

    weather_transaction = spark.read.format("org.apache.spark.sql.cassandra") \
    .options(table='transaction_data5', keyspace='bike_share_analytics').load().cache()


    #temperature analytics
    weather_transaction=weather_transaction.fillna(0)
    weather_transaction.createOrReplaceTempView('weather_transaction')


    avg_temp=spark.sql('SELECT temperature,COUNT(*) AS number_of_trips,AVG(tripduration) AS average_duration \
                         FROM weather_transaction \
                         GROUP BY(temperature)')
    

    avg_temp.write.format("org.apache.spark.sql.cassandra") \
    .options(table='avg_temp', keyspace='bike_share_analytics').save()



    #precipitation and humidity analytics
    phmd=spark.sql('SELECT first(precipitation) AS precipitation,first(humidity) AS humidity,COUNT(*) AS number_of_trips,AVG(tripduration) AS average_duration \
                    FROM weather_transaction \
                    GROUP BY(precipitation,humidity)')
    #phmd.printSchema()
    phmd.write.format("org.apache.spark.sql.cassandra") \
    .options(table='phmd', keyspace='bike_share_analytics').save()
    

    #windspeed analytics
    windspeed=spark.sql("SELECT first(windspeed) AS windspeed,COUNT(*) as number_of_trips,AVG(tripduration) AS average_duration \
                         FROM weather_transaction \
                         GROUP BY(windspeed)")
    
    windspeed.write.format("org.apache.spark.sql.cassandra") \
    .options(table='windspeed', keyspace='bike_share_analytics').save()


if __name__ == '__main__':
    main()
