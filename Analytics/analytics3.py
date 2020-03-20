from pyspark.sql import SparkSession, functions, types
from pyspark.sql.functions import lit
spark = SparkSession.builder.appName('citi analytics') \
    .config('spark.cassandra.connection.host', '127.0.0.1').getOrCreate()
spark.sparkContext.setLogLevel('WARN')
sc = spark.sparkContext

def main():
    #Count of trips based on Year and Month
    transaction_data = spark.read.format("org.apache.spark.sql.cassandra") \
    .options(table='transaction_data5', keyspace='bike_share_analytics').load()

    transaction_data_grouped = transaction_data.groupby(['startyear','startmonth']).count()

    transaction_data_grouped.write.format("org.apache.spark.sql.cassandra") \
    .options(table='grouped_count', keyspace='bike_share_analytics').save()
    

if __name__ == '__main__':
    main()
