from pyspark.sql import SparkSession, functions, types
from pyspark.sql.window import Window
#from pyspark.sql.functions import lit
spark = SparkSession.builder.appName('citi analytics') \
    .config('spark.cassandra.connection.host', '127.0.0.1').getOrCreate()
spark.sparkContext.setLogLevel('WARN')
sc = spark.sparkContext

def main():

    transaction_data = spark.read.format("org.apache.spark.sql.cassandra") \
    .options(table='grouped_count', keyspace='bike_share_analytics').load()


    transaction_data.createOrReplaceTempView('count_df')

    #Percentage change between years and months for number of trips

    prev_count=spark.sql("SELECT current.startmonth AS month,current.count AS current_month_count,previous.count AS previous_month_count, \
                        current.startyear AS year\
                          FROM count_df as current,count_df AS previous \
                          WHERE current.startmonth=previous.startmonth+1 AND current.startyear = previous.startyear")
    prev_count.createOrReplaceTempView('prev_count')


    percentage_change=spark.sql("SELECT month,year,((current_month_count-previous_month_count)/current_month_count)*100 AS percentage_change \
                          FROM prev_count")
    

    percentage_change.write.format("org.apache.spark.sql.cassandra") \
    .options(table='percentage_change', keyspace='bike_share_analytics').save()



if __name__ == '__main__':
    main()
