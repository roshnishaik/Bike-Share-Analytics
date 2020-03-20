from pyspark.sql import SparkSession, functions, types
from pyspark.sql.functions import lit

spark = SparkSession.builder.appName('citi analytics') \
    .config('spark.cassandra.connection.host', '127.0.0.1').getOrCreate()
spark.sparkContext.setLogLevel('WARN')
sc = spark.sparkContext


@functions.udf(returnType=types.StringType())
def convert_to_season(startmonth):
    if startmonth>=3 and startmonth<=5:
        return 'Spring'
    elif startmonth>=6 and startmonth<=8:
        return 'Summer'
    elif startmonth>=9 and startmonth<=11:
        return "Fall"
    else:
        return "Winter"

@functions.udf(returnType=types.StringType())
def convert_to_apm(starthour):
    if(starthour<12 and starthour!=0):
        return str(starthour)+"AM"
    elif(starthour>12):
        starthour-=12
        return str(starthour)+"PM"
    elif(starthour==0):
        return "12AM"
    elif(starthour==12):
        return "12PM"

def main():

    transaction_data = spark.read.format("org.apache.spark.sql.cassandra") \
    .options(table='transaction_data5', keyspace='bike_share_analytics').load()

    transaction_data_grouped = transaction_data.groupby(['startyear','startmonth','starthour']).count()

    #Trips by season 

    transaction_data_grouped=transaction_data_grouped.withColumn('season',convert_to_season(transaction_data_grouped['startmonth'])).withColumn('starthour',convert_to_apm(transaction_data_grouped['starthour']))
    

    transaction_data_grouped.write.format("org.apache.spark.sql.cassandra") \
    .options(table='grouped_count_season', keyspace='bike_share_analytics').save()
    

if __name__ == '__main__':
    main()
