Bike Share Analytics

1. Dowload trips data from - "https://s3.amazonaws.com/tripdata/index.html" and save it in data folder.
   Download events data from - "https://data.cityofnewyork.us/City-Government/NYC-Permitted-Event-Information/tvpp-9vvx"	

2. Create a local Cassandra cluster at 127.0.0.1:9042


3. Start cqlsh 

4. Run Create_statements.cql file at CQLSH shell using command - 
	source 'file_where_file_stored_locally/Create_statements.cql'

	For ref - https://docs.datastax.com/en/cql/3.3/cql/cql_reference/cqlshSource.html

	Give the name of the folder where file is stored on your local

5. Run the spark codes in the ETL folder in the following order on your terminal using the command - 

	spark-submit --packages datastax:spark-cassandra-connector:2.4.0-s_2.11 --driver-memory 4G file_name.py


	4.1 transaction.py
	4.2 station_data.py
	4.3 weather_etl.py
	4.4 transaction_weather.py
	4.5	events_cassandra_load.py

These will load data in the tables we created in instruction 4.

6. Run the files in below sequence in order to get the streaming data

   6.1 create_topic.py
   6.2 citi_producer.py
   6.3 citi_streaming.py
   
   Command to run 6.1 and 6.2:
   python3 filename.py
   
   Command to run 6.3:
   spark-submit --packages datastax:spark-cassandra-connector:2.4.0-s_2.11 org.apache.spark:spark-sql-kafka-0-10_2.11:2.4.4 citi_streaming.py

7. Now run the files in the Prediction folder in following order - 

	7.1 predict_bikes.py
	7.2 predict_bikes_weather.py

	Command to run 7.1 and 7.2: spark-submit filename.py

These will run the prediction models on the data , save the predictions in cassandra and the train/validation errors on the console.

8. Now run the files in Analytics folder in following order - 

	8.1 analytics1.py
	8.2 analytics2.py
	8.3 analytics3.py
	8.4 analytics4.py
	8.5 analytics5.py
	8.6 analytics6.py

These will generate data from analytics in the Cassandra tables.

9. Run the files in the below order to get the find the nearest events to the stations with high bike inflow.

	9.1 events_etl.py
	Command to run 9.1: spark-submit filename.py events_input_file_location events_processed_outputfile_location

	9.2 nearest_events.py
	Command to run 9.2: spark-submit filename.py bike_trip_inputfile_location events_processed_outputfile_location nearestevents_outputfile_location

10. For visualizations we have used Tableau. 

	To connect Tableau with Cassandra, we have used Simba ODBC Connector to create a Data Source Name.

	Then in Tableau we connect to a ODBC souce with the given Data Source Name.

