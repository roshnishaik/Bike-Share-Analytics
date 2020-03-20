CREATE KEYSPACE bike_share_analytics WITH REPLICATION = {
'class': 'SimpleStrategy', 'replication_factor': 2 };


CREATE TABLE station_data ( 
    altitude text, 
    "availableBikes" int,
    "availableDocks" int,
    city text,
    id int PRIMARY KEY,
    "landMark" text,
    "lastCommunicationTime" text,
    latitude float,
    longitude float,
    location text,
    "postalCode" text,
    "stAddress1" text,
    "stAddress2" text,
    "stationName" text,
    "statusKey" int,
    "statusValue" text,
    "testStation" boolean,
    "totalDocks" int,
    "weather_station_id" text);



CREATE TABLE weather_station_data ( 
    id text PRIMARY KEY,
    name text,
    region text,
    wmo text,
    icao text,
    iata text,
    latitude float,
    longitude float,
    elevation float,
    time_zone text
    );


CREATE TABLE weather_data1 ( 
    id text, 
    name text,
    latitude float,
    longitude float,
    time text,
    time_local text,
    temperature float,
    dewpoint float,
    humidity float,
    precipitation text,
    precipitation_3 text,
    precipitation_6 text,
    snowdepth text,
    windspeed float,
    peakgust text,
    winddirection float,
    pressure text,
    condition text,
    PRIMARY KEY (id,time));

CREATE TABLE transaction_data2 ( 
    tripduration int, 
    starttime timestamp,
    stoptime timestamp,
    "start station id" int,
    "start station name" text,
    "start station latitude" float,
    "start station longitude" float,
    "end station id" int,
    "end station name" text,
    "end station latitude" float,
    "end station longitude" float,
    bikeid int,
    usertype text,
    "birth year" int,
    gender int,
    startmonth int,
    starthour int,
    weekday int,
    weekend int,
    stopmonth int,
    stophour int,
    age int,
    PRIMARY KEY ("start station id",bikeid,starttime));

CREATE TABLE transaction_data5 ( 
    tripduration int, 
    starttime timestamp,
    stoptime timestamp,
    "start station id" int,
    "start station name" text,
    "start station latitude" float,
    "start station longitude" float,
    "end station id" int,
    "end station name" text,
    "end station latitude" float,
    "end station longitude" float,
    bikeid int,
    usertype text,
    "birth year" int,
    gender int,
    age int,
    dayofyear int,
    starthour int,
    startmonth int,
    startyear int,
    stophour int,
    stopmonth int,
    weekday int,
    weekend int,
    temperature float,
    precipitation float,
    humidity int,
    dewpoint float,
    windspeed float,
    PRIMARY KEY ("start station id",bikeid,starttime));


CREATE TABLE trip_duration_age ( 
    age int PRIMARY KEY, 
    avg_trip_duration float);

CREATE TABLE trips_age ( 
    age int PRIMARY KEY, 
    total_count int,
    males int,
    females int,
    unknown int);

CREATE TABLE grouped_count ( 
    startyear int , 
    startmonth int,
    count int,
    PRIMARY KEY (startyear,startmonth));

CREATE TABLE grouped_count_season ( 
    startyear int , 
    startmonth int,
    starthour text,
    count int,
    season text,
    PRIMARY KEY (startyear,startmonth,starthour));

CREATE TABLE percentage_change ( 
    month int , 
    year int,
    percentage_change float,
    PRIMARY KEY (year,month));

CREATE TABLE total_trips ( 
    total_bikes int PRIMARY KEY);

CREATE TABLE distinct_bikes ( 
    number_of_bikes int PRIMARY KEY);

CREATE TABLE start_trip_top_20 ( 
    "start station name" text PRIMARY KEY, 
    "start station latitude" float,
    "start station longitude" float,
    number_of_trips int);

CREATE TABLE average_trip_user_type ( 
    usertype text PRIMARY KEY,
    number_of_trips int,
    average_duration float,
    );

CREATE TABLE start_to_end_top_20 ( 
    usertype text,
    route text,
    number_of_trips int,
    average_duration float,
    PRIMARY KEY ("usertype","route"));

CREATE TABLE end_trip_top_20 ( 
    "end station name" text PRIMARY KEY, 
    "end station latitude" float,
    "end station longitude" float,
    number_of_trips int);

CREATE TABLE avg_temp ( 
    temperature float PRIMARY KEY, 
    number_of_trips int,
    average_duration float);

CREATE TABLE phmd ( 
    precipitation float,
    humidity int,
    number_of_trips int,
    average_duration float,
    PRIMARY KEY ("precipitation","humidity")
    );

CREATE TABLE windspeed ( 
    windspeed float PRIMARY KEY,
    number_of_trips int,
    average_duration float);

CREATE TABLE events ( 
    event_id int,
    event_name text,
    start_date_time text,
    end_date_time text,
    event_agency text,
    event_type text,
    event_borough text,
    event_location text,
    latitude float,
    longitude float,
    PRIMARY KEY(event_id,start_date_time));


create table dtree(
sstid int,
starthour int,
startdate date,
startmonth int,
weekday int,
weekend int,
past_trips int,
number_of_trips int,
prediction float,
PRIMARY KEY (sstid,startdate,starthour)
)

create table gbt(
sstid int,
starthour int,
startdate date,
startmonth int,
weekday int,
weekend int,
past_trips int,
number_of_trips int,
prediction float,
PRIMARY KEY (sstid,startdate,starthour)
)
create table rf(
sstid int,
starthour int,
startdate date,
startmonth int,
weekday int,
weekend int,
past_trips int,
number_of_trips int,
prediction float,
PRIMARY KEY (sstid,startdate,starthour)
)

create table dtree_weather(
sstid int,
starthour int,
startdate date,
startmonth int,
weekday int,
weekend int,
past_trips int,
number_of_trips int,
prediction float,
PRIMARY KEY (sstid,startdate,starthour)
);

create table gbt_weather(
sstid int,
starthour int,
startdate date,
startmonth int,
weekday int,
weekend int,
past_trips int,
number_of_trips int,
prediction float,
PRIMARY KEY (sstid,startdate,starthour)
);
create table rf_weather(
sstid int,
starthour int,
startdate date,
startmonth int,
weekday int,
weekend int,
past_trips int,
number_of_trips int,
prediction float,
PRIMARY KEY (sstid,startdate,starthour)
);

CREATE table stationinfo(
stationName string,
availableBikes int,
latitude float,
longitude float,
availableDocks int,
PRIMARY KEY (stationName)
);