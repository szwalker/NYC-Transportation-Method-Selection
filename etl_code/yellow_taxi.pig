my_input = LOAD 'yellow_tripdata_2018-01.csv' USING PigStorage(',') AS (VendorID:int, tpep_pickup_datetime:chararray, tpep_dropoff_datetime:chararray,passenger_count:int,trip_distance:float,RatecodeID:int,store_and_fwd_flag:chararray,PULocationID:int,DOLocationID:int,payment_type:int,fare_amount:float,mta_tax:float,tip_amount:float,tolls_amount:float,improvement_surcharge:float,total_amount:float);
cleaner_input = FOREACH my_input GENERATE VendorID,tpep_pickup_datetime,tpep_dropoff_datetime,trip_distance,PULocationID,DOLocationID;
cleaned_input = FILTER cleaner_input BY VendorID IS NOT NULL AND tpep_pickup_datetime IS NOT NULL AND tpep_dropoff_datetime IS NOT NULL AND trip_distance IS NOT NULL AND PULocationID IS NOT NULL AND DOLocationID IS NOT NULL;
STORE cleaned_input INTO 'cleaned_taxi/taxi_cleaned_1';

my_input = LOAD 'yellow_tripdata_2018-02.csv' USING PigStorage(',') AS (VendorID:int, tpep_pickup_datetime:chararray, tpep_dropoff_datetime:chararray,passenger_count:int,trip_distance:float,RatecodeID:int,store_and_fwd_flag:chararray,PULocationID:int,DOLocationID:int,payment_type:int,fare_amount:float,mta_tax:float,tip_amount:float,tolls_amount:float,improvement_surcharge:float,total_amount:float);
cleaner_input = FOREACH my_input GENERATE VendorID,tpep_pickup_datetime,tpep_dropoff_datetime,trip_distance,PULocationID,DOLocationID;
cleaned_input = FILTER cleaner_input BY VendorID IS NOT NULL AND tpep_pickup_datetime IS NOT NULL AND tpep_dropoff_datetime IS NOT NULL AND trip_distance IS NOT NULL AND PULocationID IS NOT NULL AND DOLocationID IS NOT NULL;
STORE cleaned_input INTO 'cleaned_taxi/taxi_cleaned_2';

my_input = LOAD 'yellow_tripdata_2018-03.csv' USING PigStorage(',') AS (VendorID:int, tpep_pickup_datetime:chararray, tpep_dropoff_datetime:chararray,passenger_count:int,trip_distance:float,RatecodeID:int,store_and_fwd_flag:chararray,PULocationID:int,DOLocationID:int,payment_type:int,fare_amount:float,mta_tax:float,tip_amount:float,tolls_amount:float,improvement_surcharge:float,total_amount:float);
cleaner_input = FOREACH my_input GENERATE VendorID,tpep_pickup_datetime,tpep_dropoff_datetime,trip_distance,PULocationID,DOLocationID;
cleaned_input = FILTER cleaner_input BY VendorID IS NOT NULL AND tpep_pickup_datetime IS NOT NULL AND tpep_dropoff_datetime IS NOT NULL AND trip_distance IS NOT NULL AND PULocationID IS NOT NULL AND DOLocationID IS NOT NULL;
STORE cleaned_input INTO 'cleaned_taxi/taxi_cleaned_3';

my_input = LOAD 'yellow_tripdata_2018-04.csv' USING PigStorage(',') AS (VendorID:int, tpep_pickup_datetime:chararray, tpep_dropoff_datetime:chararray,passenger_count:int,trip_distance:float,RatecodeID:int,store_and_fwd_flag:chararray,PULocationID:int,DOLocationID:int,payment_type:int,fare_amount:float,mta_tax:float,tip_amount:float,tolls_amount:float,improvement_surcharge:float,total_amount:float);
cleaner_input = FOREACH my_input GENERATE VendorID,tpep_pickup_datetime,tpep_dropoff_datetime,trip_distance,PULocationID,DOLocationID;
cleaned_input = FILTER cleaner_input BY VendorID IS NOT NULL AND tpep_pickup_datetime IS NOT NULL AND tpep_dropoff_datetime IS NOT NULL AND trip_distance IS NOT NULL AND PULocationID IS NOT NULL AND DOLocationID IS NOT NULL;
STORE cleaned_input INTO 'cleaned_taxi/taxi_cleaned_4';

my_input = LOAD 'yellow_tripdata_2018-05.csv' USING PigStorage(',') AS (VendorID:int, tpep_pickup_datetime:chararray, tpep_dropoff_datetime:chararray,passenger_count:int,trip_distance:float,RatecodeID:int,store_and_fwd_flag:chararray,PULocationID:int,DOLocationID:int,payment_type:int,fare_amount:float,mta_tax:float,tip_amount:float,tolls_amount:float,improvement_surcharge:float,total_amount:float);
cleaner_input = FOREACH my_input GENERATE VendorID,tpep_pickup_datetime,tpep_dropoff_datetime,trip_distance,PULocationID,DOLocationID;
cleaned_input = FILTER cleaner_input BY VendorID IS NOT NULL AND tpep_pickup_datetime IS NOT NULL AND tpep_dropoff_datetime IS NOT NULL AND trip_distance IS NOT NULL AND PULocationID IS NOT NULL AND DOLocationID IS NOT NULL;
STORE cleaned_input INTO 'cleaned_taxi/taxi_cleaned_5';

my_input = LOAD 'yellow_tripdata_2018-06.csv' USING PigStorage(',') AS (VendorID:int, tpep_pickup_datetime:chararray, tpep_dropoff_datetime:chararray,passenger_count:int,trip_distance:float,RatecodeID:int,store_and_fwd_flag:chararray,PULocationID:int,DOLocationID:int,payment_type:int,fare_amount:float,mta_tax:float,tip_amount:float,tolls_amount:float,improvement_surcharge:float,total_amount:float);
cleaner_input = FOREACH my_input GENERATE VendorID,tpep_pickup_datetime,tpep_dropoff_datetime,trip_distance,PULocationID,DOLocationID;
cleaned_input = FILTER cleaner_input BY VendorID IS NOT NULL AND tpep_pickup_datetime IS NOT NULL AND tpep_dropoff_datetime IS NOT NULL AND trip_distance IS NOT NULL AND PULocationID IS NOT NULL AND DOLocationID IS NOT NULL;
STORE cleaned_input INTO 'cleaned_taxi/taxi_cleaned_6';

-- author: Jiaqi Liu
