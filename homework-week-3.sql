-- Creating external table referring to gcs path


CREATE OR REPLACE EXTERNAL TABLE `zoomcamp-2024-rdilone.nytaxi.external_green_tripdata_2022`
OPTIONS (
  format = 'parquet',
  uris = ['gs://green-taxi-data-22/green_tripdata_2022-*.parquet']
);


-- Create a non partitioned table from external table
CREATE OR REPLACE TABLE zoomcamp-2024-rdilone.nytaxi.green_tripdata_non_partitoned AS
SELECT * FROM zoomcamp-2024-rdilone.nytaxi.external_green_tripdata_2022;


-- Question 1: What is count of records for the 2022 Green Taxi Data??

SELECT COUNT(VendorID) FROM nytaxi.green_tripdata_non_partitoned

--Question 2. Write a query to count the distinct number of PULocationIDs for the entire dataset on both the tables.
--What is the estimated amount of data that will be read when this query is executed on the External Table and the Table?

SELECT COUNT(DISTINCT PULocationID) FROM nytaxi.external_green_tripdata_2022
SELECT COUNT(DISTINCT PULocationID) FROM nytaxi.green_tripdata_non_partitoned

-- Question 3. How many records have a fare_amount of 0?

SELECT COUNT(fare_amount) FROM nytaxi.green_tripdata_non_partitoned
WHERE fare_amount = 0

-- Question 4. What is the best strategy to make an optimized table in Big Query if your query will always order the results by PUlocationID and filter based on lpep_pickup_datetime? (Create a -- new table with this strategy)

CREATE OR REPLACE TABLE nytaxi.green_tripdata_partitoned_clustered
PARTITION BY DATE(lpep_pickup_datetime)
CLUSTER BY PUlocationID AS
SELECT * FROM zoomcamp-2024-rdilone.nytaxi.external_green_tripdata_2022;

--Write a query to retrieve the distinct PULocationID between lpep_pickup_datetime 06/01/2022 and 06/30/2022 (inclusive)
--Use the materialized table you created earlier in your from clause and note the estimated bytes. Now change the table in the from clause to the partitioned table you created for --question 4 and note the estimated bytes processed. What are these values?
-- Choose the answer which most closely matches.

--This query will process 12.82 MB when run.
SELECT COUNT(DISTINCT PULocationID) FROM nytaxi.green_tripdata_non_partitoned
WHERE DATE(lpep_pickup_datetime) BETWEEN '2022-06-01' AND '2022-06-30'

--This query will process 1.12 MB when run.
SELECT COUNT(DISTINCT PULocationID) FROM nytaxi.green_tripdata_partitoned_clustered
WHERE DATE(lpep_pickup_datetime) BETWEEN '2022-06-01' AND '2022-06-30'
