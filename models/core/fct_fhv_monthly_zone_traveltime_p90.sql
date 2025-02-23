{{ config(materialized='table') }}

WITH filtered_trips AS (
SELECT
dispatching_base_num
,year
,month
,pickup_datetime
,dropOff_datetime
,PUlocationID
,DOlocationID
,SR_Flag
,Affiliated_base_number
,pickup_borough
,pickup_zone
,dropoff_borough
,dropoff_zone
,TIMESTAMP_DIFF(dropOff_datetime, pickup_datetime, SECOND) as trip_duration
FROM
{{ ref('dim_fhv_trips') }})
,percentiles AS (
    SELECT
        *,
        PERCENTILE_CONT(trip_duration, 0.90) OVER (PARTITION BY year, month, PUlocationID, DOlocationID) AS p90
    FROM filtered_trips
)
,ranked_trips as (
SELECT DISTINCT *,
  DENSE_RANK() OVER (
            PARTITION BY year, month, PUlocationID 
            ORDER BY p90 DESC
        ) AS rank
FROM percentiles
WHERE year = 2019 AND month =11 and pickup_zone in ('Newark Airport', 'SoHo', 'Yorkville East'))

SELECT pickup_zone, dropoff_zone --, p90, rank
FROM ranked_trips
--order by p90 desc, rank asc
WHERE rank = 2
group by 1,2;





