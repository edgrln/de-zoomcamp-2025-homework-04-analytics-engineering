{{ config(materialized="view") }}

SELECT 
    -- timestamps
    cast(pickup_datetime as timestamp) as pickup_datetime
    ,cast(dropoff_datetime as timestamp) as dropoff_datetime
    ,dispatching_base_num
    ,cast(PUlocationID as int64) as PUlocationID
    ,cast(DOlocationID as int64) as DOlocationID
    ,SR_Flag
    ,Affiliated_base_number


from {{ source("staging", "fhv_tripdata") }}
where dispatching_base_num is not null 


    -- dbt build --select <model_name> --vars '{'is_test_run': 'false'}'
    {% if var("is_test_run", default=true) %}

    limit 100
    {% endif %}
