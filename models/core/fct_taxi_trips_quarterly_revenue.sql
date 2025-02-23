{{ config(materialized='table') }}


WITH quarterly_revenue AS (
    SELECT
        service_type,
        EXTRACT(YEAR FROM {{ dbt.date_trunc("year", "pickup_datetime") }} ) AS year,
        EXTRACT(QUARTER FROM {{ dbt.date_trunc("quarter", "pickup_datetime") }}) AS quarter,
        SUM(total_amount) AS revenue
   FROM {{ ref('fact_trips') }}
    GROUP BY 1, 2, 3
),
quarterly_growth AS (
    SELECT
        cur_year.service_type,
        cur_year.year,
        cur_year.quarter,
        cur_year.revenue AS current_revenue,
        prev_year.revenue AS prev_year_revenue,
        cur_year.revenue/prev_year.revenue- 1 AS yoy_growth
    FROM quarterly_revenue cur_year
    LEFT JOIN quarterly_revenue prev_year
        ON cur_year.service_type = prev_year.service_type
        AND cur_year.year = prev_year.year + 1
        AND cur_year.quarter = prev_year.quarter
)
SELECT * FROM quarterly_growth
ORDER BY 
service_type,
year,
quarter


