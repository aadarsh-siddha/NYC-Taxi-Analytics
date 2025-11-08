{% set start_year = var('start_year', 2024) %}
{% set end_year_exclusive = var('end_year_exclusive', 2026) %}

WITH date_spine AS (
    {{ dbt_utils.date_spine(
        datepart="day",
        start_date="to_date('" ~ start_year ~ "-01-01')",
        end_date="to_date('" ~ end_year_exclusive ~ "-01-01')"
    ) }}
)

SELECT
    REPLACE(to_varchar(date_day), '-','') AS date_sk,
    date_day,
    EXTRACT(YEAR FROM date_day)    AS year,
    EXTRACT(MONTH FROM date_day)   AS month,
    EXTRACT(DAY FROM date_day)     AS day,
    EXTRACT(DAYOFWEEK FROM date_day) AS day_of_week
FROM date_spine