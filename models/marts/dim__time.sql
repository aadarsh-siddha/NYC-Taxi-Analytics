-- generate one second per day
WITH spine AS (
  {{ dbt_utils.date_spine(
      datepart="second",
      start_date="to_timestamp('2000-01-01 00:00:00')",
      end_date="to_timestamp('2000-01-02 00:00:00')"
  ) }}
)
SELECT
  {{ dbt_utils.generate_surrogate_key(['date_second']) }}     AS time_sk,
  TO_CHAR(date_second, 'HH24:MI:SS')                AS time_txt,
  EXTRACT(HOUR   FROM date_second)                  AS hour,
  EXTRACT(MINUTE FROM date_second)                  AS minute,
  EXTRACT(SECOND FROM date_second)                  AS second,
  CASE
    WHEN EXTRACT(HOUR FROM date_second) BETWEEN 5 AND 10 THEN 'Morning'
    WHEN EXTRACT(HOUR FROM date_second) BETWEEN 11 AND 15 THEN 'Afternoon'
    WHEN EXTRACT(HOUR FROM date_second) BETWEEN 16 AND 20 THEN 'Evening'
    ELSE 'Night'
  END AS daypart,
FROM spine
