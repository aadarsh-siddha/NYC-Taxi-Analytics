SELECT * FROM {{ ref('bronze__yellow_trips') }}
WHERE DATE_PART('year',pickup_datetime) != 2024 AND DATE_PART('year',dropoff_datetime) != 2024 