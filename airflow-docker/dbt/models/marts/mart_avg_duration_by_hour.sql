WITH appointments AS (
    SELECT *
    FROM {{ ref('stg_appointments') }}
)

SELECT 
    EXTRACT(HOUR FROM start_time) AS hour_of_day,
    AVG(duration_minutes) AS avg_duration
FROM appointments
GROUP BY hour_of_day
ORDER BY hour_of_day
