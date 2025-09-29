WITH appointments AS (
    SELECT *
    FROM {{ ref('stg_appointments') }}
),
patients_20_30 AS (
    SELECT 
        EXTRACT(HOUR FROM start_time) AS hour_of_day,
        recommendation_rating
    FROM appointments
    WHERE patient_age_at_appointment BETWEEN 20 AND 30
)
SELECT 
    hour_of_day,
    AVG(recommendation_rating) AS avg_recommendation
FROM patients_20_30
GROUP BY hour_of_day
ORDER BY hour_of_day
