WITH appointments AS (
    SELECT *
    FROM {{ ref('stg_appointments') }}
)

SELECT
    EXTRACT(YEAR FROM start_time) AS year,
    FLOOR(patient_age_at_appointment / 10) * 10 AS age_bin,
    AVG(recommendation_rating) AS avg_recommendation
FROM appointments
WHERE patient_sex = 'M'
GROUP BY year, age_bin
ORDER BY year, age_bin
