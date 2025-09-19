
-- Average duration of consultations by hour

SELECT 
    EXTRACT(HOUR FROM start_time::timestamp) AS hour_of_day,
    AVG(duration_minutes) AS avg_duration
FROM appointments_clean
GROUP BY hour_of_day
ORDER BY hour_of_day;


-- Average recommendation rating for males by year and age bin


SELECT
    EXTRACT(YEAR FROM start_time) AS year,
    FLOOR(patient_age_at_appointment / 10) * 10 AS age_bin,
    AVG(recommendation_rating) AS avg_recommendation
FROM appointments_clean
WHERE patient_sex = 'M'
GROUP BY year, age_bin
ORDER BY year, age_bin;


-- Average recommendation rating for patients aged 20-30 by hour

WITH patients_20_30 AS (
    SELECT 
        EXTRACT(HOUR FROM start_time::timestamp) AS hour_of_day,
        recommendation_rating
    FROM appointments_clean
    WHERE patient_age_at_appointment BETWEEN 20 AND 30
)
SELECT 
    hour_of_day,
    AVG(recommendation_rating) AS avg_recommendation
FROM patients_20_30
GROUP BY hour_of_day
ORDER BY hour_of_day;
