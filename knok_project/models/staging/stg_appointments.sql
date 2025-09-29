SELECT
    appointment_id,                       -- numeric
    tenant,                               -- always 'knok'
    created_at::timestamp AS created_at,  -- timestamp
    updated_at::timestamp AS updated_at,  -- timestamp
    start_time::timestamp AS start_time,  -- timestamp
    end_time::timestamp AS end_time,      -- timestamp
    started_at::timestamp AS started_at,  -- timestamp
    finished_at::timestamp AS finished_at,-- timestamp
    realization_sla,                      -- minutes
    duration_minutes,                     -- minutes
    delay_minutes,                        -- minutes
    CASE 
        WHEN status IN ('finished', 'no-show') THEN status
        ELSE 'unknown'
    END AS status,                        -- 'finished', 'no-show', 'unknown'
    CASE 
        WHEN service_type IN ('phone', 'remote') THEN service_type
        ELSE 'unknown'
    END AS service_type,                  -- 'phone', 'remote', unknown
    CASE WHEN appointment_helpful = 'True' THEN TRUE
         WHEN appointment_helpful = 'False' THEN FALSE
         ELSE NULL
    END AS appointment_helpful,           -- boolean
    appointment_classification,
    recommendation_rating,
    icd_code1,
    icd_code2,
    icd_code3,
    patient_age_at_appointment,
    CASE 
        WHEN patient_sex IN ('F', 'M') THEN patient_sex
        ELSE 'U'
    END AS patient_sex,                   -- 'F', 'M', unknown
    in_person_appointment_evaluation,
    CASE WHEN recurrence_24hours = 'yes' THEN TRUE
         WHEN recurrence_24hours = 'no' THEN FALSE
         ELSE NULL
    END AS recurrence_24hours,
    CASE WHEN recurrence_48hours = 'yes' THEN TRUE
         WHEN recurrence_48hours = 'no' THEN FALSE
         ELSE NULL
    END AS recurrence_48hours,
    CASE WHEN recurrence_72hours = 'yes' THEN TRUE
         WHEN recurrence_72hours = 'no' THEN FALSE
         ELSE NULL
    END AS recurrence_72hours,
    CASE WHEN recurrence_7days = 'yes' THEN TRUE
         WHEN recurrence_7days = 'no' THEN FALSE
         ELSE NULL
    END AS recurrence_7days,
    process_date::date AS process_date
FROM {{ source('raw_appointments', 'appointments_clean') }}
