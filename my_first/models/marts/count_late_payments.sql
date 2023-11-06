SELECT
    dl.address,
    COUNT(late_payments) AS count_of_late_payments_influence
FROM {{ ref('fact_evictions') }} fe
FULL OUTER JOIN {{ ref('dim_reasons') }} dr
ON dr.reason_id = fe.reason_id
FULL OUTER JOIN {{ ref("dim_locations") }} dl
ON fe.location_id = dl.location_id
WHERE (dr.late_payments = TRUE) AND (dl.address IS NOT NULL)
GROUP BY dl.address
ORDER BY count_of_late_payments_influence DESC