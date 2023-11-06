SELECT
    dl.address,
    COUNT(failure_to_sign_renewal) AS count_of_failure_to_sign_renewal_influence
FROM {{ ref('fact_evictions') }} fe
FULL OUTER JOIN {{ ref('dim_reasons') }} dr
ON dr.reason_id = fe.reason_id
FULL OUTER JOIN {{ ref("dim_locations") }} dl
ON fe.location_id = dl.location_id
WHERE (dr.failure_to_sign_renewal = TRUE) AND (dl.address IS NOT NULL)
GROUP BY dl.address
ORDER BY count_of_failure_to_sign_renewal_influence DESC


-- SELECT *
-- FROM non_payment1
-- FULL OUTER JOIN non_renewal
-- ON non_payment1.address = non_renewal.address
-- FULL OUTER JOIN late_payment1
-- ON non_payment1.address = late_payment1.address
-- WHERE ((dr.non_payment = TRUE) AND (dr.late_payments = True) AND (dr.failure_to_sign_renewal)) AND (dl.address IS NOT NULL)
-- GROUP BY address
-- ORDER BY count_of_non_payment_influence DESC

-- SELECT
--     dl.address,
--     COUNT(dr.non_payment) AS count_of_non_payment_influence,
--     COUNT(failure_to_sign_renewal) AS count_of_failure_to_sign_renewal_influence,
--     COUNT(late_payments) AS count_of_late_payments_influence

-- FROM {{ ref('fact_evictions') }} fe
-- FULL OUTER JOIN {{ ref('dim_reasons') }} dr
-- ON dr.reason_id = fe.reason_id
-- FULL OUTER JOIN {{ ref("dim_locations") }} dl
-- ON fe.location_id = dl.location_id
-- WHERE ((dr.non_payment = TRUE) AND (dr.late_payments = True) AND (dr.failure_to_sign_renewal)) AND (dl.address IS NOT NULL)
-- GROUP BY dl.address
-- ORDER BY count_of_non_payment_influence DESC











    -- COUNT(breach) AS count_of_breach_influence,
    -- COUNT(nuisance) AS count_of_nuisance_influence,
    -- COUNT(illegal_use) AS count_of_illegal_use_influence,
    -- COUNT(access_denial) AS count_of_access_denail_influence,
    -- COUNT(unapproved_subtenant) AS count_of_unapproved_sunbtenant_influence,
    -- COUNT(owner_move_in) AS count_of_owner_moving_in_influence,
    -- COUNT(demolition) AS count_of_demolition_influence,
    -- COUNT(capital_improvement) AS count_of_capital_improvement_influence,
    -- COUNT(substantial_rehab) AS count_of_substantial_rehab_influence,
    -- COUNT(ellis_act_withdrawal) AS count_of_ellis_act_withdrawal_influence,
    -- COUNT(condo_conversion) AS count_of_condo_conversion_influence,
    -- COUNT(roommate_same_unit) AS count_of_roomante_same_unit_influence,
    -- COUNT(other_cause) AS count_of_other_cause_influence,
    -- COUNT(lead_remediation) AS count_of_lead_remediation_influence,
    -- COUNT(development) AS count_of_development_influence,
    -- COUNT(good_samaritan_ends) count_of_good_samaritans_ends_influence
-- OR    (breach = True)
-- OR    (nuisance = True) 
-- OR    (illegal_use = True) 
-- OR    (failure_to_sign_renewal = True) 
-- OR    (access_denial = True)
-- OR    (unapproved_subtenant = True) 
-- OR    (owner_move_in = True)
-- OR    (demolition = True)
-- OR    (capital_improvement = True)
-- OR    (substantial_rehab = True)
-- OR    (ellis_act_withdrawal = True)
-- OR    (condo_conversion = True)
-- OR    (roommate_same_unit = True) 
-- OR    (other_cause = True)
-- OR    (late_payments = True) 
-- OR    (lead_remediation = True) 
-- OR    (development = True)
-- OR    (good_samaritan_ends = True)