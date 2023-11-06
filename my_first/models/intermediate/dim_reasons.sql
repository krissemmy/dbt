WITH source AS (
  SELECT
    ROW_NUMBER() OVER() AS reason_id,
    {{ adapter.quote("non_payment") }},
    {{ adapter.quote("breach") }},
    {{ adapter.quote("nuisance") }},
    {{ adapter.quote("illegal_use") }},
    {{ adapter.quote("failure_to_sign_renewal") }},
    {{ adapter.quote("access_denial") }},
    {{ adapter.quote("unapproved_subtenant") }},
    {{ adapter.quote("owner_move_in") }},
    {{ adapter.quote("demolition") }},
    {{ adapter.quote("capital_improvement") }},
    {{ adapter.quote("substantial_rehab") }},
    {{ adapter.quote("ellis_act_withdrawal") }},
    {{ adapter.quote("condo_conversion") }},
    {{ adapter.quote("roommate_same_unit") }},
    {{ adapter.quote("other_cause") }},
    {{ adapter.quote("late_payments") }},
    {{ adapter.quote("lead_remediation") }},
    {{ adapter.quote("development") }},
    {{ adapter.quote("good_samaritan_ends") }}
  FROM  {{ ref("stg_eviction") }}
)
SELECT DISTINCT * FROM source





                              
 