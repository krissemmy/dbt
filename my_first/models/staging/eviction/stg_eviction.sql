with source as (

    select * from {{ source('alt_data', 'eviction_data_table') }}

),

renamed as (

    select
        eviction_id,
        address,
        city,
        state,
        zip,
        file_date,
        non_payment,
        breach,
        nuisance,
        illegal_use,
        failure_to_sign_renewal,
        access_denial,
        unapproved_subtenant,
        owner_move_in,
        demolition,
        capital_improvement,
        substantial_rehab,
        ellis_act_withdrawal,
        condo_conversion,
        roommate_same_unit,
        other_cause,
        late_payments,
        lead_remediation,
        development,
        good_samaritan_ends,
        supervisor_district,
        neighborhood,
        client_location,
        shape,
        constraints_date

    from source

)


select * from renamed

