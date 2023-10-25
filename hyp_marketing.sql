{{
    config(
        groups="marketing"
    )
}}

SELECT * FROM {{ ref("orders") }}