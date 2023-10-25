{% snapshot snapshot_distribution_centre %}

{{
    config(
      target_schema='alt_data',
      unique_key='id',
      strategy='check',
      check_cols=['name', 'latitude', 'longitude']
    )
}}

SELECT * FROM {{ ref("distribution_center") }}

{% endsnapshot %}