{{ config(
    materialized='incremental',
    unique_key='customer_sk',
    contract={'enforced': true},
    columns=[
        {'name': 'customer_sk', 'data_type': 'string', 'constraints': [{'type': 'not_null'}, {'type': 'unique'}]},
        {'name': 'customer_id', 'data_type': 'int'},
        {'name': 'name', 'data_type': 'string'},
        {'name': 'is_current', 'data_type': 'boolean'},
        {'name': 'valid_from', 'data_type': 'timestamp'},
        {'name': 'valid_to', 'data_type': 'timestamp'}
    ]
) }}
-- Gold dim_customer SCD2 with surrogate key + unknown member -1 (Architecture 7.4)
-- Surrogate: {{ dbt_utils.generate_surrogate_key(['customer_id', 'valid_from']) }} pattern
with dedup as (
  select customer_id, name, max(silver_loaded_at) as valid_from from {{ ref('silver_customers') }} group by 1,2
)
select
  {{ dbt_utils.generate_surrogate_key(['customer_id', 'valid_from']) }} as customer_sk,
  customer_id, name, true as is_current, valid_from, cast(null as timestamp) as valid_to
from dedup
union all
select '-1', -1, 'Unknown', false, cast('1900-01-01' as timestamp), cast(null as timestamp)
