{{ config(
    materialized='incremental',
    unique_key='customer_sk'
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
