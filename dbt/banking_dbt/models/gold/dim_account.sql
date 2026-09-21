{{ config(materialized='incremental', unique_key='account_sk', contract={'enforced': true}) }}
-- dim_account SCD2
with dedup as (
  select account_id, customer_id, branch_id, account_type, status, max(silver_loaded_at) as valid_from from {{ ref('silver_accounts') }} group by 1,2,3,4,5
)
select {{ dbt_utils.generate_surrogate_key(['account_id', 'valid_from']) }} as account_sk, account_id, customer_id, branch_id, account_type, status, true as is_current, valid_from, cast(null as timestamp) as valid_to from dedup
union all select '-1', -1, -1, -1, 'Unknown', 'Unknown', false, cast('1900-01-01' as timestamp), cast(null as timestamp)
