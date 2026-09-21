{{ config(
    materialized='incremental',
    unique_key='transaction_id',
    tags=['gold']
) }}
-- fct_transactions incremental via Silver (not Bronze) — FK to dim_account, no orphans (Architecture 7.4)
select
  s.transaction_id,
  s.account_id,
  coalesce(d.account_sk, '-1') as account_sk,
  s.amount,
  cast(s.created_at as date) as txn_date,
  s.channel,
  s.silver_loaded_at as _loaded_at
from {{ ref('silver_transactions') }} s
left join {{ ref('dim_account') }} d on s.account_id = d.account_id and d.is_current
{% if is_incremental() %} where s.silver_loaded_at > (select max(_loaded_at) from {{ this }}) {% endif %}
