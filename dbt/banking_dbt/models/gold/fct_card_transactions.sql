{{ config(materialized='incremental', unique_key='card_txn_id', tags=['gold'], contract={'enforced': true}) }}
-- fct_card_transactions via Silver — FK to dim, no orphans
select
  s.card_txn_id,
  s.card_id,
  s.amount,
  s.is_fraud,
  cast(s.txn_date as date) as txn_date,
  s.silver_loaded_at as _loaded_at
from {{ ref('silver_card_transactions') }} s
{% if is_incremental() %} where s.silver_loaded_at > (select max(_loaded_at) from {{ this }}) {% endif %}
