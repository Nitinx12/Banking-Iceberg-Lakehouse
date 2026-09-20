{{ config(materialized='incremental', unique_key='card_txn_id', incremental_strategy='merge') }}
-- Silver card_transactions: from Bronze card_transactions
select
  json_extract_scalar(_doc, '$.card_txn_id') as card_txn_id,
  json_extract_scalar(_doc, '$.card_id') as card_id,
  json_extract_scalar(_doc, '$.amount') as amount,
  json_extract_scalar(_doc, '$.is_fraud') as is_fraud,
  _ingested_at as silver_loaded_at,
  _batch_id as _bronze_batch_id,
  _doc_hash as _bronze_doc_hash
from {{ source('bronze','card_transactions') }}
{% if is_incremental() %} where _ingested_at > (select max(silver_loaded_at) from {{ this }}) {% endif %}
