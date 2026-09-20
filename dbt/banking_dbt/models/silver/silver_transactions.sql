{{ config(materialized='incremental', unique_key='transaction_id', incremental_strategy='merge') }}
-- Silver transactions: from Bronze transactions
select
  json_extract_scalar(_doc, '$.transaction_id') as transaction_id,
  json_extract_scalar(_doc, '$.account_id') as account_id,
  json_extract_scalar(_doc, '$.txn_type') as txn_type,
  json_extract_scalar(_doc, '$.amount') as amount,
  json_extract_scalar(_doc, '$.channel') as channel,
  _ingested_at as silver_loaded_at,
  _batch_id as _bronze_batch_id,
  _doc_hash as _bronze_doc_hash
from {{ source('bronze','transactions') }}
{% if is_incremental() %} where _ingested_at > (select max(silver_loaded_at) from {{ this }}) {% endif %}
