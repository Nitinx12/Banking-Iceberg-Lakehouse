{{ config(materialized='incremental', unique_key='card_id', incremental_strategy='merge') }}
-- Silver cards: from Bronze cards
select
  json_extract_scalar(_doc, '$.card_id') as card_id,
  json_extract_scalar(_doc, '$.customer_id') as customer_id,
  json_extract_scalar(_doc, '$.account_id') as account_id,
  json_extract_scalar(_doc, '$.card_type') as card_type,
  json_extract_scalar(_doc, '$.status') as status,
  _ingested_at as silver_loaded_at,
  _batch_id as _bronze_batch_id,
  _doc_hash as _bronze_doc_hash
from {{ source('bronze','cards') }}
{% if is_incremental() %} where _ingested_at > (select max(silver_loaded_at) from {{ this }}) {% endif %}
