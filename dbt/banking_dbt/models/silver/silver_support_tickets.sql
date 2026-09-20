{{ config(materialized='incremental', unique_key='ticket_id', incremental_strategy='merge') }}
-- Silver support_tickets: from Bronze support_tickets
select
  json_extract_scalar(_doc, '$.ticket_id') as ticket_id,
  json_extract_scalar(_doc, '$.customer_id') as customer_id,
  json_extract_scalar(_doc, '$.status') as status,
  json_extract_scalar(_doc, '$.satisfaction_score') as satisfaction_score,
  _ingested_at as silver_loaded_at,
  _batch_id as _bronze_batch_id,
  _doc_hash as _bronze_doc_hash
from {{ source('bronze','support_tickets') }}
{% if is_incremental() %} where _ingested_at > (select max(silver_loaded_at) from {{ this }}) {% endif %}
