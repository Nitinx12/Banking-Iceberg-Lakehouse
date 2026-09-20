{{ config(materialized='incremental', unique_key='payment_id', incremental_strategy='merge') }}
-- Silver loan_payments: from Bronze loan_payments
select
  json_extract_scalar(_doc, '$.payment_id') as payment_id,
  json_extract_scalar(_doc, '$.loan_id') as loan_id,
  json_extract_scalar(_doc, '$.amount_paid') as amount_paid,
  _ingested_at as silver_loaded_at,
  _batch_id as _bronze_batch_id,
  _doc_hash as _bronze_doc_hash
from {{ source('bronze','loan_payments') }}
{% if is_incremental() %} where _ingested_at > (select max(silver_loaded_at) from {{ this }}) {% endif %}
