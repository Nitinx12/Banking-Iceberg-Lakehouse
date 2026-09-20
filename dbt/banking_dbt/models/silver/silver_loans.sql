{{ config(materialized='incremental', unique_key='loan_id', incremental_strategy='merge') }}
-- Silver loans: from Bronze loans
select
  json_extract_scalar(_doc, '$.loan_id') as loan_id,
  json_extract_scalar(_doc, '$.customer_id') as customer_id,
  json_extract_scalar(_doc, '$.branch_id') as branch_id,
  json_extract_scalar(_doc, '$.loan_type') as loan_type,
  json_extract_scalar(_doc, '$.loan_amount') as loan_amount,
  json_extract_scalar(_doc, '$.status') as status,
  _ingested_at as silver_loaded_at,
  _batch_id as _bronze_batch_id,
  _doc_hash as _bronze_doc_hash
from {{ source('bronze','loans') }}
{% if is_incremental() %} where _ingested_at > (select max(silver_loaded_at) from {{ this }}) {% endif %}
