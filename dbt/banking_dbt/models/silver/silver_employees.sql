{{ config(materialized='incremental', unique_key='employee_id', incremental_strategy='merge') }}
-- Silver employees: from Bronze employees
select
  json_extract_scalar(_doc, '$.employee_id') as employee_id,
  json_extract_scalar(_doc, '$.name') as name,
  json_extract_scalar(_doc, '$.branch_id') as branch_id,
  json_extract_scalar(_doc, '$.role') as role,
  _ingested_at as silver_loaded_at,
  _batch_id as _bronze_batch_id,
  _doc_hash as _bronze_doc_hash
from {{ source('bronze','employees') }}
{% if is_incremental() %} where _ingested_at > (select max(silver_loaded_at) from {{ this }}) {% endif %}
