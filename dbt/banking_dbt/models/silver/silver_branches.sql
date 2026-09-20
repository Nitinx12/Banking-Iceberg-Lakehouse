{{ config(materialized='incremental', unique_key='branch_id', incremental_strategy='merge') }}
-- Silver branches: from Bronze branches
select
  json_extract_scalar(_doc, '$.branch_id') as branch_id,
  json_extract_scalar(_doc, '$.branch_name') as branch_name,
  json_extract_scalar(_doc, '$.city') as city,
  json_extract_scalar(_doc, '$.state') as state,
  json_extract_scalar(_doc, '$.ifsc_code') as ifsc_code,
  _ingested_at as silver_loaded_at,
  _batch_id as _bronze_batch_id,
  _doc_hash as _bronze_doc_hash
from {{ source('bronze','branches') }}
{% if is_incremental() %} where _ingested_at > (select max(silver_loaded_at) from {{ this }}) {% endif %}
