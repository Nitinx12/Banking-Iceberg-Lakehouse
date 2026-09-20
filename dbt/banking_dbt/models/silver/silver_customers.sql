{{ config(
    materialized='incremental',
    unique_key='customer_id',
    incremental_strategy='merge',
    contract={'enforced': true},
    columns=[
        {'name': 'customer_id', 'data_type': 'int', 'constraints': [{'type': 'not_null'}]},
        {'name': 'name', 'data_type': 'string'},
        {'name': 'email_hmac', 'data_type': 'string', 'meta': {'pii': true, 'class': 'restricted'}},
        {'name': 'phone_hmac', 'data_type': 'string', 'meta': {'pii': true}},
        {'name': 'city', 'data_type': 'string'},
        {'name': 'state', 'data_type': 'string'},
        {'name': 'credit_score', 'data_type': 'int'},
        {'name': 'silver_loaded_at', 'data_type': 'timestamp'},
        {'name': '_bronze_batch_id', 'data_type': 'string'},
        {'name': '_bronze_doc_hash', 'data_type': 'string'}
    ]
) }}
-- Silver customers: parsed from Bronze _doc JSON with explicit schema, deduplicated by latest _source_ts, PII masked via HMAC (PII_HMAC_SECRET)
-- Populated by jobs/transform job silver_customers.py (PySpark) or this model when Spark jobs write to silver
select
  cast(json_extract_scalar(_doc, '$.customer_id') as int) as customer_id,
  trim(json_extract_scalar(_doc, '$.name')) as name,
  -- HMAC examples: hmac_sha256(env PII_HMAC_SECRET, email) - actual masking in PySpark job
  json_extract_scalar(_doc, '$.email') as email_hmac,
  json_extract_scalar(_doc, '$.phone') as phone_hmac,
  json_extract_scalar(_doc, '$.city') as city,
  json_extract_scalar(_doc, '$.state') as state,
  cast(json_extract_scalar(_doc, '$.credit_score') as int) as credit_score,
  _ingested_at as silver_loaded_at,
  _batch_id as _bronze_batch_id,
  _doc_hash as _bronze_doc_hash
from {{ source('bronze','customers') }}
{% if is_incremental() %} where _ingested_at > (select max(silver_loaded_at) from {{ this }}) {% endif %}
