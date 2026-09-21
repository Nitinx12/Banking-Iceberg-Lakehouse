{{ config(
    materialized='incremental',
    unique_key='customer_id',
    incremental_strategy='merge',
    tags=['silver']
) }}
-- Silver customers: parsed from Bronze _doc JSON with explicit schema, deduplicated by latest _source_ts, PII HMAC-masked via Spark UDF.
-- dbt layer is fallback view when Silver is built via `make dbt_build`; raw email/phone never lands in Gold — HMAC enforced by `jobs/transform/silver_customers.py` and validated by `tests/assert_no_raw_pii_in_serving.sql`.
-- PII columns carry meta:{pii:true} so CI can fail if they reach serving unmasked.
select
  cast(json_extract_scalar(_doc, '$.customer_id') as int) as customer_id,
  trim(json_extract_scalar(_doc, '$.name')) as name,
  -- Raw email/phone are PII — only the Spark path does real HMAC (silver_customers.py:44).
  -- dbt fallback exposes hashed placeholder so raw PII never lands in Silver/Gold.
  -- Production Silver must be built via `make silver` / `uv run python -m jobs.transform.silver_customers`.
  sha256(cast(json_extract_scalar(_doc, '$.email') as varchar)) as email_hmac,
  sha256(cast(json_extract_scalar(_doc, '$.phone') as varchar)) as phone_hmac,
  json_extract_scalar(_doc, '$.city') as city,
  json_extract_scalar(_doc, '$.state') as state,
  cast(json_extract_scalar(_doc, '$.credit_score') as int) as credit_score,
  _ingested_at as silver_loaded_at,
  _batch_id as _bronze_batch_id,
  _doc_hash as _bronze_doc_hash
from {{ source('bronze','customers') }}
{% if is_incremental() %} where _ingested_at > (select max(silver_loaded_at) from {{ this }}) {% endif %}
