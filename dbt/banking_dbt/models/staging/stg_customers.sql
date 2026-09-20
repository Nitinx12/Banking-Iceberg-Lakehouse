{{ config(materialized='view') }}
-- stg_customers: thin view over Silver typed customers (populated by jobs/transform Silver jobs parsing _doc)
select _id, _doc, _source_ts, _ingested_at, _batch_id from {{ source('bronze','customers') }}
