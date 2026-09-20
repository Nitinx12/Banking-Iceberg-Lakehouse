{{ config(materialized='view') }}
-- stg_accounts: thin view over Bronze accounts (staging layer per Architecture 7.2)
select _id, _doc, _source_ts, _ingested_at, _batch_id, _doc_hash from {{ source('bronze','accounts') }}
