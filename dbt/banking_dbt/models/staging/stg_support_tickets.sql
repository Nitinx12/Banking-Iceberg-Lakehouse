{{ config(materialized='view') }}
-- stg_support_tickets: thin view over Bronze support_tickets (staging layer per Architecture 7.2)
select _id, _doc, _source_ts, _ingested_at, _batch_id, _doc_hash from {{ source('bronze','support_tickets') }}
