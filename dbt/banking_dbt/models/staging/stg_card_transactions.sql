{{ config(materialized='view') }}
-- stg_card_transactions: thin view over Bronze card_transactions (staging layer per Architecture 7.2)
select _id, _doc, _source_ts, _ingested_at, _batch_id, _doc_hash from {{ source('bronze','card_transactions') }}
