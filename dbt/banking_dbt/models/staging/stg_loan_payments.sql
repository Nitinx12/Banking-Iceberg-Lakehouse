{{ config(materialized='view') }}
-- stg_loan_payments: thin view over Bronze loan_payments (staging layer per Architecture 7.2)
select _id, _doc, _source_ts, _ingested_at, _batch_id, _doc_hash from {{ source('bronze','loan_payments') }}
