{{ config(
    materialized='incremental',
    unique_key='transaction_id',
    contract={'enforced': true},
    columns=[
        {'name': 'transaction_id', 'data_type': 'int'},
        {'name': 'account_id', 'data_type': 'int'},
        {'name': 'customer_sk', 'data_type': 'string'},
        {'name': 'amount', 'data_type': 'decimal(18,2)'},
        {'name': 'txn_date', 'data_type': 'date'}
    ]
) }}
-- fct_transactions incremental, FK to dim_customer with no orphans (Architecture 7.4)
select
  cast(json_extract_scalar(_doc, '$.transaction_id') as int) as transaction_id,
  cast(json_extract_scalar(_doc, '$.account_id') as int) as account_id,
  '-1' as customer_sk, -- resolved via account→customer in Silver
  cast(json_extract_scalar(_doc, '$.amount') as decimal(18,2)) as amount,
  cast(json_extract_scalar(_doc, '$.txn_date') as date) as txn_date
from {{ source('bronze','transactions') }}
{% if is_incremental() %} where _ingested_at > (select max(txn_date) from {{ this }}) {% endif %}
