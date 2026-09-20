{% snapshot dim_account_snapshot %}
{{
  config(
    target_schema='silver',
    strategy='check',
    unique_key='account_id',
    check_cols=['customer_id','branch_id','account_type','status','balance'],
    invalidate_hard_deletes=True,
  )
}}
-- SCD Type 2 for dim_account — tracks status/product changes (Architecture 7.4)
select account_id, customer_id, branch_id, account_type, status, balance from {{ ref('silver_accounts') }}
{% endsnapshot %}
