{% snapshot dim_customer_snapshot %}
{{
  config(
    target_schema='silver',
    strategy='check',
    unique_key='customer_id',
    check_cols=['name','city','state','credit_score'],
    invalidate_hard_deletes=True,
  )
}}
-- SCD Type 2 for dim_customer — tracks slowly changing customer attributes (Architecture 7.4)
select customer_id, name, city, state, credit_score from {{ ref('silver_customers') }}
{% endsnapshot %}
