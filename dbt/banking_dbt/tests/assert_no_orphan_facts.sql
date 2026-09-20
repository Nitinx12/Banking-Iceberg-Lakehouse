-- Singular test: no orphan facts per Architecture 7.4
select f.transaction_id
from {{ ref('fct_transactions') }} f
left join {{ ref('dim_account') }} d on f.account_sk = d.account_sk
where d.account_sk is null and f.account_sk != '-1'
