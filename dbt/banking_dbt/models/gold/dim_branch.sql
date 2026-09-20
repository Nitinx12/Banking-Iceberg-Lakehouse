{{ config(materialized='table') }}
-- dim_branch Type1 (150 rows)
select cast(branch_id as int) as branch_id, branch_name, city, state, ifsc_code from {{ ref('silver_branches') }}
union all select -1, 'Unknown', 'Unknown', 'Unknown', 'Unknown'
