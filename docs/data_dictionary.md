# Data Dictionary

## content_catalog
| column | type | meaning |
|---|---|---|
| content_id | string PK | unique content |
| title | string | title |
| genre | string | Drama/Comedy/... |
| release_date | date | original release |
| content_type | string | movie/series |
| runtime_minutes | int | duration |
| rating | string | G/PG/... |
| added_at | date | added to catalog |

## subscriptions (CDC)
| column | type |
|---|---|
| event_type | insert/update/delete |
| subscription_id | string |
| user_id | string |
| plan_tier | basic/standard/premium |
| status | active/canceled/paused |
| change_timestamp | timestamp |
| previous_plan_tier | string nullable |

## watch_events
| column | type |
|---|---|
| event_id | string PK |
| user_id | string |
| content_id | string FK -> content_catalog |
| event_type | play/pause/seek/stop |
| event_timestamp | timestamp |
| watch_duration_seconds | int |
| device_type | string |
| session_id | string |

## billing_transactions
| column | type |
|---|---|
| transaction_id | string PK |
| user_id | string |
| amount | double |
| currency | string |
| transaction_type | charge/refund |
| transaction_timestamp | timestamp |
| plan_tier | string |
