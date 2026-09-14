# Data Dictionary

Column-level reference for all 11 sources, as emitted by `generator/`.
Bronze adds `_ingested_at` (timestamp) and `_source_file` (path) to every table.
Types shown are the intended Silver types — sources land as JSON strings and
are cast during cleaning. Pipeline overview: [`SCHEMA.md`](SCHEMA.md).

## watch_events (event stream)

| Column | Type | Description |
|---|---|---|
| `event_id` | string PK | `evt_<uuid>` |
| `user_id` | string | `user_{i:06d}`, shared ID space |
| `content_id` | string FK | `ct_…` → content_catalog |
| `event_type` | string | play / pause / seek / stop |
| `event_timestamp` | timestamp | event time (5-day window) |
| `watch_duration_seconds` | int | 0–7200, nullable |
| `device_type` | string | tv / mobile / web / tablet / console, nullable |
| `session_id` | string | `sess_<uuid>` |

## subscriptions_cdc (CDC changelog)

| Column | Type | Description |
|---|---|---|
| `event_type` | string | insert / update / delete |
| `subscription_id` | string PK | `sub_<uuid>` |
| `user_id` | string | shared ID space |
| `plan_tier` | string | basic / standard / premium (SCD2-tracked) |
| `status` | string | active / canceled / paused (SCD2-tracked) |
| `change_timestamp` | timestamp | CDC event time |
| `previous_plan_tier` | string | nullable back-compat field |

## content_catalog (batch dim)

| Column | Type | Description |
|---|---|---|
| `content_id` | string PK | `ct_<uuid>` |
| `title` | string | Faker catch-phrase |
| `genre` | string | nullable messiness |
| `release_date` | date | |
| `content_type` | string | movie / series / documentary / short |
| `runtime_minutes` | int | 22–210 |
| `rating` | string | MPAA-style (PG-13 …), nullable |
| `added_at` | timestamp | release + 1–30 days |

## billing (event stream)

| Column | Type | Description |
|---|---|---|
| `transaction_id` | string PK | `txn_<uuid>`; MERGE key in Silver |
| `user_id` | string | shared ID space |
| `amount` | double | plan price ± jitter; negative for refunds |
| `currency` | string | USD / EUR / GBP, nullable |
| `transaction_type` | string | charge / refund |
| `transaction_timestamp` | timestamp | 90-day window |
| `plan_tier` | string | basic / standard / premium |

## devices_cdc (CDC changelog)

| Column | Type | Description |
|---|---|---|
| `event_type` | string | insert / update / delete |
| `device_id` | string PK | SCD2 key |
| `user_id` | string | shared ID space |
| `device_type` | string | tv / mobile / web / tablet / console (tracked) |
| `os_family` | string | e.g. android / ios / tvos (tracked) |
| `os_version` | string | nullable messiness (tracked) |
| `app_version` | string | (tracked) |
| `is_primary` | boolean | one primary device per user (tracked) |
| `change_timestamp` | timestamp | CDC event time |

## profiles (batch dim)

| Column | Type | Description |
|---|---|---|
| `profile_id` | string PK | `prof_<uuid>` |
| `user_id` | string | 1 user → N household profiles |
| `profile_name` | string | |
| `is_kids` | boolean | ~20% kids profiles |
| `language` | string | en / es / fr / de / hi; `xx` = invalid messiness |
| `avatar` | string | avatar id |
| `created_at` | timestamp | up to 700 days back |

## promotions (batch dim)

| Column | Type | Description |
|---|---|---|
| `promo_code` | string PK | `PROMO26_{i:06X}`, shared deterministic space |
| `description` | string | |
| `discount_pct` | int | 0–100; out-of-range messiness |
| `starts_at` | timestamp | validity window start |
| `ends_at` | timestamp | end; end-before-start messiness |
| `eligible_plan_tier` | string | nullable = all tiers |
| `max_redemptions` | int | cap |
| `is_active` | boolean | ~90% active |

## promotion_redemptions (bridge / factless fact)

| Column | Type | Description |
|---|---|---|
| `redemption_id` | string PK | `red_<uuid>` |
| `promo_code` | string FK | → promotions; `PROMO26_DOESNOTEXIST` = orphan messiness |
| `user_id` | string | shared ID space |
| `subscription_id` | string | |
| `redeemed_at` | timestamp | 180-day window |
| `billing_period_start` | date | month start of redemption |

## support_tickets (semi-structured)

| Column | Type | Description |
|---|---|---|
| `ticket_id` | string PK | `tkt_<uuid>` |
| `user_id` | string | shared ID space |
| `created_at` / `updated_at` | timestamp | |
| `status` | string | open / pending / resolved / closed |
| `channel` | string | chat / email / phone |
| `csat_score` | int | 1–5, nullable; 0/9 = out-of-range messiness |
| `payload` | string (JSON) | nested `{category, subcategory, device{device_type, os}, resolution_notes}`; malformed JSON messiness → Silver flattens via `from_json` |

## cdn_stream_logs (high-volume telemetry)

| Column | Type | Description |
|---|---|---|
| `log_id` | string PK | `log_<uuid>` |
| `session_id` | string | groups 5–30 heartbeat logs |
| `user_id` / `content_id` | string | shared ID spaces |
| `device_type` | string | |
| `event_timestamp` | timestamp | 5-day window; >30-min gaps split sessions |
| `bitrate_kbps` | int | 500–15000, nullable |
| `rebuffer_ms` | int | ≥ 0 |
| `startup_ms` | int | first log of session only, else null |
| `cdn_edge` | string | edge-us-east … edge-ap-south |
| `playback_position_seconds` | double | advances with wall clock |

## content_ratings (upsert stream)

| Column | Type | Description |
|---|---|---|
| `rating_id` | string PK | `rt_<uuid>`; NOT the Silver natural key |
| `user_id` + `content_id` | string | Silver MERGE key (latest-wins) |
| `rating` | int | 1–5; 0/6 = out-of-range messiness |
| `review_text` | string | nullable (~40% filled) |
| `rated_at` / `updated_at` | timestamp | `updated_at` decides the winner |

## Silver-added columns (all tables)

| Column | Description |
|---|---|
| `quarantine_reason` | on `silver.quarantine` rows only; `;`-joined reasons |
| `_q_date` | quarantine partition date |
| `effective_date` / `end_date` / `is_current` | SCD2 bookkeeping (subscriptions_scd2, devices_scd2) |
| `session_number` | gap-based session index (cdn_stream_sessions) |
| `event_date` / `created_date` / `session_date` | partition columns |
