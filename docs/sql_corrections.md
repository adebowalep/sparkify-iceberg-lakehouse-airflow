# Correction to three provided SQL templates (verified runtime defect)

`raw.logs.ts` and `registration` are Unix **milliseconds**. The provided SQL passed them to `from_unixtime()` (seconds) without dividing by 1000.
Athena evidence (docs/athena_results/date_sanity_check_BEFORE_sql_fix.csv):

| column | min | max |
|---|---|---|
| events.ts (already used `/1000`) | 2018-11-01 | 2018-11-30 |
| events.event_date | 50805-09-16 | 50884-12-23 |
| users.registration | +50767-10-10 | +50805-06-20 |
| user_levels.changed_at | +50805-09-13 | +50882-07-01 |

Minimal fix (original files kept in docs/original_sql/): convert with `CAST(x / 1000 AS TIMESTAMP)`, the same expression `events.sql` already used for `ts`. This also gives the timestamp types the data dictionary lists (previously `varchar`).
- users.sql: `registration`, `event_ts`
- user_levels.sql: both `changed_at` expressions
- events.sql: `event_date` = `CAST(CAST(ts / 1000 AS TIMESTAMP) AS DATE)`

## After the fix (docs/athena_results/final_date_sanity_after_sql_fix.csv)
| column | min | max |
|---|---|---|
| users.registration | 2018-10-19 | 2018-11-01 |
| user_levels.changed_at | 2018-11-01 | 2018-11-29 |
| events.ts | 2018-11-01 | 2018-11-30 |
| events.event_date | 2018-11-01 | 2018-11-30 |

Row counts of every table are unchanged by the fix. `transactions.users.registration` / `last_seen_at` are now `timestamp`.
