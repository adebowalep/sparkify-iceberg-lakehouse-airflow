# Correction to three provided SQL templates (verified runtime defect)

`raw.logs.ts` and `registration` are Unix **milliseconds**. The provided SQL passed them to `from_unixtime()` (seconds) without dividing by 1000.
Athena evidence (docs/athena_results/date_sanity_check_BEFORE_sql_fix.csv):

| column | min | max |
|---|---|---|
| events.ts (already used `/1000`) | 2018-11-01 | 2018-11-30 |
| events.event_date | 50805-09-16 | 50884-12-23 |
| users.registration | +50767-10-10 | +50805-06-20 |
| user_levels.changed_at | +50805-09-13 | +50882-07-01 |

Minimal fix (the unmodified Udacity files are not republished here, see README "Licensing"; the replaced expressions are listed below): convert with `CAST(x / 1000 AS TIMESTAMP)`, the same expression `events.sql` already used for `ts`. This also gives the timestamp types the data dictionary lists (previously `varchar`).
- users.sql: `from_unixtime(registration)` and `from_unixtime(ts)` became `CAST(registration / 1000 AS TIMESTAMP)` and `CAST(ts / 1000 AS TIMESTAMP)`
- user_levels.sql: both `from_unixtime(ts)` became `CAST(ts / 1000 AS TIMESTAMP)`
- events.sql: `CAST(from_unixtime(ts) AS DATE)` became `CAST(CAST(ts / 1000 AS TIMESTAMP) AS DATE)`

## After the fix (docs/athena_results/final_date_sanity_after_sql_fix.csv)
| column | min | max |
|---|---|---|
| users.registration | 2018-10-19 | 2018-11-01 |
| user_levels.changed_at | 2018-11-01 | 2018-11-29 |
| events.ts | 2018-11-01 | 2018-11-30 |
| events.event_date | 2018-11-01 | 2018-11-30 |

Row counts of every table are unchanged by the fix. `transactions.users.registration` / `last_seen_at` are now `timestamp`.

## Later changes (made after the AWS verification above; verified locally only)
These changes were made after the Athena evidence in this repository was produced and have **not** been re-run on AWS. See `docs/engineering_notes.md`.

- `events.sql`: candidate songs are now joined to their artist and must match the title **and** the artist name (previously only the title was used, so a play could be attached to another artist's song). Ties resolve deterministically (closest duration, then lowest `version_id`, then lowest `song_id`).
- `users.sql`, `user_levels.sql`, `songs.sql`, `artists.sql`: `ROW_NUMBER()` ordering made total so ties never depend on row order.
