# CLAUDEXDATABRICKS

Databricks airlines pipeline, deployed with Declarative Automation Bundles (`databricks.yml`).

- Catalog: `claudexdatabricks`
- Schemas: `raw` (source tables: `bookings`, `passengers`, `airports`), `enr` (enriched output)
- Schema `cur`: curated views (`bookings_per_city` from `build_gold`, which runs after `check_quality`)
- Job: `airlines_pipeline` (`ingest_raw` -> `enrich_bookings` -> `check_quality` -> `build_gold`), serverless, daily 6 AM, PAUSED
- Scripts live in `src/`
- Writes use overwrite mode
- Never re-run `ingest_raw` unless explicitly asked
- No secrets in the repo; use Databricks CLI profiles for auth
