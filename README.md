# NASA-EONET — Natural Events Medallion Data Pipeline

An end-to-end data engineering project that ingests [NASA's Earth Observatory
Natural Event Tracker (EONET)](https://eonet.gsfc.nasa.gov/) — wildfires, storms,
volcanoes, icebergs and other natural events — into a **medallion-architecture
pipeline** on Databricks, published to **Unity Catalog**.

The project has two builds of the same pipeline:

1. **Exploratory notebooks** (`01`–`05`) — the original interactive build, notebook by notebook.
2. **Lakeflow Spark Declarative Pipeline (SDP)** (`pipeline/`) — the productionized version:
   declarative, refreshable datasets with data-quality expectations, running on serverless compute.

---

## Architecture

The declarative pipeline refreshes every dataset on each update. EONET is a snapshot-style
open-access REST API (no key required), so the bronze layer materializes the latest snapshot
with ingestion metadata, and every downstream layer derives from it.

![Pipeline architecture](Screenshot%202026-11-24%20192752.png)

### Layer responsibilities

| Layer | Datasets | What happens |
| --- | --- | --- |
| Bronze | `bronze_events`, `bronze_categories`, `bronze_sources` | Fetch the latest API snapshot (events: last 365 days) with `ingestion_id` / `ingestion_timestamp` provenance. `geometries` are kept as a JSON string (the raw dicts mix types Spark cannot infer as a struct). |
| Silver | `categories`, `sources`, `events`, `events_geo` | Type casts and ingestion-column removal; parse `geometries` JSON into typed structs (`from_json`) and build `array<geometry>` with `st_geomfromgeojson`; extract `category_id` / `source_id` arrays, `min_date` / `max_date` / `event_duration`; compute each event's centroid (`avg_point`) and attribute a country via a spatial `ST_Within` join against a countries GeoJSON reference. |
| Gold | `dim_categories`, `dim_sources`, `dim_points`, `dim_geometry`, `fact_events` | Star schema: dimensions for categories, sources, distinct centroid points (surrogate `point_id` via `ST_AsText` dedup), and countries; `fact_events` joins events to `dim_points` with `ST_Equals` and drops heavy geometry payloads. |

### Data quality (SDP expectations)

* `bronze_events` / `fact_events`: **fail** on missing event id.
* `events`: warn if no geometry parses (`size(geometry) > 0`).
* `events_geo`: warn when a country cannot be attributed (legitimate for open-ocean events — ~2% of events).
* `categories` / `sources`: **drop** rows without id or title.

---

## Discoveries (EONET + Spark)

* EONET v2.1 limit=0 returns **zero** events (not "no limit") — the client omits the parameter instead.
* geometries must be stored as a JSON string in bronze: the raw dicts mix str and list values.
* Every EONET v2.1 geometry observed is a Point with flat [lon, lat] coordinates.
* size() returns -1 for NULL arrays; aggregate() needs cast(0.0 as double) to match a DOUBLE accumulator.
* Spark cannot hash or DISTINCT the GEOMETRY type — dedup on ST_AsText instead.
