"""Bronze layer: latest snapshot of EONET natural events."""

import json
import uuid
from datetime import datetime, timezone

from pyspark import pipelines as dp

from eonet_client import EVENTS_LOOKBACK_DAYS, fetch_endpoint


@dp.materialized_view(
    comment=(
        f"Latest snapshot of EONET natural events from the last {EVENTS_LOOKBACK_DAYS} days, "
        "with ingestion metadata."
    )
)
@dp.expect_or_fail("valid_event", "id IS NOT NULL AND title IS NOT NULL")
def bronze_events():
    payload = fetch_endpoint("events", {"days": EVENTS_LOOKBACK_DAYS})
    ingested_at = datetime.now(timezone.utc)
    rows = [
        {
            "id": e["id"],
            "title": e["title"],
            "description": e.get("description") or "",
            # geometries are stored as a JSON string: the raw dicts mix
            # str/list values that Spark cannot infer as a struct.
            "geometries": json.dumps(e.get("geometries", [])),
            "categories": e.get("categories", []),
            "sources": e.get("sources", []),
            "ingestion_id": str(uuid.uuid4()),
            "ingestion_timestamp": ingested_at,
        }
        for e in payload.get("events", [])
    ]
    return spark.createDataFrame(rows)
