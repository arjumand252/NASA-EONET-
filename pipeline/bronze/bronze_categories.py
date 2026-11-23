"""Bronze layer: latest snapshot of the EONET categories reference list."""

import uuid
from datetime import datetime, timezone

from pyspark import pipelines as dp

from eonet_client import fetch_endpoint


@dp.materialized_view(
    comment=(
        "Latest snapshot of the EONET categories reference list, "
        "with ingestion metadata."
    )
)
@dp.expect("has_title", "title IS NOT NULL")
def bronze_categories():
    payload = fetch_endpoint("categories")
    ingested_at = datetime.now(timezone.utc)
    rows = [
        {
            "id": c["id"],
            "title": c["title"],
            "description": c.get("description") or "",
            "ingestion_id": str(uuid.uuid4()),
            "ingestion_timestamp": ingested_at,
        }
        for c in payload.get("categories", [])
    ]
    return spark.createDataFrame(rows)
