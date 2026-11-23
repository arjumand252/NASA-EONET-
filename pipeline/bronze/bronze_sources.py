"""Bronze layer: latest snapshot of the EONET sources reference list."""

import uuid
from datetime import datetime, timezone

from pyspark import pipelines as dp

from eonet_client import fetch_endpoint


@dp.materialized_view(
    comment=(
        "Latest snapshot of the EONET event sources reference list, "
        "with ingestion metadata."
    )
)
@dp.expect("has_title", "title IS NOT NULL")
def bronze_sources():
    payload = fetch_endpoint("sources")
    ingested_at = datetime.now(timezone.utc)
    rows = [
        {
            "id": s["id"],
            "title": s["title"],
            "ingestion_id": str(uuid.uuid4()),
            "ingestion_timestamp": ingested_at,
        }
        for s in payload.get("sources", [])
    ]
    return spark.createDataFrame(rows)
