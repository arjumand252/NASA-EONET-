"""Silver layer: clean source lookup."""

from pyspark import pipelines as dp
from pyspark.sql import functions as F


@dp.materialized_view(
    comment="Clean source lookup: ingestion columns removed."
)
@dp.expect_or_drop("valid_source", "id IS NOT NULL AND title IS NOT NULL")
def sources():
    return (
        spark.read.table("bronze_sources")
        .select("id", "title")
    )
