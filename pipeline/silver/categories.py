"""Silver layer: clean category lookup."""

from pyspark import pipelines as dp
from pyspark.sql import functions as F


@dp.materialized_view(
    comment="Clean category lookup: typed id, ingestion columns removed."
)
@dp.expect_or_drop("valid_category", "id IS NOT NULL AND title IS NOT NULL")
def categories():
    return (
        spark.read.table("bronze_categories")
        .select(F.col("id").cast("int").alias("id"), "title", "description")
    )
