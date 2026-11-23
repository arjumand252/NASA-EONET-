"""Gold layer: source dimension."""

from pyspark import pipelines as dp


@dp.materialized_view(comment="Source dimension (EONET event sources).")
def dim_sources():
    return spark.read.table("sources")
