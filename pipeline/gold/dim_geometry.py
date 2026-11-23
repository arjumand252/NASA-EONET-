"""Gold layer: country dimension."""

from pyspark import pipelines as dp


@dp.materialized_view(
    comment="Country dimension: name, ISO 3166-1 Alpha-3 code, and boundary polygon."
)
def dim_geometry():
    return (
        spark.read.table("countries_ref")
        .select("name", "ISO3166-1-Alpha-3", "country_geom")
    )
