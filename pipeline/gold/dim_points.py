"""Gold layer: distinct event centroid points."""

from pyspark import pipelines as dp
from pyspark.sql import functions as F


@dp.materialized_view(comment="Distinct event centroid points with a surrogate point_id.")
def dim_points():
    return (
        spark.read.table("events_geo")
        # Spark cannot hash the GEOMETRY type, so dedup on its WKT text.
        .select(F.expr("ST_AsText(avg_point)").alias("point_wkt"))
        .distinct()
        .withColumn(
            "point_geometry",
            F.expr("ST_SetSRID(st_geomfromwkt(point_wkt), 4326)"),
        )
        .withColumn("point_id", F.expr("uuid()"))
        .select("point_id", "point_geometry")
    )
