"""Gold layer: event facts."""

from pyspark import pipelines as dp
from pyspark.databricks.sql import functions as dbf


@dp.materialized_view(
    comment="Event facts keyed to dim_points via ST_Equals; geometry payloads removed."
)
@dp.expect_or_fail("valid_event_id", "id IS NOT NULL")
@dp.expect("valid_dates", "min_date IS NOT NULL AND max_date IS NOT NULL AND event_duration >= 0")
def fact_events():
    events = spark.read.table("events_geo")
    points = spark.read.table("dim_points")
    return (
        events
        .join(
            points,
            on=dbf.st_equals(events["avg_point"], points["point_geometry"]),
            how="left",
        )
        .drop("geometry", "avg_point", "country_geom")
    )
