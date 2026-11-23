"""Silver layer: clean, validated natural events with parsed geometries."""

from pyspark import pipelines as dp
from pyspark.sql import functions as F
from pyspark.sql.types import (
    ArrayType,
    DoubleType,
    StringType,
    StructField,
    StructType,
)

# EONET geometries are GeoJSON {date, type, coordinates} objects. In v2.1
# every observed geometry is a Point with flat [lon, lat] coordinates.
GEOMS_SCHEMA = ArrayType(
    StructType([
        StructField("date", StringType()),
        StructField("type", StringType()),
        StructField("coordinates", ArrayType(DoubleType())),
    ])
)


@dp.materialized_view(
    comment=(
        "Clean events: category/source ids, min/max observation dates, "
        "parsed point geometries, and event duration in days."
    )
)
@dp.expect_or_fail("valid_event_id", "id IS NOT NULL")
@dp.expect("geometry_parsed", "size(geometry) > 0")
def events():
    return (
        spark.read.table("bronze_events")
        .withColumn(
            "category_id",
            F.transform(F.col("categories"), lambda c: c.getItem("id")),
        )
        .withColumn(
            "source_id",
            F.transform(F.col("sources"), lambda s: s.getItem("id")),
        )
        .withColumn("geoms", F.from_json("geometries", GEOMS_SCHEMA))
        .withColumn(
            "min_date",
            F.array_min(
                F.transform(F.col("geoms"), lambda g: F.to_timestamp(g.getItem("date")))
            ),
        )
        .withColumn(
            "max_date",
            F.array_max(
                F.transform(F.col("geoms"), lambda g: F.to_timestamp(g.getItem("date")))
            ),
        )
        .withColumn(
            "geometry",
            F.expr(
                "transform(geoms, g -> st_geomfromgeojson("
                "to_json(named_struct('type', g.type, 'coordinates', g.coordinates))))"
            ),
        )
        .withColumn("event_duration", F.datediff(F.col("max_date"), F.col("min_date")))
        .drop(
            "categories", "sources", "geometries", "geoms",
            "ingestion_id", "ingestion_timestamp",
        )
    )
