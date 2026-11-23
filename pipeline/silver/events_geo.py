"""Silver layer: events enriched with country attribution (spatial join)."""

from pyspark import pipelines as dp
from pyspark.sql import functions as F
from pyspark.sql.types import (
    ArrayType,
    MapType,
    StringType,
    StructField,
    StructType,
)

COUNTRIES_GEOJSON = "/Volumes/nasa_eonet/events_silver/countriesgeojson/countries.geojson"

_GEOJSON_SCHEMA = StructType([
    StructField("features", ArrayType(StructType([
        StructField("properties", MapType(StringType(), StringType())),
        StructField("geometry", StringType()),  # raw GeoJSON string
    ])))
])


@dp.temporary_view()
def countries_ref():
    """Country reference parsed from the GeoJSON volume file with pure Spark
    (no GeoPandas): each feature's geometry is captured as a raw JSON string
    and converted with st_geomfromgeojson (SRID 4326)."""
    return (
        spark.read.schema(_GEOJSON_SCHEMA)
        .option("multiLine", "true")
        .json(COUNTRIES_GEOJSON)
        .select(F.explode("features").alias("f"))
        .select(
            F.col("f.properties").getItem("name").alias("name"),
            F.col("f.properties").getItem("ISO3166-1-Alpha-3").alias("ISO3166-1-Alpha-3"),
            F.expr("st_geomfromgeojson(f.geometry)").alias("country_geom"),
        )
        .filter(F.col("country_geom").isNotNull())
    )


@dp.materialized_view(
    comment=(
        "Events with a centroid point (average of observation coordinates) "
        "and the country containing it (ST_Within). Country is NULL for "
        "legit open-ocean events."
    )
)
@dp.expect("country_matched", "`ISO3166-1-Alpha-3` IS NOT NULL")
def events_geo():
    events = spark.read.table("events")
    countries = spark.read.table("countries_ref")

    with_avg_point = (
        events
        .withColumn(
            "avg_x",
            F.expr(
                "aggregate(transform(geometry, g -> ST_X(g)), "
                "cast(0.0 as double), (acc, x) -> acc + x) / size(geometry)"
            ),
        )
        .withColumn(
            "avg_y",
            F.expr(
                "aggregate(transform(geometry, g -> ST_Y(g)), "
                "cast(0.0 as double), (acc, y) -> acc + y) / size(geometry)"
            ),
        )
        .withColumn(
            "avg_point",
            F.expr("ST_SetSRID(ST_Point(round(avg_x, 5), round(avg_y, 5)), 4326)"),
        )
        .drop("avg_x", "avg_y")
    )

    return with_avg_point.join(
        countries,
        how="left",
        on=F.expr("ST_Within(avg_point, country_geom)"),
    )
