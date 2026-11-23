"""Gold layer: category dimension."""

from pyspark import pipelines as dp


@dp.materialized_view(
    comment=(
        "Category dimension. (The exploratory notebook additionally extracts "
        "spaCy hotwords; add spaCy to the pipeline environment to port it.)"
    )
)
def dim_categories():
    return spark.read.table("categories")
