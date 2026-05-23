

from pyspark import pipelines as dp

@dp.table(
    name="streaming_catalog.silver_schema.source_data_for_gold",
)
def orders():
    return spark.readStream.table("streaming_catalog.silver_schema.orders")