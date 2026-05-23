from pyspark import pipelines as dp
from pyspark.sql.functions import *

# Temporary view to extract and validate product records
@dp.temporary_view()
@dp.expect_all_or_drop({
    'valid_product_id': 'product_id IS NOT NULL',
    'valid_ingestion_time': 'ingestion_time IS NOT NULL',
    'valid_price': 'price > 0'
})
def product_changes():
    return (
        spark.readStream.table("streaming_catalog.silver_schema.source_data_for_gold")
        .select(
            col("product_id"),
            col("product_name"),
            col("category"),
            col("brand"),
            col("price"),
            col("ingestion_time")
        )
    )

# Create target table with identity-based surrogate key
# SCD Type 1 maintains only latest product state
dp.create_streaming_table(
    name="streaming_catalog.gold_schema.dim_product",
    comment="Gold layer: Product dimension with SCD Type 1 (latest values only)",
    schema="""
        product_sk BIGINT GENERATED ALWAYS AS IDENTITY (START WITH 1 INCREMENT BY 1),
        product_id INT,
        product_name STRING,
        category STRING,
        brand STRING,
        price BIGINT,
        ingestion_time TIMESTAMP
    """,
    cluster_by=["category", "product_id"],
    table_properties={
        "quality": "gold",
        "delta.enableChangeDataFeed": "true"
    }
)

# Apply Auto CDC with SCD Type 1
dp.create_auto_cdc_flow(
    target="streaming_catalog.gold_schema.dim_product",
    source="product_changes",
    keys=["product_id"],
    sequence_by="ingestion_time",
    stored_as_scd_type=1,
    ignore_null_updates=True
)
