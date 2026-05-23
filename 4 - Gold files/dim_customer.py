from pyspark import pipelines as dp
from pyspark.sql.functions import *

# Temporary view to extract and validate customer records
@dp.temporary_view()
@dp.expect_all_or_drop({
    'valid_customer_id': 'customer_id IS NOT NULL',
    'valid_ingestion_time': 'ingestion_time IS NOT NULL'
})
def customer_changes():
    return (
        spark.readStream.table("streaming_catalog.silver_schema.source_data_for_gold")
        .select(
            col("customer_id"),
            col("customer_name"),
            col("email"),
            col("city"),
            col("state"),
            col("ingestion_time")
        )
        
    )

# Create target table with identity-based surrogate key
# SCD Type 2 tracks customer history with __START_AT and __END_AT
dp.create_streaming_table(
    name="streaming_catalog.gold_schema.dim_customer",
    comment="Gold layer: Customer dimension with SCD Type 2 history tracking",
    schema="""
        customer_sk BIGINT GENERATED ALWAYS AS IDENTITY (START WITH 1 INCREMENT BY 1),
        customer_id INT,
        customer_name STRING,
        email STRING,
        city STRING,
        state STRING,
        ingestion_time TIMESTAMP,
        __START_AT TIMESTAMP,
        __END_AT TIMESTAMP
    """,
    cluster_by=["customer_id"],
    table_properties={
        "quality": "gold",
        "delta.enableChangeDataFeed": "true"
    }
)

# Apply Auto CDC with SCD Type 2 to track customer history
dp.create_auto_cdc_flow(
    target="streaming_catalog.gold_schema.dim_customer",
    source="customer_changes",
    keys=["customer_id"],
    sequence_by="ingestion_time",
    stored_as_scd_type=2,
    ignore_null_updates=True
)
