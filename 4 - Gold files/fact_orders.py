from pyspark import pipelines as dp
from pyspark.sql.functions import *

@dp.table(
    name="streaming_catalog.gold_schema.fact_orders",
    comment="Gold layer: Orders fact table with dimension surrogate keys and enriched attributes",
    cluster_by=["order_date", "customer_sk"],
    table_properties={
        "quality": "gold"
    }
)
def gold_fact_orders():
    
    # Read streaming source
    orders_df = spark.readStream.table("streaming_catalog.silver_schema.source_data_for_gold")
    
    # Read dimension snapshots (small tables - will auto-broadcast)
    dim_customer = spark.read.table("streaming_catalog.gold_schema.dim_customer")
    dim_product = spark.read.table("streaming_catalog.gold_schema.dim_product")
    dim_date = spark.read.table("streaming_catalog.gold_schema.dim_date")
    
    # Build fact table with dimension joins
    return (
        orders_df.alias("o")
        
        # Join customer dimension (SCD Type 2 - temporal join)
        .join(
            broadcast(dim_customer).alias("c"),
            (col("o.customer_id") == col("c.customer_id")) &
            (col("o.ingestion_time") >= col("c.__START_AT")) &
            ((col("o.ingestion_time") < col("c.__END_AT")) | col("c.__END_AT").isNull()),
            "left"
        )
        
        # Join product dimension (SCD Type 1 - simple join)
        .join(
            broadcast(dim_product).alias("p"),
            col("o.product_id") == col("p.product_id"),
            "left"
        )
        
        # Join date dimension
        .join(
            broadcast(dim_date).alias("d"),
            col("o.order_date") == col("d.date_key"),
            "left"
        )
        
        # Select final fact columns
        .select(
            col("o.order_id"),
            
            # Surrogate keys
            col("c.customer_sk"),
            col("p.product_sk"),
            col("d.date_sk"),
            
            # Business measures
            col("o.total_amount"),
            col("o.quantity"),
            
            # Audit columns
            col("o.order_date"),
            col("o.ingestion_time")
        )
    )
