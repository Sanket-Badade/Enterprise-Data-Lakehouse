from pyspark import pipelines as dp

@dp.materialized_view(
    name="streaming_catalog.gold_schema.dim_date",
    comment="Gold layer: Date dimension with pre-generated calendar attributes (2020-2030)",
    cluster_by=["date_sk"],
    table_properties={
        "quality": "gold"
    }
)
def gold_dim_date():
    return spark.sql("""
        SELECT
            CAST(date_format(date_key, 'yyyyMMdd') AS INT) as date_sk,
            date_key,
            CAST(date_key AS STRING) as date_id,
            YEAR(date_key) as year,
            QUARTER(date_key) as quarter,
            MONTH(date_key) as month,
            DATE_FORMAT(date_key, 'MMMM') as month_name,
            WEEKOFYEAR(date_key) as week_of_year,
            DAYOFMONTH(date_key) as day,
            DAYOFWEEK(date_key) as day_of_week,
            DATE_FORMAT(date_key, 'EEEE') as day_name,
            CASE
                WHEN DAYOFWEEK(date_key) IN (1,7) THEN true
                ELSE false
            END as is_weekend
        FROM (
            SELECT explode(
                sequence(
                    to_date('2020-01-01'),
                    to_date('2030-12-31'),
                    interval 1 day
                )
            ) as date_key
        )
    """)
