from delta.tables import DeltaTable
from pyspark.sql import functions as F, Window
#bronze.vendor.streams: isrc, country, stream_date, usage_type,
#revenue_local, currency, delivered_at, file_name
def update_daily_revenue(spark, run_date):
 new = (spark.table("bronze.vendor.streams")
        .filter(F.col("delivered_at") >= F.date_sub(F.lit(run_date), 3)))

 w = (Window.partitionBy("isrc", "country", "stream_date")
           .orderBy(F.col("delivered_at").desc(), F.col("file_name").desc()))
 latest = new.withColumn("rn", F.row_number().over(w)).filter("rn = 1").drop("rn")

 fx = spark.table("silver.ref.fx_rates")  # currency, stream_date, usd_rate
 rev = (latest.join(F.broadcast(fx), ["currency", "stream_date"], "left")
       .withColumn("revenue_usd",
                   F.col("revenue_local") * F.coalesce("usd_rate", F.lit(1.0)))
       .groupBy("isrc", "stream_date")
       .agg(F.sum("revenue_usd").alias("revenue_usd")))
 (DeltaTable.forName(spark, "gold.finance.daily_revenue").alias("t")
   .merge(rev.alias("s"), "t.isrc = s.isrc AND t.stream_date = s.stream_date")
   .whenMatchedUpdateAll()
   .whenNotMatchedInsertAll()
   .execute())