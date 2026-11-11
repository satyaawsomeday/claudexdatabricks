import argparse

from pyspark.sql import SparkSession, functions as F


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--catalog", required=True)
    parser.add_argument("--raw_schema", default="raw")
    parser.add_argument("--enr_schema", default="enr")
    args = parser.parse_args()

    spark = SparkSession.builder.getOrCreate()

    raw = f"{args.catalog}.{args.raw_schema}"
    target = f"{args.catalog}.{args.enr_schema}.booking_enriched"

    spark.sql(f"CREATE SCHEMA IF NOT EXISTS {args.catalog}.{args.enr_schema}")

    bookings = spark.table(f"{raw}.bookings").alias("b")
    passengers = spark.table(f"{raw}.passengers").alias("p")
    airports = spark.table(f"{raw}.airports").alias("a")

    enriched = (
        bookings
        .join(passengers, F.col("b.passenger_id") == F.col("p.passenger_id"), "left")
        .join(airports, F.col("b.airport_id") == F.col("a.airport_id"), "left")
        .select(
            F.col("b.booking_id"),
            F.col("b.flight_id"),
            F.col("b.passenger_id"),
            F.col("b.airport_id"),
            F.col("b.amount"),
            F.col("b.booking_date"),
            F.col("p.name").alias("passenger_name"),
            F.col("p.gender").alias("passenger_gender"),
            F.col("p.nationality").alias("passenger_nationality"),
            F.col("a.airport_name"),
            F.col("a.city").alias("airport_city"),
            F.col("a.country").alias("airport_country"),
        )
    )

    enriched.write.mode("overwrite").saveAsTable(target)
    print(f"Wrote {spark.table(target).count()} rows to {target}")


if __name__ == "__main__":
    main()
