import argparse
import sys

from pyspark.sql import SparkSession, functions as F


def duplicate_keys(spark, table, key):
    return (
        spark.table(table)
        .groupBy(key)
        .count()
        .filter(F.col("count") > 1)
        .count()
    )


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--catalog", required=True)
    parser.add_argument("--raw_schema", default="raw")
    parser.add_argument("--enr_schema", default="enr")
    args = parser.parse_args()

    spark = SparkSession.builder.getOrCreate()

    raw = f"{args.catalog}.{args.raw_schema}"
    enr = f"{args.catalog}.{args.enr_schema}"

    bookings_count = spark.table(f"{raw}.bookings").count()
    enriched_count = spark.table(f"{enr}.booking_enriched").count()
    dup_passengers = duplicate_keys(spark, f"{raw}.passengers", "passenger_id")
    dup_airports = duplicate_keys(spark, f"{raw}.airports", "airport_id")

    failures = []
    if enriched_count != bookings_count:
        failures.append(f"booking_enriched count {enriched_count} != bookings count {bookings_count}")
    if dup_passengers > 0:
        failures.append(f"{dup_passengers} duplicated passenger_id value(s) in passengers")
    if dup_airports > 0:
        failures.append(f"{dup_airports} duplicated airport_id value(s) in airports")

    print(f"bookings={bookings_count} booking_enriched={enriched_count}")
    print(f"duplicate passenger_id={dup_passengers} duplicate airport_id={dup_airports}")

    if failures:
        for f in failures:
            print(f"FAIL: {f}")
        sys.exit(1)
    print("PASS: all quality checks passed")


if __name__ == "__main__":
    main()
