import argparse

from pyspark.sql import SparkSession


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--catalog", required=True)
    parser.add_argument("--enr_schema", default="enr")
    parser.add_argument("--cur_schema", default="cur")
    args = parser.parse_args()

    spark = SparkSession.builder.getOrCreate()

    spark.sql(f"CREATE SCHEMA IF NOT EXISTS {args.catalog}.{args.cur_schema}")
    spark.sql(f"""
        CREATE OR REPLACE VIEW {args.catalog}.{args.cur_schema}.bookings_per_city AS
        SELECT airport_city AS city, COUNT(*) AS booking_count
        FROM {args.catalog}.{args.enr_schema}.booking_enriched
        GROUP BY airport_city
    """)
    print(f"Created view {args.catalog}.{args.cur_schema}.bookings_per_city")


if __name__ == "__main__":
    main()
