import argparse

import pandas as pd
from pyspark.sql import SparkSession, functions as F
from pyspark.sql.types import StructType, StructField, StringType, DecimalType, DateType

BASE_URL = "https://raw.githubusercontent.com/satyaawsomeday/claudexdatabricks/refs/heads/main"

TABLES = {
    "airports": StructType([
        StructField("airport_id", StringType()),
        StructField("airport_name", StringType()),
        StructField("city", StringType()),
        StructField("country", StringType()),
    ]),
    "bookings": StructType([
        StructField("booking_id", StringType()),
        StructField("passenger_id", StringType()),
        StructField("flight_id", StringType()),
        StructField("airport_id", StringType()),
        StructField("amount", DecimalType(12, 2)),
        StructField("booking_date", DateType()),
    ]),
    "passengers": StructType([
        StructField("passenger_id", StringType()),
        StructField("name", StringType()),
        StructField("gender", StringType()),
        StructField("nationality", StringType()),
    ]),
}


def clean_columns(pdf):
    pdf.columns = [c.strip().lower().replace(" ", "_") for c in pdf.columns]
    return pdf


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--catalog", required=True)
    parser.add_argument("--raw_schema", default="raw")
    args = parser.parse_args()

    spark = SparkSession.builder.getOrCreate()
    spark.sql(f"CREATE SCHEMA IF NOT EXISTS {args.catalog}.{args.raw_schema}")

    for table, schema in TABLES.items():
        pdf = clean_columns(pd.read_csv(f"{BASE_URL}/{table}.csv", dtype=str, keep_default_na=False))
        sdf = spark.createDataFrame(pdf)
        # Cast each column to the type of the existing raw table; empty strings become NULL.
        sdf = sdf.select([
            F.when(F.col(f.name) == "", None).otherwise(F.col(f.name)).cast(f.dataType).alias(f.name)
            for f in schema.fields
        ])
        target = f"{args.catalog}.{args.raw_schema}.{table}"
        sdf.write.mode("overwrite").saveAsTable(target)
        print(f"Wrote {sdf.count()} rows to {target}")


if __name__ == "__main__":
    main()
