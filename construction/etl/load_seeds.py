from pyspark.sql import SparkSession
from pathlib import Path
from loguru import logger
import argparse


def main(catalog: str, schema: str):

    spark = SparkSession.builder.getOrCreate()
    spark.catalog.setCurrentCatalog(catalog)
    spark.catalog.setCurrentDatabase(schema)

    # Path where the CSV is uploaded in the workspace/volume
    seeds = Path("seeds").glob("*.csv")

    for seed in seeds:
        target_table = seed.stem

        logger.info(f"Creating seed table {target_table}...")
        df = (
            spark.read.format("csv")
            .option("header", "true")
            .option("inferSchema", "true")
            .load(str(seed))
        )

        df.write.format("delta").mode("overwrite").saveAsTable(target_table)
        logger.info("Seed table created successfully.")


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--catalog", type=str, required=True)
    parser.add_argument("--schema", type=str, required=True)
    args = parser.parse_args()

    main(args.catalog, args.schema)
