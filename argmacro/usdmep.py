import argparse
from datetime import datetime, timezone

import pandas as pd
import pyspark.sql.functions as f
import requests
import yaml
from loguru import logger
from pyspark.sql import DataFrame, SparkSession
from utils import get_latest_date


class Config:
    def __init__(self, config):
        with open(config) as f:
            self.config = yaml.safe_load(f)

    def __getattr__(self, name):
        return self.config["usdmep"][name]


def dedupe(df: DataFrame) -> DataFrame:

    duplicated = df.groupBy("date").count().filter(f.col("count") > 1)
    duplicate_count = duplicated.count()
    if duplicate_count == 0:
        return df

    # Collect rows into pandas representation for clean logging
    sample_rows = duplicated.limit(20).toPandas()
    logger.info(
        f"Dropping {duplicate_count} duplicated rows. Sample:\n{sample_rows}",
    )

    return df.dropDuplicates(["date"])


def dropna(df: DataFrame) -> DataFrame:

    invalid_condition = (f.col("value") == 0) | f.col("value").isNull()

    valid_df = df.filter(~invalid_condition)

    # Handle logging if invalid rows exist
    invalid_df = df.filter(invalid_condition)
    invalid_count = invalid_df.count()

    if invalid_count > 0:
        # Collect rows into pandas representation for clean logging
        sample_rows = invalid_df.limit(20).toPandas()
        logger.info(
            f"Dropping {invalid_count} rows with nulls or 0s. Sample:\n{sample_rows}",
        )

    return valid_df


def dq(df: DataFrame) -> DataFrame:

    df = dedupe(df)
    df = dropna(df)

    return df


def get_usdmep(
    catalog: str,
    schema: str,
    table_name: str,
    start: str,
) -> pd.DataFrame:

    logger.info("Starting Fetch of USD MEP")

    spark = SparkSession.builder.getOrCreate()
    spark.catalog.setCurrentCatalog(catalog)
    spark.catalog.setCurrentDatabase(schema)

    if not start:
        logger.info("Start date not provided")
        if spark.catalog.tableExists(table_name):
            logger.info("Table exists, reading latest available date")
            latest = get_latest_date(spark, table_name)
            logger.info(f"Latest available date is {latest:%Y-%m-%d}")
            start = (latest + pd.offsets.BDay(1)).date()
            logger.info(f"Setting fetch start date as {start:%Y-%m-%d}")
        else:
            start = Config("config.yml").timeseries_start
            logger.info(f"Table does not exist, defaulting to {start:%Y-%m-%d}")

    headers = {
        "User-Agent": (
            "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
            "AppleWebKit/605.1.15 (KHTML, like Gecko) Version/18.5 "
            "Safari/605.1.15"
        ),
    }

    today = datetime.now(tz=timezone.utc).date()

    if start > today:
        logger.warning("Start date is in the future, nothing to do. Exiting")
        return

    start = start.strftime("%Y-%m-%d")
    today = today.strftime("%Y-%m-%d")

    logger.info(f"Fetching data from {start} to {today}")

    res = requests.get(
        f"https://mercados.ambito.com/dolarrava/mep/grafico/{start}/{today}",
        headers=headers,
    )

    df = pd.DataFrame(res.json()[1:], columns=["date", "value"])
    df["date"] = pd.to_datetime(df["date"], format="%d/%m/%Y")

    fetched_row_count = len(df)
    logger.info(f"Fetched {fetched_row_count} rows")

    df = spark.createDataFrame(df).withColumn("date", f.col("date").cast("date"))

    if spark.catalog.tableExists(table_name):
        df = df.join(
            spark.table(table_name),
            on=["date"],
            how="left_anti",
        )
        new_dates_count = df.count()
        if new_dates_count != fetched_row_count:
            dropped = fetched_row_count - new_dates_count
            logger.info(
                f"Already have data for {dropped}/{fetched_row_count} days, "
                f"skipping those. Inserting {new_dates_count} new rows",
            )
            return

    df = dq(df)

    logger.info(f"The following rows will be inserted:\n {df.toPandas()}")

    df.write.mode("append").saveAsTable(table_name)


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--catalog", type=str, required=True)
    parser.add_argument("--schema", type=str, required=True)
    parser.add_argument("--table", type=str, required=True)
    parser.add_argument("--start", type=str, required=False)
    args = parser.parse_args()

    get_usdmep(args.catalog, args.schema, args.table, args.start)
