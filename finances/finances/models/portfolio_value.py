import pyspark.sql.functions as f
import yfinance as yf


def model(dbt, session):

    dbt.config(
        materialized="incremental",
        unique_key=["ts"],
        incremental_strategy="merge",
    )

    portfolio = dbt.ref("portfolio").withColumn("currency", f.upper("currency"))

    stock = portfolio.filter(f.col("ticker") != "cash").select(
        "ticker", "units", "platform"
    )
    cash = (
        portfolio.filter(f.col("ticker") == "cash")
        .select(
            "ticker",
            "units",
            "currency",
            "platform",
        )
        .withColumn("price", f.lit(1))
    )

    current_value = dbt.ref("ticker_price").select("ticker", "price", "currency")

    # FX rate
    gbpusd = yf.Ticker("GBPUSD=X").info["regularMarketPrice"]
    arsusd = dbt.source("argmacro", "usdmep").select(f.last("value")).collect()[0][0]

    return (
        stock.join(
            current_value,
            on="ticker",
            how="left",
        )
        .unionByName(cash)
        .withColumn("gbpusd", f.lit(gbpusd))
        .withColumn("arsusd", f.lit(arsusd))
        .withColumn(
            "usd_price",
            f.when(
                f.col("currency") == "GBP",
                f.col("price") * f.col("gbpusd"),
            )
            .when(
                f.col("currency") == "GBp",  # Pence
                f.col("price") * f.col("gbpusd") / 100,
            )
            .when(f.col("currency") == "ARS", f.col("price") / f.col("arsusd"))
            .otherwise(f.col("price")),
        )
        .withColumn(
            "value",
            f.col("units") * f.col("usd_price"),
        )
        .withColumn(
            "ts",
            f.current_timestamp(),
        )
    )
