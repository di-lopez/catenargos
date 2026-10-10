import pyspark.sql.functions as f
import yfinance as yf


def model(dbt, session):

    portfolio = dbt.ref("portfolio").withColumn("currency", f.col("currency").upper())

    stock = portfolio.filter(f.col("ticker") != "cash").select("ticker", "units")
    cash = (
        portfolio.filter(f.col("ticker") == "cash")
        .select(
            "ticker",
            "units",
            "currency",
        )
        .withColumn("price", f.lit(1))
    )

    current_value = dbt.ref("ticker_price").select("ticker", "price", "currency")

    # FX rate
    gbpusd = yf.Ticker("GBPUSD=X").info["regularMarketPrice"]

    return (
        stock.join(
            current_value,
            on="ticker",
            how="left",
        )
        .unionByName(cash)
        .withColumn("gbpusd", f.lit(gbpusd))
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
            .otherwise(f.col("price")),
        )
        .withColumn(
            "value",
            f.col("units") * f.col("usd_price"),
        )
        .withColumn(
            "date",
            f.current_date(),
        )
    )
