import pandas as pd
import pyspark.sql.functions as f
import yfinance as yf
from loguru import logger


def get_current_ticker_value(ticker: str) -> tuple[float, str]:
    logger.info(f"Getting price for {ticker=}")
    info = yf.Ticker(ticker).info
    return {
        "ticker": ticker,
        "price": info["regularMarketPrice"],
        "currency": info["currency"],
    }


def model(dbt, session):
    portfolio = dbt.ref("portfolio")

    tickers = [
        t["ticker"]
        for t in portfolio.filter(f.col("ticker") != "cash")
        .select("ticker")
        .distinct()
        .collect()
    ]

    # Current value of each asset with a timestamp
    return pd.DataFrame([get_current_ticker_value(t) for t in tickers]).assign(
        ts=pd.Timestamp.now(),
    )
