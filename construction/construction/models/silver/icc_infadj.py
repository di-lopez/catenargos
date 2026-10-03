import io

import pandas as pd
import pyspark.sql.functions as f
import requests


def get_us_cpi(start_date: str):
    print(f"Fetching US CPI from {start_date}")

    res = requests.get(
        f"https://fred.stlouisfed.org/graph/fredgraph.csv?id=CPIAUCSL&cosd={start_date}",
    )

    if not res.ok:
        print(f"Failed to fetch US CPI {res.status_code}: {res.reason}")
        res.raise_for_status()

    return (
        pd.read_csv(io.StringIO(res.content.decode("utf-8")))
        .rename(
            columns={"observation_date": "date", "CPIAUCSL": "cpi"},
        )
        .assign(date=lambda x: pd.to_datetime(x.date, format="%Y-%m-%d").dt.date)
    )


def model(dbt, session):

    df = dbt.ref("icc_usd").withColumn("date", f.col("date").cast("date")).toPandas()
    cpi = get_us_cpi(df.date.min().strftime("%Y-%m-%d"))

    df = df.merge(cpi, how="left", on="date").assign(
        cpi=lambda x: x.cpi.fillna(method="ffill"),
    )

    latest_cpi = (
        df.sort_values("date", ascending=False).head(1)["cpi"].astype(float).values[0]
    )

    return (
        df.assign(
            value_usd_infadj=lambda x: x.value_usd * latest_cpi / x.cpi,
        )
        .drop(columns=["cpi", "value_ars"])
        .sort_values("date")
    )
