from pyspark.sql import DataFrame, SparkSession
import pyspark.sql.functions as f
from datetime import date


def get_latest_date(spark: SparkSession, table_name: str) -> date:
    """Get the latest available date in a given table.

    Parameters
    ----------
    spark : SparkSession
        A spark session on the required catalog.schema
    table_name : str
        A table name, or a fully-qualified table name if the spark session
        is not qualified.

    Returns
    -------
    date
        The latest available date in the table

    """
    return (
        spark.table(table_name)
        .select(
            f.max("date").cast("date").alias("latest"),
        )
        .collect()[0]["latest"]
    )
