from abc import ABC, abstractmethod
from datetime import date, datetime, timezone
from pathlib import Path

import pyspark.sql.functions as f
import requests
from loguru import logger
from pyspark.sql import DataFrame, SparkSession


class BaseFileProcessor(ABC):
    def __init__(
        self,
        file: bytes,
        catalog: str,
        schema: str,
        volume: str,
        table_name: str,
        file_date: datetime,
        prefix: str = "",
    ):
        self.file = file
        self.catalog = catalog
        self.schema = schema
        self.volume = volume
        self.table_name = table_name
        self.file_name = f"{prefix}{file_date.year}_{file_date.month:02}.xls"
        self.spark = SparkSession.builder.getOrCreate()
        self.spark.catalog.setCurrentCatalog(catalog)
        self.spark.catalog.setCurrentDatabase(schema)

    def process(self):
        self.store_file()

        logger.info("Starting file parsing")
        df = self.parse_file()
        logger.info("Completed file parsing")

        logger.info("Writing to table")
        self.write_table(df)
        logger.info("Completed writing to table")

    def write_table(self, df: DataFrame):
        df.write.mode("overwrite").saveAsTable(self.table_name)

    def store_file(self):

        path = Path("/Volumes") / self.catalog / "raw" / self.volume / self.file_name
        logger.info(f"Storing file at {path.absolute()}")

        with path.open("wb") as f:
            f.write(self.file)

        if path.exists():
            logger.info("File stored successfully")
        else:
            logger.error("File storage failed")
            raise Exception("File storage failed")

    @abstractmethod
    def parse_file(self) -> DataFrame:
        pass


class BaseFileDownloader(ABC):
    """Download the an INDEC file for a given month and year."""

    FILE_SIZE_TOLERANCE = 0.75

    def __init__(self, date: datetime | None = None):
        self.date = date or datetime.now(tz=timezone.utc)

    @property
    def month(self) -> str:
        return f"{self.date.month:02}"

    @property
    def year(self) -> str:
        return f"{self.date.year}"[-2:]  # last 2 digits only

    @property
    @abstractmethod
    def url(self) -> str:
        """Subclasses must provide the the download URL."""

    @property
    @abstractmethod
    def expected_size(self) -> int:
        """Subclasses must provide the expected file size in megabytes."""

    @property
    def file_exists(self) -> bool:

        try:
            # Use HEAD to fetch headers only, set timeout to prevent hanging
            res = requests.head(self.url, allow_redirects=True, timeout=5)

            if not res.ok:
                return False

        except requests.RequestException:
            return False

        # Verify the server reports an Excel file rather than HTML
        content_type = res.headers.get("Content-Type", "").lower()

        return "excel" in content_type

    def get_file(self):
        logger.info(f"Attempting download of file for {self.month}/{self.year}")

        if not self.file_exists:
            logger.warning("File not available yet. Exiting")
            return None

        logger.info("File exists. Proceeding to download...")

        res = requests.get(self.url, allow_redirects=True)

        if not res.ok:
            logger.error(f"Download failed with status {res.status_code}: {res.reason}")
            return None

        # Verify the size we get is of the expected order of magnitude
        content_size = int(res.headers.get("Content-Length", 0)) / 1024**2
        if content_size / self.expected_size < self.FILE_SIZE_TOLERANCE:
            logger.warning(f"Downloaded file is too small: {content_size} MB")
        else:
            logger.info(f"Download successful. File size: {content_size:.2f} MB")

        return res.content


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
