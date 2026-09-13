import argparse
from datetime import datetime, timezone
from pathlib import Path

import pandas as pd
import requests
import xlrd
import yaml
from loguru import logger
from pyspark.sql import SparkSession
from utils import get_latest_date


class Config:
    def __init__(self, config="config.yml"):
        with open(config) as f:
            self.config = yaml.safe_load(f)

    def __getattr__(self, name):
        return self.config["ipc"][name]


class FileDownloader:
    """Download the IPC file for a given month and year."""

    def __init__(self, date: datetime | None = None):
        self.date = date or datetime.now(tz=timezone.utc)

    @property
    def month(self) -> str:
        return f"{self.date.month:02}"

    @property
    def year(self) -> str:
        return f"{self.date.year}"[-2:]  # last 2 digits only

    @property
    def url(self) -> str:

        return f"https://www.indec.gob.ar/ftp/cuadros/economia/sh_ipc_{self.month}_{self.year}.xls"

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

        # We expect a file size of the order of 2.25 MB
        # Verify the size we get is of this order of magnitude
        content_size = int(res.headers.get("Content-Length", 0)) / 1024**2
        if content_size / 2.5 < 0.75:
            logger.warning(f"Downloaded file is too small: {content_size} MB")
        else:
            logger.info(f"Download successful. File size: {content_size:.2f} MB")

        return res.content


class FileProcessor:
    def __init__(self, file: bytes, catalog: str, schema: str, file_date: datetime):
        self.file = file
        self.catalog = catalog
        self.schema = schema
        self.file_name = f"ipc_{file_date.year}_{file_date.month:02}.xls"
        self.spark = SparkSession.builder.getOrCreate()
        self.spark.catalog.setCurrentCatalog(catalog)
        self.spark.catalog.setCurrentDatabase(schema)

    def process(self):
        self.store_file()
        self.parse_file()

    def store_file(self):

        path = (
            Path("/Volumes") / self.catalog / "raw" / Config().volume / self.file_name
        )
        logger.info(f"Storing file at {path.absolute()}")

        with path.open("wb") as f:
            f.write(self.file)

        if path.exists():
            logger.info("File stored successfully")
        else:
            logger.error("File storage failed")
            raise Exception("File storage failed")

    def parse_file(self):

        logger.info("Starting file parsing")

        wb = xlrd.open_workbook(file_contents=self.file)
        sheet_name = Config().excel_target_sheet_name

        sheet = wb.sheet_by_name(sheet_name)

        if not sheet:
            logger.error(f"Sheet `{sheet_name}` not found")
            raise Exception("Sheet not found")

        logger.info(
            f"Read {sheet.nrows} rows and {sheet.ncols} columns from {sheet_name}",
        )

        date_row = [c.value.lower() for c in sheet.col(0)].index("total nacional")
        value_row = [c.value.lower() for c in sheet.col(0)].index("nivel general")

        # Take from column B (1) onwards
        date = [c.value for c in sheet.row(date_row)][1:]
        values = [c.value for c in sheet.row(value_row)][1:]

        df = pd.DataFrame({"date": date, "value": values}).assign(
            date=lambda x: (
                pd.to_datetime(x.date, unit="D", origin="1899-12-30").dt.date
            ),
        )

        logger.info(
            "Extracted dataframe for range "
            f"{df.date.min():%b %Y} to {df.date.max():%b %Y}",
        )

        df = self.spark.createDataFrame(df)
        df.write.mode("overwrite").saveAsTable(Config().table_name)
        logger.info("Completed file parsing")


class Pipeline:
    def __init__(self, catalog: str, schema: str, table_name: str, date: str):
        self.date = datetime.strptime(date, "%Y-%m-%d").date()

        self.table_name = table_name
        self.catalog = catalog
        self.schema = schema

        self.spark = SparkSession.builder.getOrCreate()
        self.spark.catalog.setCurrentCatalog(catalog)
        self.spark.catalog.setCurrentDatabase(schema)

    def initial_data_load(self):
        logger.info("Table does not exist yet. Starting initial data load.")

        date = self.date
        while True:
            logger.info(f"Setting target file date as {date:%B %y}")
            if not FileDownloader(date).file_exists:
                logger.info("File not available for this month. Trying previous month")
                date = date - pd.offsets.MonthEnd(1)
            else:
                break

        file = FileDownloader(date).get_file()
        if not file:
            logger.error("File download failed. Exiting")
            return

        self.process_file(file, date)

    def process_file(self, file: bytes, file_date: datetime):
        FileProcessor(
            file,
            self.catalog,
            self.schema,
            file_date,
        ).process()
        # TODO: Use the parser to read and create the table.

    def main(self):
        logger.info(f"Starting pipeline for {self.date:%Y-%m-%d}")
        table_exists = self.spark.catalog.tableExists(self.table_name)

        if not table_exists:
            self.initial_data_load()

        logger.info("Reading latest available date from the table")
        latest = get_latest_date(self.spark, self.table_name)
        if self.date.month <= latest.month:
            logger.info(
                f"Already have CPI data for the requested month ({self.date:%B}). "
                "Nothing to do, exiting",
            )
            return

        downloader = FileDownloader(self.date)
        file = downloader.get_file()
        if not file and downloader.file_exists:
            logger.error("File download failed. Please review logs")
            raise Exception("File download failed")

        self.process_file(file)


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--catalog", type=str, required=True)
    parser.add_argument("--schema", type=str, required=True)
    parser.add_argument("--table", type=str, required=True)
    parser.add_argument("--date", type=str, required=True)
    args = parser.parse_args()

    Pipeline(args.catalog, args.schema, args.table, args.date).main()
