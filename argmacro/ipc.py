import argparse
from datetime import datetime

import pandas as pd
import xlrd
import yaml
from loguru import logger
from pyspark.sql import DataFrame, SparkSession
from utils import BaseFileDownloader, BaseFileProcessor, get_latest_date


class Config:
    def __init__(self, config="config.yml"):
        with open(config) as f:
            self.config = yaml.safe_load(f)

    def __getattr__(self, name):
        return self.config["ipc"][name]


class FileDownloader(BaseFileDownloader):
    """Download the IPC file for a given month and year."""

    def __init__(self, date: datetime | None = None):
        super().__init__(date)

    @property
    def url(self) -> str:
        return f"https://www.indec.gob.ar/ftp/cuadros/economia/sh_ipc_{self.month}_{self.year}.xls"

    @property
    def expected_size(self) -> int:
        """Expected file size in MB."""
        return 2.5


class FileProcessor(BaseFileProcessor):
    def __init__(self, file: bytes, catalog: str, schema: str, file_date: datetime):

        super().__init__(
            file,
            catalog,
            schema,
            Config().volume,
            Config().table_name,
            file_date,
            prefix="ipc_",
        )

    def parse_file(self) -> DataFrame:
        """Parse the IPC Excel file and extract relevant data into a pandas DataFrame."""
        wb = xlrd.open_workbook(file_contents=self.file)
        sheet_name = Config().excel_target_sheet_name

        sheet = wb.sheet_by_name(sheet_name)

        if not sheet:
            logger.error(f"Sheet `{sheet_name}` not found")
            msg = "Sheet not found"
            raise Exception(msg)

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

        return self.spark.createDataFrame(df)


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

    def main(self):
        logger.info(f"Starting pipeline for {self.date:%Y-%m-%d}")
        table_exists = self.spark.catalog.tableExists(self.table_name)

        if not table_exists:
            self.initial_data_load()

        logger.info("Reading latest available date from the table")
        latest = get_latest_date(self.spark, self.table_name)
        if self.date.month - 1 <= latest.month:  # -1 as IPC is one month delayed
            logger.info(
                f"Already have CPI data for the requested release ({self.date:%B})."
                " Nothing to do, exiting",
            )
            return

        downloader = FileDownloader(self.date)
        file = downloader.get_file()
        if not file and downloader.file_exists:
            logger.error("File download failed. Please review logs")
            raise Exception("File download failed")

        self.process_file(file, self.date)


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--catalog", type=str, required=True)
    parser.add_argument("--schema", type=str, required=True)
    parser.add_argument("--table", type=str, required=True)
    parser.add_argument("--date", type=str, required=True)
    args = parser.parse_args()

    Pipeline(args.catalog, args.schema, args.table, args.date).main()
