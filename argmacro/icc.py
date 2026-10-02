import argparse
import io
from datetime import datetime

import pandas as pd
from loguru import logger
from pyspark.sql import DataFrame, SparkSession
from utils import BaseFileDownloader, BaseFileProcessor, Config, get_latest_date


class FileDownloader(BaseFileDownloader):
    """Download the icc file for a given month and year."""

    def __init__(self, date: datetime | None = None):
        super().__init__(date)

    @property
    def url(self) -> str:
        return "https://www.indec.gob.ar/ftp/cuadros/economia/indice_icc_general_capitulos.csv"

    @property
    def expected_size(self) -> int:
        """Expected file size in MB."""
        return 0.008  # file is a small csv


class FileProcessor(BaseFileProcessor):
    def __init__(self, file: bytes, catalog: str, schema: str, file_date: datetime):

        super().__init__(
            file,
            catalog,
            schema,
            Config("icc").volume,
            Config("icc").table_name,
            file_date,
            prefix="icc_",
            suffix="csv",
        )

    def parse_file(self) -> DataFrame:
        """Parse the icc csv file."""
        df = (
            pd.read_csv(io.BytesIO(self.file), delimiter=";", decimal=",")
            .rename(
                columns={
                    "periodo": "date",
                    "nivel_general_aperturas": "index",
                    "indice_icc": "value",
                },
            )
            .assign(date=lambda x: pd.to_datetime(x.date, format="%d/%m/%Y").dt.date)
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

        file = FileDownloader(self.date).get_file()
        if not file:
            logger.error("File download failed. Exiting")
            return

        self.process_file(file, self.date)

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
        if self.date.month - 1 <= latest.month:  # -1 as ICC is one month delayed
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
