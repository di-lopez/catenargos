import argparse
import io
from datetime import datetime

import pandas as pd
from loguru import logger
from pyspark.sql import DataFrame, SparkSession
from utils import BaseFileDownloader, BaseFileProcessor, Config


class FileDownloader(BaseFileDownloader):
    """Download the IPC file for a given month and year."""

    def __init__(self, date: datetime | None = None):
        super().__init__(date)

    @property
    def url(self) -> str:
        return (
            f"https://www.indec.gob.ar/ftp/cuadros/economia/sh_isac_20{self.year}.xls"
        )

    @property
    def expected_size(self) -> int:
        """Expected file size in MB."""
        return 0.75


class FileProcessor(BaseFileProcessor):
    def __init__(self, file: bytes, catalog: str, schema: str, file_date: datetime):

        super().__init__(
            file,
            catalog,
            schema,
            Config("isac").volume,
            Config("isac").table_name,
            file_date,
            prefix="isac_",
        )

    def parse_file(self) -> DataFrame:
        monthmap = {
            "enero": "01",
            "febrero": "02",
            "marzo": "03",
            "abril": "04",
            "mayo": "05",
            "junio": "06",
            "julio": "07",
            "agosto": "08",
            "septiembre": "09",
            "octubre": "10",
            "noviembre": "11",
            "diciembre": "12",
        }

        df = (
            pd.read_excel(
                io.BytesIO(self.file),
                sheet_name=Config("isac").excel_target_sheet_name,
                skiprows=5,
                skipfooter=5,
                usecols=[0, 1, 2, 6, 9],
                names=[
                    "year",
                    "month",
                    "isac_general",
                    "isac_desestacionalizado",
                    "isac_tendendiaciclo",
                ],
            )
            .dropna(subset=["isac_general"])
            .assign(
                year=lambda x: x.year.ffill().astype(int).astype(str),
                month=lambda x: x.month.str.lower().str.strip().map(monthmap),
                date=lambda x: (
                    pd.to_datetime(
                        x[["year", "month"]].astype(str).agg("-".join, axis=1),
                        format="%Y-%m",
                    ).dt.date
                ),
            )
            .drop(columns=["year", "month"])
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

    def main(self):
        logger.info(f"Starting pipeline for {self.date:%Y-%m-%d}")

        downloader = FileDownloader(self.date)
        file = downloader.get_file()
        if not file and downloader.file_exists:
            logger.error("File download failed. Please review logs")
            msg = "File download failed"
            raise Exception(msg)

        processor = FileProcessor(
            file,
            self.catalog,
            self.schema,
            self.date,
        )

        # Check latest file for differences
        logger.info(f"Checking for existing files in {processor.file_path.parent}")
        available_files = list(processor.file_path.parent.glob("*.xls"))
        latest_file = max(available_files) if available_files else None
        if not latest_file:
            logger.info("No file has been downloaded yet. Processing this one...")
            # No file exists yet, this would be the first
            processor.process()
            return

        with latest_file.open("rb") as f:
            latest_file_data = f.read()

        if latest_file_data == file:
            logger.info(
                f"Data from existing file {latest_file.name} is up to date. "
                "Nothing to do, exiting.",
            )
            return

        processor.process()


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--catalog", type=str, required=True)
    parser.add_argument("--schema", type=str, required=True)
    parser.add_argument("--table", type=str, required=True)
    parser.add_argument("--date", type=str, required=True)
    args = parser.parse_args()

    Pipeline(args.catalog, args.schema, args.table, args.date).main()
