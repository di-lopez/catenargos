import hashlib
import uuid
from abc import ABC, abstractmethod
from pathlib import Path

import pandas as pd
import yaml
from loguru import logger
from pyspark.sql import DataFrame, SparkSession


class Config:
    def __init__(self, target: str, config: str = "config.yml"):
        self.target = target
        with open(config) as f:
            self.config = yaml.safe_load(f)

    def __getattr__(self, name):
        return self.config[self.target][name]


class BaseFileProcessor(ABC):
    def __init__(
        self,
        catalog: str,
        schema: str,
        volume: str,
        table_name: str,
        prefix: str = "",
    ):
        self.catalog = catalog
        self.schema = schema
        self.volume = volume
        self.table_name = table_name
        self.spark = SparkSession.builder.getOrCreate()
        self.spark.catalog.setCurrentCatalog(catalog)
        self.spark.catalog.setCurrentDatabase(schema)

        self.raw_path = (
            Path("/Volumes") / self.catalog / "raw" / self.volume / self.name
        )

        self.silver_path = (
            Path("/Volumes") / self.catalog / "silver" / self.volume / self.name
        )

        self.files = [f for f in (self.raw_path).iterdir() if f.is_file()]

    @property
    @abstractmethod
    def name(self) -> str:
        pass

    def process(self):
        logger.info(f"{self.name.upper()} File Processor: Starting")

        if not self.files:
            logger.info("No new files to process. Exiting")
            return

        logger.info(f"Detected {len(self.files)} files to process")

        for i, file in enumerate(self.files, start=1):
            logger.info(f"{i:2d} | Starting file parsing for {file.name}")
            df = self.parse_file(file).assign(source=self.name.lower())
            # Add a transaction id for singling-out transactions as needed
            df["transaction_id"] = df.apply(
                lambda row: hashlib.sha256(
                    "|".join(
                        str(value)
                        for value in (
                            row["source"],
                            row["date"],
                            row["description"],
                            row["amount"],
                        )
                    ).encode("utf-8"),
                ).hexdigest(),
                axis=1,
            )
            df = self.spark.createDataFrame(df).select(
                "date",
                "amount",
                "description",
                "source",
                "transaction_id",
            )

            logger.info("Completed file parsing")

            self.store_file(file)

            logger.info("Writing to table")
            self.write_table(df)
            logger.info("Completed writing to table")

    def write_table(self, df: DataFrame):
        incoming_count = df.count()
        unique_df = df.dropDuplicates(["transaction_id"])

        if self.spark.catalog.tableExists(self.table_name):
            existing_ids = (
                self.spark.table(self.table_name).select("transaction_id").distinct()
            )
            unique_df = unique_df.join(
                existing_ids,
                on="transaction_id",
                how="left_anti",
            )

        new_count = unique_df.count()
        duplicate_count = incoming_count - new_count

        if duplicate_count:
            logger.info(f"Found {duplicate_count} duplicate rows")

        if new_count == 0:
            logger.info("All rows are duplicates; nothing to write")
            return

        unique_df.write.mode("append").saveAsTable(self.table_name)

    def store_file(self, file: Path):

        name = hashlib.sha256(uuid.uuid4().bytes).hexdigest()
        file_path = self.silver_path / f"{name}{file.suffix}"

        file_path.parent.mkdir(parents=True, exist_ok=True)

        logger.info(f"Storing file at {file_path.absolute()}")

        with file_path.open("wb") as f:
            f.write(file.open("rb").read())

        if file_path.exists():
            logger.info("File stored successfully")
            file.unlink()
        else:
            logger.error("File storage failed")
            msg = "File storage failed"
            raise Exception(msg)

    @abstractmethod
    def parse_file(self, file: Path) -> DataFrame:
        pass


class HSBCFile(BaseFileProcessor):
    @property
    def name(self) -> str:
        return "hsbc"

    def parse_file(self, file: Path) -> DataFrame:
        return (
            pd.read_csv(
                file.as_posix(),
                skipfooter=1,
                names=["date", "description", "amount"],
                engine="python",
            )
            .assign(
                date=lambda x: pd.to_datetime(x.date, format="%d/%m/%Y").dt.date,
                amount=lambda x: (
                    x.amount.astype(str).str.replace(",", "").astype(float)
                ),
                description=lambda x: x.description.astype(str).str.lower(),
            )
            .filter(items=["date", "amount", "description"])
        )


class ToptalFile(BaseFileProcessor):
    @property
    def name(self) -> str:
        return "toptal"

    def parse_file(self, file: Path) -> DataFrame:
        return (
            pd.read_excel(file.as_posix())
            .assign(
                date=lambda x: pd.to_datetime(x["Transaction date"]).dt.date,
                amount=lambda x: x.Amount.astype(float) / 1.3,  # convert to GBP
                description=lambda x: x.Description.astype(str).str.lower(),
            )
            .filter(items=["date", "amount", "description"])
        )
