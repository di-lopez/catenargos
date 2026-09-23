import argparse
import re
from datetime import date, datetime
from pathlib import Path

import pandas as pd
import requests
from bs4 import BeautifulSoup
from loguru import logger
from pydantic import BaseModel, Field, field_validator, model_validator
from utils import Config


class Document(BaseModel):
    release_date: date = Field(alias="href")
    url: str = Field(alias="href")
    file_name: str | None = None
    file: bytes | None = None

    @field_validator("release_date", mode="before")
    def parse_date(cls, text):
        year = re.search(r"(\d{4}).pdf", text).group(1)
        month = re.search(r"(\d*)\.IC", text).group(1)
        month = f"0{month}" if len(month) == 1 else month

        return datetime(int(year), int(month), 1).date()

    @model_validator(mode="after")
    def compute_file_name(self):
        self.file_name = f"{self.release_date.year}_{self.release_date.month:02}.pdf"
        return self

    def download_file(self):
        res = requests.get(self.url)

        if not res.ok:
            logger.error(f"Download failed with status {res.status_code}: {res.reason}")
            return

        self.file = res.content

    def store_file(self, catalog: str, volume: str):

        file_path = Path("/Volumes") / catalog / "raw" / volume / self.file_name

        logger.info(f"Storing file at {file_path.absolute()}")

        with file_path.open("wb") as f:
            f.write(self.file)

        if file_path.exists():
            logger.info("File stored successfully")
        else:
            msg = "File storage failed"
            logger.error(msg)
            raise Exception(msg)


class YearSection(BaseModel):
    year: int = Field(alias="title")
    documents: list[Document] = Field(alias="text")
    time_span: list[date, date] | None = None

    @field_validator("documents", mode="before")
    def split_links(cls, text):
        return [
            Document(href=href["href"])
            for href in BeautifulSoup(text, "html.parser").find_all("a", href=True)
        ]

    @model_validator(mode="after")
    def compute_span(self):
        earliest = min(self.documents, key=lambda d: d.release_date).release_date
        latest = max(self.documents, key=lambda d: d.release_date).release_date
        self.time_span = [earliest, latest]
        return self


def find_latest_download(catalog: str, volume: str):
    volume = Path("/Volumes") / catalog / "raw" / volume
    files = list(volume.glob("*.pdf"))
    if not files:
        return None

    dates = [
        datetime(int(file.name.split("_")[0]), int(file.name.split("_")[1]), 1).date()
        for file in files
    ]
    return max(dates)


def main(catalog: str, volume: str):

    logger.info(f"Started CPAU data fetch on {datetime.now().strftime('%Y-%m-%d')}")

    logger.info("Fetching available files")

    res = requests.get(
        "https://cpauorgapi.azurewebsites.net/api/SiteConsumer/ListContentBySection?sectionName=/biblioteca/servicios-y-productos/indices-y-costos-de-la-construccion&onlyBaseInfo=false&nocache=true",
    )

    if not res.ok:
        logger.error(f"Failed to fetch available files {res.status_code}: {res.reason}")
        return

    sections = [YearSection.model_validate(a) for a in res.json()["items"][1:]]

    # Filter sections based on timeseries start date
    sections = [
        section
        for section in sections
        if section.time_span[0] >= Config("cpau").timeseries_start
    ]

    logger.info(f"Identified data for {len(sections)} years")

    summary = pd.DataFrame(
        [
            {
                "year": s.year,
                "earliest_available": s.time_span[0],
                "latest_available": s.time_span[1],
                "ndocs": len(s.documents),
            }
            for s in sections
        ],
    ).sort_values("year")

    documents = [doc for section in sections for doc in section.documents]

    logger.info(f"The following {len(documents)} documents are available:\n{summary}")

    latest_download = find_latest_download(catalog, volume)
    if not latest_download:
        logger.info(
            "No files downloaded previously. Proceeding to download all available",
        )
    else:
        logger.info(f"Latest file downloaded for {latest_download:%Y-%m}")
        documents = [doc for doc in documents if doc.release_date > latest_download]
        if not documents:
            logger.info("No new files to download, already up to date. Exiting")
            return

    logger.info(f"Will download the following {len(documents)} files:")
    for d in documents:
        logger.info(f"{d.release_date:%Y-%m}: {d.url}")

    for d in documents:
        logger.info(f"Processing file for {d.release_date:%Y-%m}")
        d.download_file()
        d.store_file(catalog, volume)


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--catalog", type=str, required=True)
    parser.add_argument("--volume", type=str, required=True)
    args = parser.parse_args()

    main(args.catalog, args.volume)
