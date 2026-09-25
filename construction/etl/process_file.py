import argparse
import json
import re
from datetime import datetime, timezone
from pathlib import Path

import dateparser
import fitz
import pandas as pd
from loguru import logger
from process_image import process_image
from pyspark.dbutils import DBUtils
from pyspark.sql import SparkSession


class FileProcessor:
    def __init__(
        self,
        file_path: Path,
        catalog: str,
        schema: str,
        volume: str,
        table_name: str,
    ):
        self.file_path = file_path
        self.catalog = catalog
        self.schema = schema
        self.volume = volume
        self.table_name = table_name
        with self.file_path.open("rb") as f:
            self.file = f.read()

        self.spark = SparkSession.builder.getOrCreate()
        self.spark.catalog.setCurrentCatalog(catalog)
        self.spark.catalog.setCurrentDatabase(schema)

        dbutils = DBUtils(self.spark)
        self.api_key = dbutils.secrets.get(scope="my_secrets", key="gemini_api_key")

    def process(self):
        logger.info(f"Starting processing of file {self.file_path.absolute()}")
        if not self.file_path.exists():
            msg = f"File {self.file_path.absolute()} does not exist"
            logger.error(msg)
            raise Exception(msg)

        doc = fitz.open(self.file_path)
        parser = CPAUParser(doc)

        doc_date = parser.date
        logger.info(f"Identified date in document as {doc_date:%B %Y}")

        self.file_path = (
            Path("/Volumes")
            / self.catalog
            / self.schema
            / self.volume
            / f"{doc_date.year}_{doc_date.month:02}"
            / "source_file.pdf"
        )

        logger.info(f"Copying file at processed location {self.file_path.absolute()}")

        self.file_path.parent.mkdir(parents=True, exist_ok=True)

        with self.file_path.open("wb") as f:
            f.write(self.file)

        if self.file_path.exists():
            logger.info("File stored successfully")
        else:
            msg = "File storage failed"
            logger.error(msg)
            raise Exception(msg)

        logger.info(
            "The parser will extract data from pages "
            f"{parser.start_page} to {parser.end_page}",
        )

        images = parser.get_images()
        document_data = []
        for image_name, image_data in images.items():
            logger.info(f"Processing image {image_name}")
            logger.info("Saving image at Volume location")
            image_path = self.file_path.parent / image_name
            image_path.write_bytes(image_data)

            logger.info("Processing image")
            image_data = process_image(image_path, self.api_key)

            logger.info(f"Extracted image data:\n{json.dumps(image_data, indent=2)}")

            document_data += image_data

        df = pd.DataFrame(document_data).assign(
            date=doc_date,
            extraction_ts=datetime.now(tz=timezone.utc),
            source_path=str(self.file_path.as_posix()),
            model_id=lambda x: x.model_id.str.lower(),
            model_type=lambda x: x.model_type.str.lower(),
            arsm2=lambda x: x.arsm2.astype(float),
        )

        logger.info(
            "Completed file processing. "
            f"Identified {df.model_type.nunique()} model types",
        )

        logger.info(f"The following data will be written:'\n{df}")

        df = self.spark.createDataFrame(df)

        logger.info(
            f"Writing to table {self.catalog}.{self.schema}.{self.table_name}",
        )
        df.write.mode("append").saveAsTable(self.table_name)
        logger.info("Completed processing")


class CPAUParser:
    START_PHRASE = "costos de los modelos"
    END_PHRASE = "fuente"

    def __init__(self, pdf_doc: fitz.Document):
        self.pdf_doc = pdf_doc
        self.start_page, self.end_page = self.get_target_slice()
        self._date = None

    @property
    def date(self) -> datetime.date:
        if self._date is None:
            self.get_doc_date()

        return self._date

    def get_images(self):
        logger.info("Extracting images from document")
        images = {}
        image_counter = 0
        for page in self.pdf_doc[self.start_page : self.end_page]:
            for img in page.get_images():
                xref = img[0]
                base_image = self.pdf_doc.extract_image(xref)
                if base_image["size"] < 20000:
                    # This is too small for the typical sizes we expect
                    continue

                image_counter += 1
                images[f"img{image_counter}.{base_image['ext']}"] = base_image["image"]
        logger.info(f"Extracted {image_counter} images")
        return images

    def get_target_slice(self) -> tuple[int, int]:
        start_page, end_page = None, None
        for page_num, page in enumerate(self.pdf_doc):
            text = page.get_text("text")

            if (
                "ndice general" in text.lower()
                and self.START_PHRASE.lower() in text.lower()
            ):
                start_page, end_page = self._get_slice_from_index(page)

                if start_page is not None:
                    return start_page, end_page

            if (
                start_page is None
                and self.START_PHRASE.lower() in text.lower()
                and "ndice general" not in text.lower()
            ):
                start_page = page_num

            if start_page is not None and self.END_PHRASE.lower() in text.lower():
                end_page = page_num
                break

        return start_page, end_page

    def _get_slice_from_index(self, index_page):
        logger.info("Attempting to get target pdf slice from index")
        index = [
            l.strip() for l in index_page.get_text("text").split("\n") if l.strip()
        ]

        start_page, end_page = None, None

        i = 0
        while i < len(index):
            if self.START_PHRASE.lower() in index[i].lower():
                start_page = int(re.search(r"(\d+)", index[i + 1]).group(1)) - 1
                end_page = int(re.search(r"(\d+)", index[i + 3]).group(1)) - 1
                break
            i += 1

        if not start_page:
            msg = "Unable to find relevant slice from index"
            logger.warning(msg)

        logger.info(f"Found slice {start_page} to {end_page} from index")

        return start_page, end_page

    def get_doc_date(self):
        logger.info("Getting document date")
        page = self.pdf_doc[self.start_page]
        lines = [
            line.strip().lower() for line in page.get_text().split("\n") if line.strip()
        ]

        date_idx = [
            i + 1
            for i, line in enumerate(lines)
            if self.START_PHRASE.lower() in line.lower()
        ][0]

        if date_idx >= len(lines):
            msg = "Unable to find document date"
            logger.error(msg)
            raise Exception(msg)

        self._date = (dateparser.parse(lines[date_idx]) + pd.offsets.MonthEnd(1)).date()


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--file")
    parser.add_argument("--catalog", required=True)
    parser.add_argument("--schema", required=True)
    parser.add_argument("--table", required=True)
    parser.add_argument("--volume", required=True)
    args = parser.parse_args()

    logger.info("Workflow triggered by file arrival")
    pdf_paths = list(Path(args.file).glob("*.pdf"))
    logger.info(f"Detected {len(pdf_paths)} new files")
    for path in pdf_paths:
        FileProcessor(
            path,
            args.catalog,
            args.schema,
            args.volume,
            args.table,
        ).process()
        logger.info(f"Completed parsing for PDF: {path}")
        path.rename(path.with_suffix(".processed"))


if __name__ == "__main__":
    main()
