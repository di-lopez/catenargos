from pathlib import Path

from google import genai
from google.genai import types
from PIL import Image
from pydantic import BaseModel, Field


# 2. Define your target JSON schema using Pydantic
class ModelEntry(BaseModel):
    model_id: str = Field(
        description="The model identifier, e.g., 'MODELO 1'",
    )
    model_type: str = Field(
        description="The title/name of the model, e.g., 'Vivienda unifamiliar'",
    )
    arsm2: float = Field(
        description="The cost per m2 value formatted as a number",
    )


class PageExtraction(BaseModel):
    models: list[ModelEntry]


def process_image(image_path: Path, api_key: str) -> list[dict]:

    image = Image.open(image_path)

    # 1. Initialize the client (reads GEMINI_API_KEY from environment)
    client = genai.Client(api_key=api_key)

    # 4. Call Gemini with Structured Output enforcing the schema
    response = client.models.generate_content(
        model="gemini-3.5-flash-lite",
        contents=[
            image,
            (
                "Extract all 'MODELO' cards from this document page. "
                "For each card, get its model number, title, and the cost per m2 ('Costo por m2')."
            ),
        ],
        config=types.GenerateContentConfig(
            response_mime_type="application/json",
            response_schema=PageExtraction,
        ),
    )

    return response.parsed.model_dump()["models"]


if __name__ == "__main__":
    image_path = "img31.jpeg"
    image = Image.open(image_path)
    page = process_image(image)
    print(page)
