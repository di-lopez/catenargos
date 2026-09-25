import threading
import time
from collections import deque
from pathlib import Path

from google import genai
from google.genai import types
from PIL import Image
from pydantic import BaseModel, Field
from utils import Config


class RateLimiter:
    """Sliding-window limiter: at most `max_calls` per `period` seconds."""

    def __init__(self, max_calls: int, period: float = 60.0):
        self.max_calls = max_calls
        self.period = period
        self._timestamps: deque[float] = deque()
        self._lock = threading.Lock()

    def wait(self) -> None:
        while True:
            with self._lock:
                now = time.monotonic()

                # Drop timestamps that have fallen out of the window
                while self._timestamps and now - self._timestamps[0] >= self.period:
                    self._timestamps.popleft()

                if len(self._timestamps) < self.max_calls:
                    self._timestamps.append(now)
                    return

                # Time until the oldest call leaves the window
                sleep_for = self.period - (now - self._timestamps[0])

            time.sleep(max(sleep_for, 0.01))


# Module-level: shared by every call to process_image
gemini_limiter = RateLimiter(max_calls=Config("cpau").llm_rpm, period=60.0)


class ModelEntry(BaseModel):
    model_id: str = Field(description="The model identifier, e.g., 'MODELO 1'")
    model_type: str = Field(
        description="The title/name of the model, e.g., 'Vivienda unifamiliar'"
    )
    arsm2: float = Field(description="The cost per m2 value formatted as a number")


class PageExtraction(BaseModel):
    models: list[ModelEntry]


def process_image(image_path: Path, api_key: str) -> list[dict]:
    image = Image.open(image_path)
    client = genai.Client(api_key=api_key)

    gemini_limiter.wait()  # blocks until a slot is available

    response = client.models.generate_content(
        model=Config("cpau").llm_model,
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
