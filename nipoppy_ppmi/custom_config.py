from pathlib import Path
from typing import Optional

from pydantic import BaseModel


class FileConfig(BaseModel):
    FILEPATH: Path
    DESCRIPTION: Optional[str] = None


class TabularFileConfig(FileConfig):
    COLUMN: str
    IS_STATIC: bool


class CustomConfig(BaseModel):
    DEMOGRAPHICS: dict[str, TabularFileConfig]
    ASSESSMENTS: dict[str, TabularFileConfig]
    IMAGING_INFO: FileConfig
