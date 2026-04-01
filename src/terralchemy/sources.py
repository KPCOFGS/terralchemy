"""Source definition parser for terralchemy."""

from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
from typing import Optional

import yaml


@dataclass
class Source:
    name: str
    path: str
    format: str
    crs: Optional[str] = None
    description: str = ""

    @property
    def resolved_path(self) -> Path:
        return Path(self.path)


def load_sources(sources_dir: Path, project_dir: Path) -> dict[str, Source]:
    """Load all source definitions from YAML files in the sources directory."""
    sources: dict[str, Source] = {}

    if not sources_dir.exists():
        return sources

    for yml_file in sorted(sources_dir.glob("*.yml")):
        with open(yml_file) as f:
            raw = yaml.safe_load(f)

        if not raw or "sources" not in raw:
            continue

        for src in raw["sources"]:
            # Resolve relative paths against project directory
            src_path = src["path"]
            if not Path(src_path).is_absolute():
                src_path = str(project_dir / src_path)

            fmt = src.get("format", "")
            if not fmt:
                fmt = Path(src_path).suffix.lstrip(".")

            source = Source(
                name=src["name"],
                path=src_path,
                format=fmt,
                crs=src.get("crs"),
                description=src.get("description", ""),
            )
            sources[source.name] = source

    return sources
