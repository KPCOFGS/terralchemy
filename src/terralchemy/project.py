"""Project configuration loader for terralchemy."""

from __future__ import annotations

from dataclasses import dataclass, field
from pathlib import Path

import yaml


@dataclass
class ProjectConfig:
    name: str
    version: str = "0.1.0"
    database: str = "./target/terralchemy.duckdb"
    sources_path: str = "sources"
    models_path: str = "models"
    tests_path: str = "tests"
    target_path: str = "target"
    vars: dict = field(default_factory=dict)

    @classmethod
    def from_file(cls, path: Path) -> "ProjectConfig":
        with open(path) as f:
            raw = yaml.safe_load(f)

        return cls(
            name=raw["name"],
            version=raw.get("version", "0.1.0"),
            database=raw.get("profile", {}).get("database", "./target/terralchemy.duckdb"),
            sources_path=raw.get("sources_path", "sources"),
            models_path=raw.get("models_path", "models"),
            tests_path=raw.get("tests_path", "tests"),
            target_path=raw.get("target_path", "target"),
            vars=raw.get("vars", {}),
        )

    @property
    def project_dir(self) -> Path:
        """Inferred from the database path's parent."""
        return Path(self.database).parent.parent


def find_project_file(start: Path = None) -> Path:
    """Walk up from start directory to find terralchemy_project.yml."""
    current = start or Path.cwd()
    while True:
        candidate = current / "terralchemy_project.yml"
        if candidate.exists():
            return candidate
        parent = current.parent
        if parent == current:
            raise FileNotFoundError(
                "No terralchemy_project.yml found. Run 'terralchemy init' to create a project."
            )
        current = parent
