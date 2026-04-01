"""Model parser with ref()/source() resolution for terralchemy."""

from __future__ import annotations

import re
from dataclasses import dataclass, field
from pathlib import Path
from typing import Optional


# Patterns to match {{ ref('model_name') }} and {{ source('source_name') }}
REF_PATTERN = re.compile(r"\{\{\s*ref\(\s*['\"](\w+)['\"]\s*\)\s*\}\}")
SOURCE_PATTERN = re.compile(r"\{\{\s*source\(\s*['\"](\w+)['\"]\s*\)\s*\}\}")

# Model config from SQL comments: -- config: key = value
CONFIG_PATTERN = re.compile(r"^--\s*(\w+)\s*:\s*(.+)$", re.MULTILINE)


@dataclass
class Model:
    name: str
    sql_path: Path
    raw_sql: str
    ref_dependencies: list[str] = field(default_factory=list)
    source_dependencies: list[str] = field(default_factory=list)
    config: dict[str, str] = field(default_factory=dict)

    @property
    def output_format(self) -> str:
        return self.config.get("output_format", "geoparquet")

    @property
    def crs(self) -> Optional[str]:
        return self.config.get("crs")

    @property
    def description(self) -> str:
        return self.config.get("description", "")

    def resolve_sql(self, source_views: dict[str, str], model_views: dict[str, str]) -> str:
        """Replace {{ source(...) }} and {{ ref(...) }} with actual view names."""
        sql = self.raw_sql

        def replace_source(match):
            name = match.group(1)
            view = source_views.get(name)
            if not view:
                raise ValueError(
                    f"Model '{self.name}' references undefined source '{name}'. "
                    f"Available sources: {', '.join(source_views.keys())}"
                )
            return view

        def replace_ref(match):
            name = match.group(1)
            view = model_views.get(name)
            if not view:
                raise ValueError(
                    f"Model '{self.name}' references undefined model '{name}'. "
                    f"Available models: {', '.join(model_views.keys())}"
                )
            return view

        sql = SOURCE_PATTERN.sub(replace_source, sql)
        sql = REF_PATTERN.sub(replace_ref, sql)
        return sql


def parse_model(sql_path: Path) -> Model:
    """Parse a SQL model file, extracting config, dependencies, and SQL."""
    raw_sql = sql_path.read_text()
    name = sql_path.stem

    # Extract config from comment headers
    config = {}
    for match in CONFIG_PATTERN.finditer(raw_sql):
        key = match.group(1).strip()
        value = match.group(2).strip()
        config[key] = value

    # Extract dependencies
    refs = REF_PATTERN.findall(raw_sql)
    sources = SOURCE_PATTERN.findall(raw_sql)

    # Strip config comments from SQL for execution
    clean_lines = []
    for line in raw_sql.splitlines():
        if CONFIG_PATTERN.match(line):
            continue
        clean_lines.append(line)
    clean_sql = "\n".join(clean_lines).strip()

    return Model(
        name=name,
        sql_path=sql_path,
        raw_sql=clean_sql,
        ref_dependencies=refs,
        source_dependencies=sources,
        config=config,
    )


def load_models(models_dir: Path) -> dict[str, Model]:
    """Load all SQL model files from the models directory."""
    models: dict[str, Model] = {}

    if not models_dir.exists():
        return models

    for sql_file in sorted(models_dir.glob("**/*.sql")):
        model = parse_model(sql_file)
        models[model.name] = model

    return models
