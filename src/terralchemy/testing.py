"""Spatial-aware test runner for terralchemy pipelines."""

from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path

import yaml
from rich.console import Console
from rich.table import Table

from terralchemy.engine import SpatialEngine

console = Console()


@dataclass
class TestResult:
    name: str
    model: str
    test_type: str
    passed: bool
    message: str
    failing_rows: int = 0


# Built-in spatial test definitions
SPATIAL_TESTS = {
    "geometry_is_valid": {
        "description": "All geometries pass ST_IsValid",
        "query": "SELECT COUNT(*) FROM __model__{model} WHERE NOT ST_IsValid(geometry)",
        "expect_zero": True,
    },
    "geometry_not_empty": {
        "description": "No empty geometries",
        "query": "SELECT COUNT(*) FROM __model__{model} WHERE ST_IsEmpty(geometry)",
        "expect_zero": True,
    },
    "geometry_not_null": {
        "description": "No NULL geometries",
        "query": "SELECT COUNT(*) FROM __model__{model} WHERE geometry IS NULL",
        "expect_zero": True,
    },
    "no_duplicate_geometries": {
        "description": "No exact duplicate geometries",
        "query": (
            "SELECT COUNT(*) - COUNT(DISTINCT ST_AsText(geometry)) "
            "FROM __model__{model}"
        ),
        "expect_zero": True,
    },
    "bounds_check": {
        "description": "All geometries within valid WGS84 bounds",
        "query": (
            "SELECT COUNT(*) FROM __model__{model} "
            "WHERE ST_XMin(geometry) < -180 OR ST_XMax(geometry) > 180 "
            "OR ST_YMin(geometry) < -90 OR ST_YMax(geometry) > 90"
        ),
        "expect_zero": True,
    },
    "row_count_positive": {
        "description": "Model has at least one row",
        "query": "SELECT COUNT(*) FROM __model__{model}",
        "expect_positive": True,
    },
}


def load_tests(tests_dir: Path) -> list[dict]:
    """Load test definitions from YAML files."""
    all_tests = []

    if not tests_dir.exists():
        return all_tests

    for yml_file in sorted(tests_dir.glob("*.yml")):
        with open(yml_file) as f:
            raw = yaml.safe_load(f)

        if not raw or "tests" not in raw:
            continue

        all_tests.extend(raw["tests"])

    return all_tests


def run_test(engine: SpatialEngine, test_def: dict) -> TestResult:
    """Run a single spatial test."""
    name = test_def["name"]
    model = test_def["model"]
    test_type = test_def["test"]

    # Check if it's a built-in test
    if test_type in SPATIAL_TESTS:
        return _run_builtin_test(engine, name, model, test_type)

    # Custom SQL test
    if test_type == "custom_sql":
        return _run_custom_test(engine, name, model, test_def)

    return TestResult(
        name=name,
        model=model,
        test_type=test_type,
        passed=False,
        message=f"Unknown test type: {test_type}",
    )


def _run_builtin_test(
    engine: SpatialEngine, name: str, model: str, test_type: str
) -> TestResult:
    """Run a built-in spatial test."""
    test_spec = SPATIAL_TESTS[test_type]
    query = test_spec["query"].format(model=model)

    try:
        result = engine.query(query)
        count = result[0][0]

        if test_spec.get("expect_zero"):
            passed = count == 0
            msg = f"Found {count} failing rows" if not passed else "All rows pass"
            return TestResult(
                name=name, model=model, test_type=test_type,
                passed=passed, message=msg, failing_rows=count,
            )

        if test_spec.get("expect_positive"):
            passed = count > 0
            msg = f"Row count: {count}" if passed else "Model is empty"
            return TestResult(
                name=name, model=model, test_type=test_type,
                passed=passed, message=msg,
            )

    except Exception as e:
        return TestResult(
            name=name, model=model, test_type=test_type,
            passed=False, message=f"Error: {e}",
        )

    return TestResult(
        name=name, model=model, test_type=test_type,
        passed=False, message="Test spec misconfigured",
    )


def _run_custom_test(
    engine: SpatialEngine, name: str, model: str, test_def: dict
) -> TestResult:
    """Run a custom SQL test."""
    query = test_def.get("query", "")
    if not query:
        return TestResult(
            name=name, model=model, test_type="custom_sql",
            passed=False, message="No query specified",
        )

    query = query.replace("{{ model }}", f"__model__{model}")

    try:
        result = engine.query(query)
        count = result[0][0] if result else 0
        expect = test_def.get("expect", "zero")

        if expect == "zero":
            passed = count == 0
            msg = f"Found {count} failing rows" if not passed else "All rows pass"
        elif expect == "positive":
            passed = count > 0
            msg = f"Count: {count}" if passed else "Expected positive count, got 0"
        else:
            passed = count == int(expect)
            msg = f"Expected {expect}, got {count}"

        return TestResult(
            name=name, model=model, test_type="custom_sql",
            passed=passed, message=msg, failing_rows=count if not passed else 0,
        )
    except Exception as e:
        return TestResult(
            name=name, model=model, test_type="custom_sql",
            passed=False, message=f"Error: {e}",
        )


def run_all_tests(
    engine: SpatialEngine, tests_dir: Path
) -> list[TestResult]:
    """Run all tests and return results."""
    test_defs = load_tests(tests_dir)
    results = []

    for test_def in test_defs:
        result = run_test(engine, test_def)
        results.append(result)

    return results


def print_test_results(results: list[TestResult]) -> None:
    """Print test results as a table."""
    table = Table(title="Test Results")
    table.add_column("Status", width=6)
    table.add_column("Test", style="bold")
    table.add_column("Model", style="cyan")
    table.add_column("Type", style="dim")
    table.add_column("Message")

    for r in results:
        status = "[green]PASS[/green]" if r.passed else "[red]FAIL[/red]"
        table.add_row(status, r.name, r.model, r.test_type, r.message)

    console.print(table)

    passed = sum(1 for r in results if r.passed)
    failed = sum(1 for r in results if not r.passed)
    total = len(results)

    if failed == 0:
        console.print(f"\n[bold green]All {total} tests passed.[/bold green]")
    else:
        console.print(f"\n[bold red]{failed} of {total} tests failed.[/bold red]")
