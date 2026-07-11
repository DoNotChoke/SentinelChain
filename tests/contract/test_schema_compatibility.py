"""Contract tests: every schema on disk must be registrable and BACKWARD-compatible (ADR-001).

This is the gate that stops a breaking schema change from reaching main. It runs against a live
Schema Registry (``make up``); when the registry is unreachable the whole module skips, so the
default unit-test run stays hermetic.

    make up && make register-schemas
    pytest tests/contract -m integration
"""

from __future__ import annotations

import json
from collections.abc import Iterator
from pathlib import Path

import pytest
from confluent_kafka.schema_registry import Schema, SchemaRegistryClient
from confluent_kafka.schema_registry.error import SchemaRegistryError

from sentinelchain_common.config import BaseServiceSettings

pytestmark = pytest.mark.integration

REPO_ROOT = Path(__file__).resolve().parents[2]
SCHEMA_DIR = REPO_ROOT / "schemas" / "avro"
EXPECTED_COMPATIBILITY = "BACKWARD"

SCHEMA_FILES = sorted(SCHEMA_DIR.glob("*.avsc"))


@pytest.fixture(scope="module")
def client() -> Iterator[SchemaRegistryClient]:
    # `with` matters: the client holds an HTTP session, and an unclosed socket surfaces as a
    # ResourceWarning — which this repo escalates to an error (filterwarnings = ["error"]).
    settings = BaseServiceSettings()
    with SchemaRegistryClient({"url": settings.schema_registry_url}) as registry:
        try:
            registry.get_subjects()
        except Exception as exc:  # any transport failure means "registry not available"
            pytest.skip(f"Schema Registry unreachable at {settings.schema_registry_url}: {exc}")
        yield registry


def _subject(schema_file: Path) -> str:
    """Filename convention: ``<topic>.avsc`` → subject ``<topic>-value``."""
    return f"{schema_file.stem}-value"


@pytest.mark.parametrize("schema_file", SCHEMA_FILES, ids=lambda p: p.stem)
def test_schema_file_is_valid_avro(schema_file: Path) -> None:
    """Catches a malformed .avsc before it ever reaches the registry — no infra needed."""
    import fastavro

    fastavro.parse_schema(json.loads(schema_file.read_text(encoding="utf-8")))


@pytest.mark.parametrize("schema_file", SCHEMA_FILES, ids=lambda p: p.stem)
def test_subject_enforces_backward_compatibility(
    client: SchemaRegistryClient, schema_file: Path
) -> None:
    """A subject left on the registry default would let a breaking change through unnoticed."""
    subject = _subject(schema_file)
    try:
        level = client.get_compatibility(subject)
    except SchemaRegistryError as exc:
        pytest.fail(
            f"subject '{subject}' has no compatibility config ({exc}). "
            "Run `make register-schemas` — it pins the level."
        )
    assert level == EXPECTED_COMPATIBILITY


@pytest.mark.parametrize("schema_file", SCHEMA_FILES, ids=lambda p: p.stem)
def test_local_schema_is_compatible_with_registered_version(
    client: SchemaRegistryClient, schema_file: Path
) -> None:
    """The working-tree schema must be accepted against what is already registered.

    This is the real contract check: it fails when someone removes/renames a payload field
    without bumping the topic version.
    """
    subject = _subject(schema_file)
    schema = Schema(schema_file.read_text(encoding="utf-8"), schema_type="AVRO")

    if subject not in client.get_subjects():
        pytest.skip(f"subject '{subject}' not registered yet — run `make register-schemas`")

    assert client.test_compatibility(subject, schema) is True
