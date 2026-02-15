from pathlib import Path

import pytest

from automationv3.framework.requirements import (
    REQUIREMENT_ID_PATTERN,
    Requirement,
    default_requirements_csv_path,
    load_default_requirements,
    load_requirements_from_csv,
)


def test_default_csv_path_exists():
    path = default_requirements_csv_path()
    assert path.exists()
    assert path.name == "space_controller_requirements.csv"


def test_load_default_requirements_returns_embedded_controller_data():
    requirements = load_default_requirements()

    assert len(requirements) >= 10
    for req in requirements:
        assert isinstance(req, Requirement)
        assert REQUIREMENT_ID_PATTERN.fullmatch(req.id)
        assert req.specification_id == "ECS"
        assert 3 <= len(req.system_id) <= 4
        assert req.traceability_links


def test_requirement_sequence_comes_from_id():
    req = Requirement(
        id="ECSCTRL00123",
        specification_id="ECS",
        system_id="CTRL",
        text="Sample requirement.",
        traceability_links=("ECS-1.1",),
    )
    assert req.sequence == 123


def test_load_requirements_from_csv_parses_traceability_links(tmp_path: Path):
    csv_path = tmp_path / "reqs.csv"
    csv_path.write_text(
        "\n".join(
            [
                "id,specification-id,text,system-id,traceability-links",
                "ECSCOM00001,ECS,Command shall be acknowledged.,COM,ECS-5.1;TEST-COM-123;ANALYSIS-7",
            ]
        ),
        encoding="utf-8",
    )

    requirements = load_requirements_from_csv(csv_path)
    assert len(requirements) == 1
    assert requirements[0].traceability_links == (
        "ECS-5.1",
        "TEST-COM-123",
        "ANALYSIS-7",
    )


def test_load_requirements_from_csv_rejects_invalid_id_format(tmp_path: Path):
    csv_path = tmp_path / "invalid.csv"
    csv_path.write_text(
        "\n".join(
            [
                "id,specification-id,text,system-id,traceability-links",
                "BAD-REQ-ID,ECS,Invalid id sample.,CTRL,ECS-1.0",
            ]
        ),
        encoding="utf-8",
    )

    with pytest.raises(ValueError, match="Invalid requirement id format"):
        load_requirements_from_csv(csv_path)


def test_load_requirements_from_csv_rejects_mismatched_spec_prefix(tmp_path: Path):
    csv_path = tmp_path / "mismatch.csv"
    csv_path.write_text(
        "\n".join(
            [
                "id,specification-id,text,system-id,traceability-links",
                "ABCCTRL00001,ECS,Spec mismatch sample.,CTRL,ECS-1.0",
            ]
        ),
        encoding="utf-8",
    )

    with pytest.raises(ValueError, match="specification-id"):
        load_requirements_from_csv(csv_path)


def test_load_requirements_from_csv_rejects_missing_columns(tmp_path: Path):
    csv_path = tmp_path / "missing_cols.csv"
    csv_path.write_text(
        "\n".join(
            [
                "id,specification-id,text,system-id",
                "ECSCTRL00001,ECS,Column test.,CTRL",
            ]
        ),
        encoding="utf-8",
    )

    with pytest.raises(ValueError, match="missing required columns"):
        load_requirements_from_csv(csv_path)
