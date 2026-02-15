"""Requirement data model and CSV-backed loading utilities."""

from __future__ import annotations

import csv
import re
from dataclasses import dataclass
from pathlib import Path
from typing import Iterable


REQUIREMENT_ID_PATTERN = re.compile(
    r"^(?P<spec>[A-Z]{3})(?P<system>[A-Z]{3,4})(?P<sequence>\d{5})$"
)

CSV_COLUMNS = {
    "id",
    "specification-id",
    "text",
    "system-id",
    "traceability-links",
}


@dataclass(frozen=True)
class Requirement:
    id: str
    specification_id: str
    text: str
    system_id: str
    traceability_links: tuple[str, ...]

    def __post_init__(self):
        match = REQUIREMENT_ID_PATTERN.fullmatch(self.id)
        if not match:
            raise ValueError(
                "Invalid requirement id format "
                f"'{self.id}'. Expected: 3-char spec + 3-4 char system + 5 digits."
            )

        spec = match.group("spec")
        system = match.group("system")
        if spec != self.specification_id:
            raise ValueError(
                f"Requirement id '{self.id}' spec prefix '{spec}' does not match "
                f"specification-id '{self.specification_id}'."
            )
        if system != self.system_id:
            raise ValueError(
                f"Requirement id '{self.id}' system prefix '{system}' does not match "
                f"system-id '{self.system_id}'."
            )
        if not self.text.strip():
            raise ValueError(f"Requirement '{self.id}' text must not be empty.")

    @classmethod
    def from_csv_row(cls, row: dict[str, str]) -> "Requirement":
        return cls(
            id=row["id"].strip(),
            specification_id=row["specification-id"].strip(),
            text=row["text"].strip(),
            system_id=row["system-id"].strip(),
            traceability_links=_parse_traceability_links(row["traceability-links"]),
        )

    @property
    def sequence(self) -> int:
        match = REQUIREMENT_ID_PATTERN.fullmatch(self.id)
        if match is None:
            raise ValueError(f"Invalid requirement id '{self.id}'.")
        return int(match.group("sequence"))


def _parse_traceability_links(raw: str) -> tuple[str, ...]:
    if not raw:
        return ()
    return tuple(part.strip() for part in raw.split(";") if part.strip())


def _validate_columns(fieldnames: Iterable[str] | None):
    available = set(fieldnames or ())
    missing = CSV_COLUMNS - available
    if missing:
        raise ValueError(
            "Requirements CSV is missing required columns: "
            + ", ".join(sorted(missing))
        )


def load_requirements_from_csv(path: str | Path) -> list[Requirement]:
    csv_path = Path(path)
    with csv_path.open("r", encoding="utf-8", newline="") as f:
        reader = csv.DictReader(f)
        _validate_columns(reader.fieldnames)
        return [Requirement.from_csv_row(row) for row in reader]


def default_requirements_csv_path() -> Path:
    return Path(__file__).with_name("data") / "space_controller_requirements.csv"


def load_default_requirements() -> list[Requirement]:
    return load_requirements_from_csv(default_requirements_csv_path())

