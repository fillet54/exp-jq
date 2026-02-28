from pathlib import Path
from typing import List


class SuiteManager:
    """Manage suites of scripts stored as plaintext files."""

    def __init__(self, base_dir: Path) -> None:
        self.base_dir = Path(base_dir)
        self.base_dir.mkdir(parents=True, exist_ok=True)

    def _suite_path(self, name: str) -> Path:
        safe = name.replace("/", "_").replace("\\", "_")
        return self.base_dir / f"{safe}.suite"

    def list_suites(self) -> List[str]:
        return sorted(p.stem for p in self.base_dir.glob("*.suite"))

    def get_suite(self, name: str) -> List[str]:
        path = self._suite_path(name)
        if not path.exists():
            return []
        return [
            line.strip()
            for line in path.read_text().splitlines()
            if line.strip() and not line.strip().startswith("#")
        ]

    def create_suite(self, name: str) -> None:
        path = self._suite_path(name)
        path.touch(exist_ok=True)

    def delete_suite(self, name: str) -> None:
        path = self._suite_path(name)
        try:
            path.unlink()
        except FileNotFoundError:
            pass

    def add_script(self, name: str, script_relpath: str) -> None:
        path = self._suite_path(name)
        lines = self.get_suite(name)
        if script_relpath not in lines:
            lines.append(script_relpath)
            path.write_text("\n".join(lines) + "\n")

    def remove_script(self, name: str, script_relpath: str) -> None:
        lines = [p for p in self.get_suite(name) if p != script_relpath]
        path = self._suite_path(name)
        path.write_text("\n".join(lines) + ("\n" if lines else ""))


__all__ = ["SuiteManager"]
