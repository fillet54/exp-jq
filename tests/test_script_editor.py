from pathlib import Path

from automationv3.frontend import create_app


def _build_client(tmp_path: Path, monkeypatch):
    db_path = tmp_path / "jobqueue.db"
    scripts_root = tmp_path / "scripts"
    suites_dir = tmp_path / "suites"
    cache_dir = tmp_path / ".fscache_scripts"
    scripts_root.mkdir(parents=True, exist_ok=True)
    suites_dir.mkdir(parents=True, exist_ok=True)
    cache_dir.mkdir(parents=True, exist_ok=True)

    monkeypatch.setenv("JOBQUEUE_DB", str(db_path))
    monkeypatch.setenv("SCRIPT_ROOT", str(scripts_root))
    monkeypatch.setenv("SUITES_DIR", str(suites_dir))
    monkeypatch.setenv("SCRIPT_CACHE_DIR", str(cache_dir))
    monkeypatch.setenv("JOBQUEUE_DOCS_ENABLED", "0")

    app = create_app()
    app.testing = True
    return app.test_client(), scripts_root


def test_script_raw_page_renders_editor(tmp_path: Path, monkeypatch) -> None:
    client, scripts_root = _build_client(tmp_path, monkeypatch)
    script_path = scripts_root / "demo.rst"
    script_path.write_text(
        "Demo\n====\n\nBody.\n",
        encoding="utf-8",
    )

    resp = client.get("/scripts/demo.rst?view=raw")

    assert resp.status_code == 200
    body = resp.get_data(as_text=True)
    assert 'id="script-editor"' in body
    assert "codemirror.min.js" in body.lower()
    assert "Save Script" in body


def test_script_post_saves_updated_content(tmp_path: Path, monkeypatch) -> None:
    client, scripts_root = _build_client(tmp_path, monkeypatch)
    script_path = scripts_root / "editable.rst"
    script_path.write_text(
        "Editable\n========\n\nBefore.\n",
        encoding="utf-8",
    )

    resp = client.post(
        "/scripts/editable.rst?view=raw",
        data={
            "script_content": "Editable\n========\n\nAfter.\n",
        },
        follow_redirects=False,
    )

    assert resp.status_code == 303
    assert "saved=1" in resp.headers["Location"]
    assert script_path.read_text(encoding="utf-8") == "Editable\n========\n\nAfter.\n"
