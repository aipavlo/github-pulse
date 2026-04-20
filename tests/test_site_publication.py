import pathlib
import shutil
import subprocess

import pytest
import yaml

PROJECT_ROOT = pathlib.Path(__file__).resolve().parents[1]
EVIDENCE_DIR = PROJECT_ROOT / "evidence"
PAGES_DIR = EVIDENCE_DIR / "pages"
PARTIALS_DIR = EVIDENCE_DIR / "partials"
WORKFLOWS_DIR = PROJECT_ROOT / ".github" / "workflows"


def read_text(path: pathlib.Path) -> str:
    return path.read_text(encoding="utf-8")


def load_yaml(path: pathlib.Path) -> dict:
    return yaml.safe_load(read_text(path))


def workflow_on(workflow: dict) -> dict:
    return workflow.get("on", workflow.get(True, {}))


def test_evidence_local_build_entrypoints_smoke_help():
    if shutil.which("npm") is None:
        pytest.skip("npm is not available in this test environment")

    build_help = subprocess.run(
        ["npm", "run", "build:strict", "--", "--help"],
        cwd=EVIDENCE_DIR,
        check=False,
        capture_output=True,
        text=True,
    )
    assert build_help.returncode == 0
    assert "evidence build:strict" in build_help.stdout

    sources_help = subprocess.run(
        ["npm", "run", "sources", "--", "--help"],
        cwd=EVIDENCE_DIR,
        check=False,
        capture_output=True,
        text=True,
    )
    assert sources_help.returncode == 0
    assert "evidence sources" in sources_help.stdout


def test_homepage_contains_freshness_metrics_and_shared_partials():
    homepage = read_text(PAGES_DIR / "index.md")

    assert "## Snapshot" in homepage
    assert "from site_data.site_freshness" in homepage
    assert '<BigValue data={freshness} value="days_since_snapshot"' in homepage
    assert "<LineChart" in homepage
    assert '{@partial "site_nav.md"}' in homepage
    assert '{@partial "source_support.md"}' in homepage


def test_expected_site_pages_exist_and_use_shared_partials():
    expected_pages = {
        "index.md": "GitHub Pulse",
        "repos.md": "Repositories",
        "languages.md": "Languages",
        "owners.md": "Owners",
        "topics.md": "Topics",
        "trends.md": "Trends",
    }

    for filename, page_title in expected_pages.items():
        page = read_text(PAGES_DIR / filename)
        assert f"title: {page_title}" in page
        assert '{@partial "site_nav.md"}' in page
        assert '{@partial "source_support.md"}' in page


def test_navigation_partial_links_expected_pages():
    nav = read_text(PARTIALS_DIR / "site_nav.md")

    assert "[Overview](./)" in nav
    assert "[Repositories](./repos)" in nav
    assert "[Languages](./languages)" in nav
    assert "[Owners](./owners)" in nav
    assert "[Topics](./topics)" in nav
    assert "[Trends](./trends)" in nav


def test_evidence_config_uses_project_site_base_path():
    config = load_yaml(EVIDENCE_DIR / "evidence.config.yaml")

    assert config["deployment"]["basePath"] == "/github-pulse"
    assert "@evidence-dev/duckdb" in config["plugins"]["datasources"]


def test_build_check_workflow_validates_site_artifact_and_base_path():
    workflow = load_yaml(WORKFLOWS_DIR / "build-check.yml")
    workflow_trigger = workflow_on(workflow)
    build_job = workflow["jobs"]["build-check"]
    steps = build_job["steps"]

    assert "pull_request" in workflow_trigger
    assert "evidence/**" in workflow_trigger["pull_request"]["paths"]
    assert workflow_trigger["push"]["branches"] == ["main"]
    assert "evidence/**" in workflow_trigger["push"]["paths"]
    assert build_job["timeout-minutes"] == 15

    step_names = [step["name"] for step in steps]
    assert step_names == [
        "Checkout repository",
        "Setup Node.js",
        "Use npm 11",
        "Install Evidence dependencies",
        "Build source cache",
        "Build static site",
        "Verify expected Pages artifact",
        "Verify basePath is applied",
        "Verify broken dataset fails strict build",
    ]

    artifact_check = next(
        step for step in steps if step["name"] == "Verify expected Pages artifact"
    )
    assert "evidence/build/index.html" in artifact_check["run"]
    assert "evidence/build/repos/index.html" in artifact_check["run"]
    assert "evidence/build/trends/index.html" in artifact_check["run"]

    base_path_check = next(step for step in steps if step["name"] == "Verify basePath is applied")
    assert '"/github-pulse/' in base_path_check["run"]

    broken_dataset_check = next(
        step for step in steps if step["name"] == "Verify broken dataset fails strict build"
    )
    assert "build_meta.json" in broken_dataset_check["run"]
    assert "npm run build:strict" in broken_dataset_check["run"]


def test_deploy_workflow_publishes_pages_artifact_from_evidence_build():
    workflow = load_yaml(WORKFLOWS_DIR / "deploy-pages.yml")
    workflow_trigger = workflow_on(workflow)
    build_job = workflow["jobs"]["build"]
    deploy_job = workflow["jobs"]["deploy"]
    steps = build_job["steps"]

    assert "workflow_dispatch" in workflow_trigger
    assert workflow_trigger["push"]["tags"] == ["pages-*"]
    assert workflow["permissions"]["pages"] == "write"
    assert workflow["permissions"]["id-token"] == "write"
    assert workflow["concurrency"]["group"] == "github-pages"

    upload_step = next(step for step in steps if step["name"] == "Upload Pages artifact")
    assert upload_step["uses"] == "actions/upload-pages-artifact@v3"
    assert upload_step["with"]["path"] == "evidence/build"

    verify_step = next(step for step in steps if step["name"] == "Verify Pages artifact exists")
    assert "evidence/build/index.html" in verify_step["run"]
    assert "test -d evidence/build" in verify_step["run"]

    deploy_step = deploy_job["steps"][0]
    assert deploy_step["uses"] == "actions/deploy-pages@v4"
    assert deploy_job["needs"] == "build"
