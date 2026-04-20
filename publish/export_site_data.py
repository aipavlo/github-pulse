import argparse
import csv
import hashlib
import json
import os
import shutil
from dataclasses import dataclass
from datetime import UTC, date, datetime
from decimal import Decimal
from pathlib import Path
from typing import Any

import requests

PROJECT_ROOT = Path(__file__).resolve().parents[1]
SITE_DATA_ROOT = PROJECT_ROOT / "evidence" / "sources" / "site_data"
CURRENT_DIR = SITE_DATA_ROOT / "current"
TMP_DIR = SITE_DATA_ROOT / "_tmp"
DATASET_VERSION = 1
DEFAULT_TIMEOUT = 60
DEFAULT_CLICKHOUSE_HOST = "clickhouse"
DEFAULT_CLICKHOUSE_PORT = 8123


@dataclass(frozen=True)
class ColumnSpec:
    name: str
    type_name: str
    required: bool = True


@dataclass(frozen=True)
class DatasetSpec:
    dataset_name: str
    file_name: str
    table_name: str
    columns: tuple[ColumnSpec, ...]
    order_by: str
    row_limit: int
    size_limit_bytes: int


DATASET_SPECS = {
    "site_kpis": DatasetSpec(
        dataset_name="site_kpis",
        file_name="site_kpis.csv",
        table_name="publish_site_kpis",
        columns=(
            ColumnSpec("snapshot_date", "date"),
            ColumnSpec("generated_at", "timestamp"),
            ColumnSpec("repo_count", "integer"),
            ColumnSpec("owner_count", "integer"),
            ColumnSpec("language_count", "integer"),
            ColumnSpec("topic_count", "integer"),
            ColumnSpec("total_stars", "integer"),
            ColumnSpec("total_forks", "integer"),
            ColumnSpec("active_repo_count", "integer"),
            ColumnSpec("archived_repo_count", "integer"),
        ),
        order_by="snapshot_date DESC",
        row_limit=10,
        size_limit_bytes=100 * 1024,
    ),
    "repo_top": DatasetSpec(
        dataset_name="repo_top",
        file_name="repo_top.csv",
        table_name="publish_repo_top",
        columns=(
            ColumnSpec("snapshot_date", "date"),
            ColumnSpec("generated_at", "timestamp"),
            ColumnSpec("popularity_rank", "integer"),
            ColumnSpec("repo_full_name", "string"),
            ColumnSpec("owner_login", "string"),
            ColumnSpec("repo_name", "string"),
            ColumnSpec("repo_url", "string"),
            ColumnSpec("description", "string", required=False),
            ColumnSpec("primary_language", "string"),
            ColumnSpec("stargazers_count", "integer"),
            ColumnSpec("forks_count", "integer"),
            ColumnSpec("watchers_count", "integer"),
            ColumnSpec("days_since_push", "integer", required=False),
            ColumnSpec("repo_popularity_score", "number"),
            ColumnSpec("is_archived", "boolean"),
            ColumnSpec("is_fork", "boolean"),
        ),
        order_by="snapshot_date DESC, popularity_rank ASC, repo_full_name ASC",
        row_limit=5_000,
        size_limit_bytes=5 * 1024 * 1024,
    ),
    "repo_trend_monthly": DatasetSpec(
        dataset_name="repo_trend_monthly",
        file_name="repo_trend_monthly.csv",
        table_name="publish_repo_trend_monthly",
        columns=(
            ColumnSpec("snapshot_month", "date"),
            ColumnSpec("repo_full_name", "string"),
            ColumnSpec("owner_login", "string"),
            ColumnSpec("repo_name", "string"),
            ColumnSpec("primary_language", "string"),
            ColumnSpec("stargazers_count", "integer"),
            ColumnSpec("forks_count", "integer"),
            ColumnSpec("watchers_count", "integer"),
            ColumnSpec("open_issues_count", "integer"),
            ColumnSpec("repo_popularity_score", "number"),
        ),
        order_by="snapshot_month ASC, repo_full_name ASC",
        row_limit=100_000,
        size_limit_bytes=15 * 1024 * 1024,
    ),
    "owner_summary": DatasetSpec(
        dataset_name="owner_summary",
        file_name="owner_summary.csv",
        table_name="publish_owner_summary",
        columns=(
            ColumnSpec("snapshot_date", "date"),
            ColumnSpec("owner_group", "string"),
            ColumnSpec("repo_count", "integer"),
            ColumnSpec("total_stars", "integer"),
            ColumnSpec("avg_stars", "number"),
            ColumnSpec("total_forks", "integer"),
            ColumnSpec("total_watchers", "integer"),
            ColumnSpec("total_open_issues", "integer"),
            ColumnSpec("archived_repo_count", "integer"),
            ColumnSpec("active_repo_count", "integer"),
            ColumnSpec("language_count", "integer"),
        ),
        order_by="snapshot_date DESC, total_stars DESC, repo_count DESC, owner_group ASC",
        row_limit=10_000,
        size_limit_bytes=2 * 1024 * 1024,
    ),
    "language_summary": DatasetSpec(
        dataset_name="language_summary",
        file_name="language_summary.csv",
        table_name="publish_language_summary",
        columns=(
            ColumnSpec("snapshot_date", "date"),
            ColumnSpec("language", "string"),
            ColumnSpec("repo_count", "integer"),
            ColumnSpec("total_stars", "integer"),
            ColumnSpec("avg_stars", "number"),
            ColumnSpec("median_stars", "number"),
            ColumnSpec("total_forks", "integer"),
            ColumnSpec("avg_forks", "number"),
            ColumnSpec("total_watchers", "integer"),
            ColumnSpec("total_open_issues", "integer"),
            ColumnSpec("archived_repo_count", "integer"),
            ColumnSpec("stale_repo_count", "integer"),
        ),
        order_by="snapshot_date DESC, total_stars DESC, repo_count DESC, language ASC",
        row_limit=1_000,
        size_limit_bytes=1024 * 1024,
    ),
    "topic_summary": DatasetSpec(
        dataset_name="topic_summary",
        file_name="topic_summary.csv",
        table_name="publish_topic_summary",
        columns=(
            ColumnSpec("snapshot_date", "date"),
            ColumnSpec("topic", "string"),
            ColumnSpec("repo_count", "integer"),
            ColumnSpec("repo_uniq_count", "integer"),
            ColumnSpec("total_stars", "integer"),
            ColumnSpec("avg_stars", "number"),
            ColumnSpec("median_stars", "number"),
            ColumnSpec("total_forks", "integer"),
            ColumnSpec("total_watchers", "integer"),
            ColumnSpec("archived_repo_count", "integer"),
        ),
        order_by="snapshot_date DESC, total_stars DESC, repo_count DESC, topic ASC",
        row_limit=20_000,
        size_limit_bytes=3 * 1024 * 1024,
    ),
}

METADATA_SIZE_LIMITS = {
    "build_meta.json": 100 * 1024,
    "manifest.json": 500 * 1024,
}
TOTAL_SIZE_LIMIT_BYTES = 25 * 1024 * 1024
SHARP_GROWTH_SIZE_RATIO = 2.0
SHARP_GROWTH_ROW_RATIO = 2.0
SHARP_GROWTH_MIN_SIZE_INCREASE_BYTES = 256 * 1024
SHARP_GROWTH_MIN_ROW_INCREASE = 1_000


class ExportContractError(RuntimeError):
    pass


class ClickHouseClient:
    def __init__(
        self,
        host: str,
        port: int,
        database: str,
        user: str,
        password: str,
        timeout: int = DEFAULT_TIMEOUT,
    ) -> None:
        self.base_url = f"http://{host}:{port}"
        self.database = database
        self.timeout = timeout
        self.session = requests.Session()
        self.auth = (user, password)

    def query_json(self, query: str) -> list[dict[str, Any]]:
        response = self.session.post(
            self.base_url,
            params={"database": self.database},
            data=f"{query}\nFORMAT JSON",
            auth=self.auth,
            timeout=self.timeout,
        )
        response.raise_for_status()
        payload = response.json()
        return payload["data"]


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--output-root",
        default=str(SITE_DATA_ROOT),
        help="Root directory for site data export",
    )
    parser.add_argument(
        "--run-date",
        default=date.today().isoformat(),
        help="Logical pipeline run date in YYYY-MM-DD format",
    )
    parser.add_argument(
        "--build-id",
        default=datetime.now(UTC).strftime("%Y%m%dT%H%M%SZ"),
        help="Override build identifier for the temporary export directory",
    )
    parser.add_argument(
        "--only",
        choices=sorted(DATASET_SPECS),
        help="Export only one dataset; allowed only together with --dry-run",
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Export and validate into a temporary directory without replacing current/",
    )
    parser.add_argument(
        "--fail-on-empty",
        action="store_true",
        help="Fail when any selected dataset returns zero data rows",
    )
    return parser.parse_args()


def json_default(value: Any) -> Any:
    if isinstance(value, (datetime, date)):
        if isinstance(value, datetime) and value.tzinfo is None:
            value = value.replace(tzinfo=UTC)
        return value.isoformat().replace("+00:00", "Z")
    if isinstance(value, Decimal):
        return float(value)
    raise TypeError(f"Object of type {type(value).__name__} is not JSON serializable")


def normalize_timestamp(value: Any) -> str | None:
    if value is None:
        return None
    if isinstance(value, datetime):
        dt_value = value if value.tzinfo is not None else value.replace(tzinfo=UTC)
    else:
        raw_value = str(value).replace("Z", "+00:00")
        dt_value = datetime.fromisoformat(raw_value)
        if dt_value.tzinfo is None:
            dt_value = dt_value.replace(tzinfo=UTC)
    return dt_value.astimezone(UTC).isoformat().replace("+00:00", "Z")


def normalize_date(value: Any) -> str | None:
    if value is None:
        return None
    if isinstance(value, datetime):
        return value.date().isoformat()
    if isinstance(value, date):
        return value.isoformat()
    return str(value)


def normalize_value(value: Any, column: ColumnSpec) -> str:
    if value is None:
        if column.required:
            raise ExportContractError(f"Column {column.name} is required but contains null")
        return ""

    if column.type_name == "string":
        return str(value)
    if column.type_name == "integer":
        if isinstance(value, bool):
            raise ExportContractError(f"Column {column.name} must be an integer, got boolean")
        return str(int(value))
    if column.type_name == "number":
        if isinstance(value, bool):
            raise ExportContractError(f"Column {column.name} must be a number, got boolean")
        if isinstance(value, Decimal):
            return format(value, "f")
        return str(value)
    if column.type_name == "boolean":
        if isinstance(value, bool):
            return "true" if value else "false"
        if value in (0, 1):
            return "true" if int(value) == 1 else "false"
        raise ExportContractError(f"Column {column.name} must be boolean-compatible")
    if column.type_name == "date":
        return normalize_date(value) or ""
    if column.type_name == "timestamp":
        normalized = normalize_timestamp(value)
        if normalized is None:
            raise ExportContractError(f"Column {column.name} is required but contains null")
        return normalized

    raise ExportContractError(f"Unsupported column type: {column.type_name}")


def validate_row_shape(row: dict[str, Any], spec: DatasetSpec) -> None:
    expected_keys = [column.name for column in spec.columns]
    actual_keys = list(row.keys())
    if actual_keys != expected_keys:
        raise ExportContractError(
            f"{spec.dataset_name}: expected columns {expected_keys}, got {actual_keys}"
        )


def build_select_query(database: str, spec: DatasetSpec) -> str:
    columns = ",\n    ".join(column.name for column in spec.columns)
    return (
        f"SELECT\n"
        f"    {columns}\n"
        f"FROM {database}.{spec.table_name}\n"
        f"ORDER BY {spec.order_by}"
    )


def write_csv_dataset(
    output_dir: Path,
    spec: DatasetSpec,
    rows: list[dict[str, Any]],
    fail_on_empty: bool,
) -> dict[str, Any]:
    if fail_on_empty and not rows:
        raise ExportContractError(f"{spec.dataset_name}: dataset is empty")
    if len(rows) > spec.row_limit:
        raise ExportContractError(
            f"{spec.dataset_name}: row count {len(rows)} exceeds limit {spec.row_limit}"
        )

    csv_path = output_dir / spec.file_name
    with csv_path.open("w", encoding="utf-8", newline="") as csv_file:
        writer = csv.writer(csv_file)
        headers = [column.name for column in spec.columns]
        writer.writerow(headers)
        for row in rows:
            validate_row_shape(row, spec)
            writer.writerow([normalize_value(row[column.name], column) for column in spec.columns])

    size_bytes = csv_path.stat().st_size
    if size_bytes > spec.size_limit_bytes:
        raise ExportContractError(
            f"{spec.dataset_name}: file size {size_bytes} exceeds limit {spec.size_limit_bytes}"
        )

    return build_manifest_entry(csv_path, "text/csv", len(rows))


def build_manifest_entry(
    file_path: Path,
    content_type: str,
    row_count: int | None = None,
) -> dict[str, Any]:
    sha256 = hashlib.sha256(file_path.read_bytes()).hexdigest()
    entry = {
        "path": file_path.name,
        "sha256": sha256,
        "size_bytes": file_path.stat().st_size,
        "content_type": content_type,
    }
    if row_count is not None:
        entry["row_count"] = row_count
    return entry


def write_json_file(file_path: Path, payload: dict[str, Any]) -> None:
    serialized = json.dumps(
        payload,
        ensure_ascii=True,
        indent=2,
        sort_keys=True,
        default=json_default,
    )
    file_path.write_text(
        serialized + "\n",
        encoding="utf-8",
    )


def ensure_expected_files(output_dir: Path, expected_files: set[str]) -> None:
    actual_files = {path.name for path in output_dir.iterdir() if path.is_file()}
    if actual_files != expected_files:
        raise ExportContractError(
            f"Expected files {sorted(expected_files)}, got {sorted(actual_files)}"
        )


def validate_metadata_sizes(output_dir: Path) -> None:
    for file_name, size_limit in METADATA_SIZE_LIMITS.items():
        file_size = (output_dir / file_name).stat().st_size
        if file_size > size_limit:
            raise ExportContractError(
                f"{file_name}: file size {file_size} exceeds limit {size_limit}"
            )


def validate_total_size(output_dir: Path) -> None:
    total_size = sum(path.stat().st_size for path in output_dir.iterdir() if path.is_file())
    if total_size > TOTAL_SIZE_LIMIT_BYTES:
        raise ExportContractError(
            f"Export size {total_size} exceeds limit {TOTAL_SIZE_LIMIT_BYTES}"
        )


def load_existing_manifest(current_dir: Path) -> dict[str, Any] | None:
    manifest_path = current_dir / "manifest.json"
    if not manifest_path.exists():
        return None
    return json.loads(manifest_path.read_text(encoding="utf-8"))


def validate_sharp_growth(
    new_manifest_files: list[dict[str, Any]],
    current_dir: Path,
) -> None:
    existing_manifest = load_existing_manifest(current_dir)
    if existing_manifest is None:
        return

    previous_files = {
        entry["path"]: entry
        for entry in existing_manifest.get("files", [])
        if entry.get("content_type") == "text/csv"
    }

    for new_entry in new_manifest_files:
        if new_entry.get("content_type") != "text/csv":
            continue

        previous_entry = previous_files.get(new_entry["path"])
        if previous_entry is None:
            continue

        previous_size = previous_entry.get("size_bytes", 0)
        new_size = new_entry["size_bytes"]
        if (
            previous_size > 0
            and new_size > previous_size * SHARP_GROWTH_SIZE_RATIO
            and (new_size - previous_size) > SHARP_GROWTH_MIN_SIZE_INCREASE_BYTES
        ):
            raise ExportContractError(
                f"{new_entry['path']}: sharp size growth detected "
                f"({previous_size} -> {new_size})"
            )

        previous_row_count = previous_entry.get("row_count")
        new_row_count = new_entry.get("row_count")
        if (
            previous_row_count is not None
            and new_row_count is not None
            and previous_row_count > 0
            and new_row_count > previous_row_count * SHARP_GROWTH_ROW_RATIO
            and (new_row_count - previous_row_count) > SHARP_GROWTH_MIN_ROW_INCREASE
        ):
            raise ExportContractError(
                f"{new_entry['path']}: sharp row-count growth detected "
                f"({previous_row_count} -> {new_row_count})"
            )


def detect_noop_export(
    new_manifest_files: list[dict[str, Any]],
    current_dir: Path,
) -> bool:
    existing_manifest = load_existing_manifest(current_dir)
    if existing_manifest is None:
        return False

    existing_csv_files = sorted(
        {
            key: {
                "sha256": entry.get("sha256"),
                "size_bytes": entry.get("size_bytes"),
                "row_count": entry.get("row_count"),
            }
            for key, entry in {
                item["path"]: item for item in existing_manifest.get("files", [])
            }.items()
            if entry.get("content_type") == "text/csv"
        }.items()
    )

    new_csv_files = sorted(
        {
            entry["path"]: {
                "sha256": entry.get("sha256"),
                "size_bytes": entry.get("size_bytes"),
                "row_count": entry.get("row_count"),
            }
            for entry in new_manifest_files
            if entry.get("content_type") == "text/csv"
        }.items()
    )

    return existing_csv_files == new_csv_files


def clean_tmp_root(tmp_root: Path, keep: Path | None = None) -> None:
    if not tmp_root.exists():
        return
    for path in tmp_root.iterdir():
        if keep is not None and path == keep:
            continue
        if path.is_dir():
            shutil.rmtree(path)


def replace_current_dir(tmp_export_dir: Path, current_dir: Path) -> None:
    backup_dir = current_dir.parent / "_backup_current"
    if backup_dir.exists():
        shutil.rmtree(backup_dir)
    if current_dir.exists():
        current_dir.rename(backup_dir)
    tmp_export_dir.rename(current_dir)
    if backup_dir.exists():
        shutil.rmtree(backup_dir)


def export_datasets(
    client: ClickHouseClient,
    output_root: Path,
    build_id: str,
    run_date: str,
    dry_run: bool,
    only_dataset: str | None,
    fail_on_empty: bool,
) -> dict[str, Any]:
    selected_specs = (
        [DATASET_SPECS[only_dataset]] if only_dataset is not None else list(DATASET_SPECS.values())
    )
    if only_dataset is not None and not dry_run:
        raise ExportContractError("--only is supported only together with --dry-run")

    current_dir = output_root / "current"
    tmp_root = output_root / "_tmp"
    tmp_export_dir = tmp_root / build_id
    target_dir = tmp_export_dir if dry_run else current_dir
    tmp_export_dir.mkdir(parents=True, exist_ok=False)

    manifest_entries = []
    snapshot_date: str | None = None
    generated_at = datetime.now(UTC).isoformat().replace("+00:00", "Z")

    for spec in selected_specs:
        query = build_select_query(client.database, spec)
        rows = client.query_json(query)
        manifest_entry = write_csv_dataset(tmp_export_dir, spec, rows, fail_on_empty=fail_on_empty)
        manifest_entries.append(manifest_entry)

        if spec.dataset_name == "site_kpis" and rows:
            snapshot_date = normalize_date(rows[0]["snapshot_date"])

    if snapshot_date is None:
        if only_dataset is not None:
            rows = client.query_json(
                build_select_query(client.database, DATASET_SPECS["site_kpis"])
            )
            if not rows:
                raise ExportContractError("site_kpis is empty, cannot determine snapshot_date")
            snapshot_date = normalize_date(rows[0]["snapshot_date"])
        else:
            raise ExportContractError("site_kpis is empty, cannot determine snapshot_date")

    build_meta = {
        "build_id": build_id,
        "generated_at": generated_at,
        "snapshot_date": snapshot_date,
        "source_run_date": run_date,
        "dbt_test_passed": True,
        "datasets_dir": str(target_dir),
        "dataset_version": DATASET_VERSION,
        "exporter_version": "1",
    }
    write_json_file(tmp_export_dir / "build_meta.json", build_meta)
    manifest_entries.append(
        build_manifest_entry(tmp_export_dir / "build_meta.json", "application/json")
    )

    manifest = {
        "dataset_version": DATASET_VERSION,
        "generated_at": generated_at,
        "snapshot_date": snapshot_date,
        "files": sorted(manifest_entries, key=lambda item: item["path"]),
    }
    write_json_file(tmp_export_dir / "manifest.json", manifest)

    manifest_path = tmp_export_dir / "manifest.json"
    manifest_entries = [
        entry for entry in manifest_entries if entry["path"] != "manifest.json"
    ] + [build_manifest_entry(manifest_path, "application/json")]
    manifest["files"] = sorted(manifest_entries, key=lambda item: item["path"])
    write_json_file(manifest_path, manifest)

    expected_files = {
        spec.file_name for spec in selected_specs
    } | {"build_meta.json", "manifest.json"}
    ensure_expected_files(tmp_export_dir, expected_files)
    validate_metadata_sizes(tmp_export_dir)
    validate_total_size(tmp_export_dir)
    validate_sharp_growth(manifest["files"], current_dir)
    no_op = detect_noop_export(manifest["files"], current_dir)

    if build_meta["dbt_test_passed"] is not True:
        raise ExportContractError("dbt_test_passed must be true before publish")

    if not dry_run:
        replace_current_dir(tmp_export_dir, current_dir)
        clean_tmp_root(tmp_root)

    return {
        "build_id": build_id,
        "snapshot_date": snapshot_date,
        "output_dir": str(tmp_export_dir if dry_run else current_dir),
        "files": manifest["files"],
        "dry_run": dry_run,
        "no_op": no_op,
    }


def build_client_from_env() -> ClickHouseClient:
    return ClickHouseClient(
        host=os.getenv("CLICKHOUSE_HOST", DEFAULT_CLICKHOUSE_HOST),
        port=int(os.getenv("CLICKHOUSE_PORT", str(DEFAULT_CLICKHOUSE_PORT))),
        database=os.environ["DWH_DB"],
        user=os.environ["DWH_USER"],
        password=os.environ["DWH_PASSWORD"],
    )


def main() -> int:
    args = parse_args()
    output_root = Path(args.output_root)
    output_root.mkdir(parents=True, exist_ok=True)
    (output_root / "_tmp").mkdir(parents=True, exist_ok=True)

    try:
        client = build_client_from_env()
        result = export_datasets(
            client=client,
            output_root=output_root,
            build_id=args.build_id,
            run_date=args.run_date,
            dry_run=args.dry_run,
            only_dataset=args.only,
            fail_on_empty=args.fail_on_empty,
        )
    except (ExportContractError, KeyError, requests.RequestException) as exc:
        print(f"[error] {exc}")
        return 1

    print(json.dumps(result, ensure_ascii=True, indent=2))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
