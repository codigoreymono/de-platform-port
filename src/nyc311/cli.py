"""Explicit local commands. Sample always uses an in-memory HTTP fixture transport."""

import argparse
import json
import re
from collections.abc import Sequence
from datetime import date, datetime
from pathlib import Path

import httpx

from nyc311.config import execution_config
from nyc311.pipeline import run_pipeline
from nyc311.source import create_client
from nyc311.storage import object_path, output_root, read_json

DEFAULT_FIXTURE = Path(__file__).resolve().parents[2] / "data/samples/gold-metrics.json"


def fixture_transport(path: Path) -> httpx.MockTransport:
    """Simulate only source count/page responses; extraction still belongs to HTTPX source.py."""
    records = json.loads(path.read_bytes())
    if not isinstance(records, list) or not all(
        isinstance(row, dict) for row in records
    ):
        raise ValueError("Fixture must contain an array of records")
    dated = []
    for row in records:
        if not isinstance(row.get("unique_key"), str) or not row["unique_key"].strip():
            raise ValueError("Fixture identifiers must be nonempty text")
        created = datetime.fromisoformat(row["created_date"])
        if created.tzinfo is not None:
            raise ValueError("Fixture creation times must be floating NYC timestamps")
        dated.append((created, row))

    def respond(request: httpx.Request) -> httpx.Response:
        params = request.url.params
        bounds = re.findall(r"created_date (?:>=|<) '([^']+)'", params["$where"])
        if len(bounds) != 2:
            return httpx.Response(400, json={"error": "unsupported fixture query"})
        start, end = [datetime.fromisoformat(value) for value in bounds]
        selected = [row for created, row in dated if start <= created < end]
        if params["$select"] == "count(*) as n":
            return httpx.Response(200, json=[{"n": str(len(selected))}])
        cursor = re.search(r"unique_key > '((?:''|[^'])*)'", params["$where"])
        if cursor is not None:
            last = cursor.group(1).replace("''", "'")
            selected = [row for row in selected if row["unique_key"] > last]
        selected.sort(key=lambda row: row["unique_key"])
        return httpx.Response(200, json=selected[: int(params["$limit"])])

    return httpx.MockTransport(respond)


def _failure_result(
    args: argparse.Namespace, error: BaseException, existing_record: bool
) -> dict[str, object]:
    result = {"status": "failed", "run_id": args.run_id, "error": type(error).__name__}
    # Do not present a previous invocation's successful record on run-ID collision.
    if existing_record or not re.fullmatch(r"[A-Za-z0-9][A-Za-z0-9_-]*", args.run_id):
        return result
    key = f"control/runs/{args.run_id}.json"
    try:
        record = read_json(output_root(args.base, args.mode), key)
        if (
            record["kind"] != "run"
            or record["run_id"] != args.run_id
            or record["mode"] != args.mode
        ):
            return result
    except (KeyError, ValueError, TypeError, OSError):
        return result
    record.update(result)
    record["run_manifest_key"] = key
    return record


def main(argv: Sequence[str] | None = None) -> int:
    parser = argparse.ArgumentParser(
        description="Manual NYC 311 local pipeline. One invocation at a time; no readers during refresh."
    )
    commands = parser.add_subparsers(dest="command", required=True)
    for name in ("sample", "extract", "replay"):
        help_text = {
            "sample": "offline fixture pipeline",
            "extract": "explicit live extraction",
            "replay": "offline complete-Bronze replay",
        }
        command = commands.add_parser(name, help=help_text[name])
        command.add_argument("--base", type=Path, default=Path("data/local"))
        command.add_argument("--run-id", required=True)
        command.add_argument(
            "--start",
            type=date.fromisoformat,
            default=date(2026, 9, 1),
            required=name == "replay",
        )
        command.add_argument(
            "--end",
            type=date.fromisoformat,
            default=date(2026, 9, 3) if name == "sample" else date(2026, 10, 1),
            required=name == "replay",
            help="exclusive end date",
        )
        command.add_argument("--page-size", type=int, default=5000)
        command.add_argument("--max-attempts", type=int, default=5)
        if name == "sample":
            command.add_argument("--fixture", type=Path, default=DEFAULT_FIXTURE)
            command.set_defaults(mode="sample")
        else:
            command.add_argument(
                "--mode", choices=("sample", "published"), default="sample"
            )
            if name == "extract":
                command.add_argument(
                    "--live", action="store_true", help="required opt-in for real HTTP"
                )
            else:
                command.add_argument(
                    "--bronze",
                    nargs="+",
                    required=True,
                    help="complete manifest keys relative to the selected mode root",
                )
    args = parser.parse_args(argv)
    if args.command == "extract" and not args.live:
        parser.error(
            "extract requires --live; use sample or offline Bronze replay instead"
        )
    existing_record = False
    try:
        existing_record = object_path(
            output_root(args.base, args.mode), f"control/runs/{args.run_id}.json"
        ).exists()
        settings = execution_config(
            mode=args.mode, page_size=args.page_size, max_attempts=args.max_attempts
        )
        if args.command == "sample":
            with create_client(
                settings, app_token="", transport=fixture_transport(args.fixture)
            ) as client:
                result = run_pipeline(
                    args.base,
                    args.run_id,
                    args.start,
                    args.end,
                    config=settings,
                    client=client,
                )
        elif args.command == "replay":
            result = run_pipeline(
                args.base,
                args.run_id,
                args.start,
                args.end,
                config=settings,
                bronze_keys=args.bronze,
            )
        else:
            result = run_pipeline(
                args.base, args.run_id, args.start, args.end, config=settings
            )
    except KeyboardInterrupt as error:
        print(json.dumps(_failure_result(args, error, existing_record), indent=2))
        return 130
    except Exception as error:
        print(json.dumps(_failure_result(args, error, existing_record), indent=2))
        return 1
    print(json.dumps(result, indent=2))
    return 0
