"""TEST ONLY: native spill/limit diagnostics, distinct from deployment benchmarks."""

import argparse
import json
import os
import sys
from pathlib import Path
from tempfile import TemporaryDirectory


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--report", default="/evidence/spill-diagnostic.json")
    parser.add_argument("--only-tiny", action="store_true")
    args = parser.parse_args()
    sys.path.insert(0, "/var/task")
    import duckdb

    from nyc311.lambda_runtime import DUCKDB_LIMITS

    query = """
        SELECT i, repeat(md5(i::VARCHAR), 4) AS value
        FROM range(350000) t(i) ORDER BY value
    """
    report = {"status": "failed", "diagnostic_only": True, "cases": []}
    try:
        cases = (
            (("1MiB", True),) if args.only_tiny else (("256MiB", False), ("1MiB", True))
        )
        for cap, expect_failure in cases:
            with TemporaryDirectory(prefix="nyc311-spill-check-", dir="/tmp") as base:
                spill = Path(base) / "spill"
                config = {
                    **DUCKDB_LIMITS,
                    "memory_limit": "32MiB",
                    "max_temp_directory_size": cap,
                    "temp_directory": str(spill),
                }
                case = {
                    "config": config,
                    "observed_spill_bytes": 0,
                    "observed_allocated_spill_bytes": 0,
                    "rows": 0,
                }
                report["cases"].append(case)
                try:
                    with duckdb.connect(":memory:", config=config) as connection:
                        case["effective"] = connection.execute(
                            "SELECT current_setting('memory_limit'), "
                            "current_setting('max_temp_directory_size'), "
                            "current_setting('threads'), current_setting('temp_directory')"
                        ).fetchone()
                        result = connection.execute(query).fetch_record_batch(2048)
                        for batch in result:
                            case["rows"] += batch.num_rows
                            size = sum(
                                path.stat().st_size
                                for path in spill.rglob("*")
                                if path.is_file()
                            )
                            allocated = sum(
                                path.stat().st_blocks * 512
                                for path in spill.rglob("*")
                                if path.is_file()
                            )
                            case["observed_allocated_spill_bytes"] = max(
                                case["observed_allocated_spill_bytes"], allocated
                            )
                            case["observed_spill_bytes"] = max(
                                case["observed_spill_bytes"], size
                            )
                    assert case["rows"] == 350000
                    if not expect_failure:
                        assert case["observed_spill_bytes"] > 0
                    cap_bytes = (1 if expect_failure else 256) * 1024**2
                    assert case["observed_allocated_spill_bytes"] <= cap_bytes, (
                        "Observed physical native spill exceeds its cap"
                    )
                    case["result"] = "completed_within_observed_spill_cap"
                except duckdb.OutOfMemoryException:
                    if not expect_failure:
                        raise
                    case["result"] = "expected_tiny_spill_cap_rejection"
                case["files_after_connection_close"] = [
                    str(path) for path in spill.rglob("*") if path.is_file()
                ]
                assert not case["files_after_connection_close"]
            assert not Path(base).exists()
            case["workspace_removed"] = True
        assert os.statvfs("/tmp").f_bfree == os.statvfs("/tmp").f_blocks
        report["status"] = "passed"
    finally:
        Path(args.report).write_text(json.dumps(report, indent=2) + "\n")
        print(json.dumps(report, indent=2))


if __name__ == "__main__":
    main()
