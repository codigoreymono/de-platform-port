"""Clone immutable local inputs for 15 isolated ZIP trials, with no downloads.

Default task 8.5 replay uses optimized artifacts and 2 GiB/2 CPU, without loading
current runtime policy. Opt-in --profiled verifies host/ZIP runtime SHA/constants
and uses stage RAM 512/1024/2048 MiB, CPU 0.25/0.5/1, 512 MiB tmp, 900s timeouts.
--artifact-suffix ready selects /tmp/opencode/nyc311-lambda-{stage}-ready.
Historical optimized/bounded selectors remain available, but stale profiled ZIPs
must fail fingerprint validation, never mix old spill policy with current policy.
Each of three repetitions replays the largest-row/JSON days and full-month Gold,
using 234 cloned inputs, 150 Gold seed dependencies and original source clocks.
"""

import argparse
import gzip
import hashlib
import json
import os
import runpy
import shutil
import stat
import subprocess
import time
import zipfile
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
IMAGE = "public.ecr.aws/lambda/python@sha256:471e480c997b3150ab1a4fb3b862554fa45d54a4f4a5cd21e3115b320197b717"
DOCKER = ["docker", "--host", "unix:///var/run/docker.sock"]
DAYS = [f"2026-09-{day:02d}" for day in range(1, 31)]
RUN = "september-2026-001"


def digest(path):
    with path.open("rb") as stream:
        return hashlib.file_digest(stream, "sha256").hexdigest()


def snapshot(root):
    result = {}
    for path in sorted(root.rglob("*")):
        if path.is_symlink():
            raise ValueError("Input tree must not contain symlinks")
        if path.is_file():
            result[path.relative_to(root).as_posix()] = digest(path)
    return result


def load_profiles():
    """Execute only the stdlib-only resource module, never the host application."""
    path = ROOT / "src/nyc311/lambda_runtime.py"
    fingerprint = digest(path)
    namespace = runpy.run_path(str(path))
    constants = {key: value for key, value in namespace.items() if key.isupper()}
    profiles = constants["PROFILES"]
    if set(profiles) != {"ingest", "curate", "gold"}:
        raise ValueError("Expected exactly three runtime profiles")
    for profile in profiles.values():
        if (
            set(profile)
            != {
                "memory_mib",
                "timeout_seconds",
                "temporary_storage_mib",
                "benchmark_cpus",
            }
            or profile["memory_mib"] not in (512, 1024, 2048)
            or profile["timeout_seconds"] != 900
            or profile["temporary_storage_mib"] != 512
            or profile["benchmark_cpus"] not in (0.25, 0.5, 1.0)
        ):
            raise ValueError("Unexpected runtime resource profile")
    expected = {
        "SOURCE_LIMITS": {
            "page_size": 5000,
            "max_attempts": 3,
            "connect_timeout": 5.0,
            "read_timeout": 30.0,
            "max_backoff": 10.0,
            "task_deadline": 600.0,
        },
        "SDK_LIMITS": {
            "connect_timeout": 5,
            "read_timeout": 20,
            "total_max_attempts": 3,
        },
        "DUCKDB_LIMITS": {
            "memory_limit": "512MiB",
            "threads": 1,
            "temp_directory": "",
        },
    }
    if any(constants.get(key) != value for key, value in expected.items()):
        raise ValueError("Unexpected runtime source, SDK or DuckDB limits")
    if digest(path) != fingerprint:
        raise ValueError("Runtime module changed while loading")
    return {"module_sha256": fingerprint, "constants": constants}


def resource_flags(profile=None):
    if profile is None:
        return ["--memory=2g", "--memory-swap=2g", "--cpus=2"]
    return [
        f"--memory={profile['memory_mib']}M",
        f"--memory-swap={profile['memory_mib']}M",
        f"--cpus={profile['benchmark_cpus']:g}",
    ]


def parse_args(argv=None):
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--work", type=Path, required=True, help="New directory under /tmp/opencode"
    )
    parser.add_argument(
        "--profiled", action="store_true", help="Use task 8.6 ZIP profiles"
    )
    parser.add_argument(
        "--artifact-suffix",
        choices=("optimized", "bounded", "ready"),
        default="optimized",
    )
    args = parser.parse_args(argv)
    if not args.profiled and args.artifact_suffix != "optimized":
        parser.error(f"--artifact-suffix {args.artifact_suffix} requires --profiled")
    return args


def main(argv=None):
    args = parse_args(argv)
    work = args.work.resolve()
    if work.parent != Path("/tmp/opencode") or not work.parent.is_dir():
        raise ValueError("Use a new directory directly under /tmp/opencode")
    runtime = load_profiles() if args.profiled else None
    original = ROOT / "data/local/published"
    before = snapshot(original)
    if not before:
        raise ValueError("Original published local data is required")
    image = json.loads(
        subprocess.run(
            DOCKER + ["image", "inspect", IMAGE],
            check=True,
            capture_output=True,
            text=True,
        ).stdout
    )[0]
    if (
        image["Os"] != "linux"
        or image["Architecture"] != "amd64"
        or IMAGE not in image["RepoDigests"]
    ):
        raise ValueError("The approved pinned amd64 image is not locally available")
    work.mkdir()
    inputs = work / "inputs"
    published = inputs / "published"
    published.mkdir(parents=True)
    daily, keys = [], set()
    for day in DAYS:
        key = f"control/silver/{day}.json"
        silver = json.loads((original / key).read_text())
        assert silver["status"] == "complete" and silver["mode"] == "published"
        assert silver["start_date"] == day
        keys.update([key, silver["source_manifest_key"]])
        for field in ("partition", "quality_report", "quarantine"):
            artifact = silver[f"{field}_key"]
            assert digest(original / artifact) == silver[f"{field}_sha256"]
            keys.add(artifact)
        bronze = json.loads((original / silver["source_manifest_key"]).read_text())
        assert bronze["status"] == "complete" and bronze["page_size"] == 5000
        assert (
            bronze["record_count"]
            == bronze["source_count_before"]
            == bronze["source_count_after"]
        )
        json_bytes, gzip_bytes = 0, 0
        for page in bronze["pages"]:
            compressed = (original / page["key"]).read_bytes()
            content = gzip.decompress(compressed)
            assert hashlib.sha256(content).hexdigest() == page["sha256"]
            assert len(json.loads(content)) == page["row_count"]
            json_bytes += len(content)
            gzip_bytes += len(compressed)
            keys.add(page["key"])
        daily.append(
            {
                "day": day,
                "rows": bronze["record_count"],
                "pages": len(bronze["pages"]),
                "json_bytes": json_bytes,
                "gzip_bytes": gzip_bytes,
                "silver_bytes": (original / silver["partition_key"]).stat().st_size,
            }
        )
    assert len(keys) == 234 and sum(day["rows"] for day in daily) == 323627
    for key in sorted(keys):
        assert key in before
        destination = published / key
        destination.parent.mkdir(parents=True, exist_ok=True)
        shutil.copyfile(original / key, destination)
        assert digest(destination) == before[key]
    gold_key = f"control/gold/{RUN}.json"
    gold = json.loads((original / gold_key).read_text())
    assert gold["status"] == "complete" and gold["covered_days"] == DAYS
    reference_keys = [gold_key, *[value["key"] for value in gold["products"].values()]]
    for key in reference_keys:
        destination = inputs / "reference" / key
        destination.parent.mkdir(parents=True, exist_ok=True)
        shutil.copyfile(original / key, destination)
        assert digest(destination) == before[key]
    selected = list(
        dict.fromkeys(
            [
                max(daily, key=lambda item: item["rows"])["day"],
                max(daily, key=lambda item: item["json_bytes"])["day"],
            ]
        )
    )
    artifacts = {}
    for stage in ("ingest", "curate", "gold"):
        source = Path("/tmp/opencode") / f"nyc311-lambda-{stage}-{args.artifact_suffix}"
        inventory = json.loads((source / "inventory.json").read_text())
        artifact = source / f"nyc311-{stage}.zip"
        assert digest(artifact) == inventory["zip_sha256"]
        destination = work / "payloads" / stage
        with zipfile.ZipFile(artifact) as archive:
            assert archive.namelist() == [item["path"] for item in inventory["entries"]]
            for entry in inventory["entries"]:
                name = Path(entry["path"])
                assert not name.is_absolute() and ".." not in name.parts
                assert stat.S_ISREG(archive.getinfo(entry["path"]).external_attr >> 16)
                content = archive.read(entry["path"])
                assert hashlib.sha256(content).hexdigest() == entry["sha256"]
                path = destination / name
                path.parent.mkdir(parents=True, exist_ok=True)
                path.write_bytes(content)
        if runtime:
            if (
                digest(destination / "nyc311/lambda_runtime.py")
                != runtime["module_sha256"]
            ):
                raise ValueError(
                    f"{stage} {args.artifact_suffix} ZIP runtime profile does not match "
                    "host source; historical and current policies cannot be mixed"
                )
        artifacts[stage] = {
            "zip_sha256": inventory["zip_sha256"],
            "input_hashes": inventory["inputs"],
        }
        if runtime:
            artifacts[stage]["runtime_profile"] = runtime
    manifest = {
        "original_root": str(original),
        "original_snapshot": before,
        "clone_files": len(keys),
        "clone_bytes": sum((published / key).stat().st_size for key in keys),
        "daily": daily,
        "selected_days": selected,
        "artifacts": artifacts,
        "image": {
            key: image[key] for key in ("Id", "RepoDigests", "Os", "Architecture")
        },
        "source_observation_min_utc": gold["observation_min_utc"],
        "source_observation_max_utc": gold["observation_max_utc"],
    }
    if runtime:
        manifest["runtime_profile"] = runtime
        manifest["artifact_suffix"] = args.artifact_suffix
        manifest["resource_caps"] = {
            "pids": 256,
            "tmp_bytes": 512 * 1024**2,
            "swap_bytes": 0,
            "OMP_NUM_THREADS": 1,
            "OPENBLAS_NUM_THREADS": 1,
        }
    (inputs / "manifest.json").write_text(json.dumps(manifest, indent=2) + "\n")
    trials = []
    try:
        for repetition in range(1, 4):
            cases = [(stage, day) for day in selected for stage in ("ingest", "curate")]
            cases.append(("gold", None))
            for stage, day in cases:
                task = "86" if args.profiled else "85"
                profile = runtime["constants"]["PROFILES"][stage] if runtime else None
                run_id = f"benchmark{task}-{stage}-{day or 'month'}-{repetition}"
                trial = work / "trials" / run_id
                objects = trial / "objects/nyc311/published"
                objects.mkdir(parents=True)
                seed = set()
                if stage == "curate":
                    key = f"control/bronze/{RUN}/{day}.json"
                    seed.add(key)
                    seed.update(
                        page["key"]
                        for page in json.loads((published / key).read_text())["pages"]
                    )
                elif stage == "gold":
                    for creation_day in DAYS:
                        key = f"control/silver/{creation_day}.json"
                        silver = json.loads((published / key).read_text())
                        seed.update([key, silver["source_manifest_key"]])
                        seed.update(
                            silver[field]
                            for field in (
                                "partition_key",
                                "quality_report_key",
                                "quarantine_key",
                            )
                        )
                for key in sorted(seed):
                    target = objects / key
                    target.parent.mkdir(parents=True, exist_ok=True)
                    shutil.copyfile(published / key, target)
                name = (
                    f"nyc311-b{task}-{work.name}-{stage}-{day or 'month'}-{repetition}"
                )
                command = DOCKER + [
                    "run",
                    "--pull=never",
                    "--name",
                    name,
                    "--network=none",
                    "--read-only",
                    "--cap-drop=ALL",
                    "--security-opt=no-new-privileges",
                    "--user",
                    f"{os.getuid()}:{os.getgid()}",
                    *resource_flags(profile),
                    "--pids-limit=256",
                    "--tmpfs",
                    "/tmp:rw,nosuid,nodev,size=536870912,mode=1777",
                    "--mount",
                    f"type=bind,src={work / 'payloads' / stage},dst=/var/task,readonly",
                    "--mount",
                    f"type=bind,src={inputs},dst=/inputs,readonly",
                    "--mount",
                    f"type=bind,src={trial},dst=/evidence",
                    "--mount",
                    f"type=bind,src={ROOT / 'scripts/benchmark_lambda.py'},dst=/checks/benchmark_lambda.py,readonly",
                    "--mount",
                    f"type=bind,src={ROOT / 'scripts/check_lambda_runtime.py'},dst=/checks/check_lambda_runtime.py,readonly",
                    "--entrypoint",
                    "/usr/bin/env",
                    IMAGE,
                    "-i",
                    "PATH=/var/lang/bin:/usr/bin:/bin",
                    "HOME=/tmp",
                    "LD_LIBRARY_PATH=/var/lang/lib:/lib64:/usr/lib64:/var/runtime:/var/runtime/lib:/var/task:/var/task/lib:/opt/lib",
                    "AWS_EC2_METADATA_DISABLED=true",
                    "AWS_CONFIG_FILE=/dev/null",
                    "AWS_SHARED_CREDENTIALS_FILE=/dev/null",
                    "OPENBLAS_NUM_THREADS=1",
                    "OMP_NUM_THREADS=1",
                    "/var/lang/bin/python3.12",
                    "-I",
                    "-S",
                    "-B",
                    "/checks/benchmark_lambda.py",
                    "--stage",
                    stage,
                    "--run-id",
                    run_id,
                    "--output",
                    "/evidence",
                ]
                if args.profiled:
                    command.append("--profiled")
                if day:
                    command.extend(["--day", day])
                started = time.perf_counter()
                timed_out = False
                try:
                    completed = subprocess.run(
                        command, capture_output=True, text=True, timeout=900
                    )
                    returncode, stdout, stderr = (
                        completed.returncode,
                        completed.stdout,
                        completed.stderr,
                    )
                except subprocess.TimeoutExpired as error:
                    timed_out = True
                    returncode = None
                    stdout = (
                        error.stdout.decode()
                        if isinstance(error.stdout, bytes)
                        else error.stdout or ""
                    )
                    stderr = (
                        error.stderr.decode()
                        if isinstance(error.stderr, bytes)
                        else error.stderr or ""
                    )
                    subprocess.run(
                        DOCKER + ["kill", name], capture_output=True, check=False
                    )
                state = subprocess.run(
                    DOCKER + ["inspect", "--format", "{{json .State}}", name],
                    capture_output=True,
                    text=True,
                )
                record = {
                    "run_id": run_id,
                    "stage": stage,
                    "day": day,
                    "repetition": repetition,
                    "command": command,
                    "docker_returncode": returncode,
                    "outer_timeout": timed_out,
                    "docker_wall_seconds": time.perf_counter() - started,
                    "container_state": json.loads(state.stdout)
                    if state.returncode == 0
                    else None,
                }
                if runtime:
                    record.update(
                        runtime_profile=runtime,
                        stage_profile=profile,
                        resource_caps=manifest["resource_caps"],
                    )
                (trial / "stdout.txt").write_text(stdout)
                (trial / "stderr.txt").write_text(stderr)
                (trial / "runner.json").write_text(json.dumps(record, indent=2) + "\n")
                trials.append(record)
                (work / "trials.json").write_text(json.dumps(trials, indent=2) + "\n")
                subprocess.run(DOCKER + ["rm", name], capture_output=True, check=True)
                print(
                    json.dumps(
                        {
                            "trial": run_id,
                            "exit": returncode,
                            "outer_timeout": timed_out,
                        }
                    ),
                    flush=True,
                )
                if returncode != 0:
                    raise RuntimeError(
                        f"Benchmark did not complete: {run_id}; evidence retained"
                    )
    finally:
        after = snapshot(original)
        preservation = {"original_files": len(before), "unchanged": before == after}
        (work / "originals-preserved.json").write_text(
            json.dumps(preservation, indent=2) + "\n"
        )
        assert before == after, (
            "Original successful publications changed during benchmarking"
        )
    print(
        json.dumps(
            {"work": str(work), "trials": len(trials), "originals_preserved": True}
        )
    )


if __name__ == "__main__":
    main()
