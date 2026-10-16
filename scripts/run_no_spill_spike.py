"""Offline 8.6: --production verifies ready policy; default retains the old spike."""

import argparse
import hashlib
import json
import os
import shutil
import subprocess
import time
import zipfile
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
DOCKER = ["docker", "--host", "unix:///var/run/docker.sock"]
IMAGE = "public.ecr.aws/lambda/python@sha256:471e480c997b3150ab1a4fb3b862554fa45d54a4f4a5cd21e3115b320197b717"
NATIVE_SHA256 = "cd857a08b9c56a9a253b475a3424383adf7a6d51e613e0db122c5760498e0b21"


def digest(path):
    with path.open("rb") as stream:
        return hashlib.file_digest(stream, "sha256").hexdigest()


def snapshot(root):
    result = {}
    for path in sorted(root.rglob("*")):
        if path.is_symlink():
            raise ValueError("Protected trees must not contain symlinks")
        if path.is_file():
            result[path.relative_to(root).as_posix()] = digest(path)
    return result


def parse_args(argv=None):
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--work", type=Path, required=True)
    parser.add_argument(
        "--case",
        choices=("positive", "negative", "gold", "fail-first", "fail-late"),
        required=True,
    )
    parser.add_argument("--trial", type=int, default=1)
    parser.add_argument("--production", action="store_true")
    return parser.parse_args(argv)


def main(argv=None):
    args = parse_args(argv)
    work = args.work.resolve()
    if work.parent != Path("/tmp/opencode") or args.trial < 1:
        raise ValueError(
            "Use an isolated directory directly under /tmp/opencode and a positive trial"
        )
    if shutil.which("strace") is None:
        raise ValueError(
            "An existing host strace is required; no installation is allowed"
        )
    original = ROOT / "data/local/published"
    protected = {
        str(path): digest(path)
        for path in [
            ROOT / name
            for name in (
                "src/nyc311/gold.py",
                "src/nyc311/lambda_gold.py",
                "src/nyc311/lambda_runtime.py",
                "pyproject.toml",
                "uv.lock",
                "devenv.nix",
                "flake.nix",
            )
        ]
    }
    script_hashes = {
        name: digest(ROOT / "scripts" / name)
        for name in (
            "spike_duckdb_no_spill.py",
            "run_no_spill_spike.py",
            "audit_no_spill_trace.py",
            "benchmark_lambda.py",
            "check_lambda_runtime.py",
        )
    }
    if args.production:
        protected.update(
            (str(ROOT / directory / key), value)
            for directory in ("src", "sql")
            for key, value in snapshot(ROOT / directory).items()
        )
    before = snapshot(original)
    source = Path(
        "/tmp/opencode/nyc311-benchmark-ready/inputs"
        if args.production
        else "/tmp/opencode/nyc311-benchmark86/inputs"
    )
    artifact_root = Path(
        "/tmp/opencode/nyc311-lambda-gold-ready"
        if args.production
        else "/tmp/opencode/nyc311-lambda-gold-bounded"
    )
    artifact = artifact_root / "nyc311-gold.zip"
    approved = Path("/tmp/opencode/nyc311-lambda-gold-optimized/nyc311-gold.zip")
    if args.production and (not source.is_dir() or not artifact.is_file()):
        raise ValueError(
            "Production ready ZIP/inputs missing; create ready benchmark artifacts first"
        )
    if not work.exists():
        work.mkdir()
        shutil.copytree(source, work / "inputs")
        inventory = json.loads((artifact_root / "inventory.json").read_text())
        assert digest(artifact) == inventory["zip_sha256"]
        payload = work / "payload"
        with zipfile.ZipFile(artifact) as archive:
            assert archive.namelist() == [
                entry["path"] for entry in inventory["entries"]
            ]
            for entry in inventory["entries"]:
                key = Path(entry["path"])
                assert not key.is_absolute() and ".." not in key.parts
                content = archive.read(entry["path"])
                assert hashlib.sha256(content).hexdigest() == entry["sha256"]
                target = payload / key
                target.parent.mkdir(parents=True, exist_ok=True)
                target.write_bytes(content)
        native = "_duckdb.cpython-312-x86_64-linux-gnu.so"
        with zipfile.ZipFile(approved) as archive:
            assert hashlib.sha256(archive.read(native)).hexdigest() == digest(
                payload / native
            )
        if args.production:
            assert digest(payload / native) == NATIVE_SHA256
            for path in (payload / "nyc311").glob("*.py"):
                assert digest(path) == digest(ROOT / "src/nyc311" / path.name)
            for path in (ROOT / "sql").glob("*.sql"):
                assert digest(path) == digest(payload / "sql" / path.name)
        (work / "baseline.json").write_text(
            json.dumps(
                {
                    "originals": before,
                    "protected": protected,
                    "payload": snapshot(payload),
                    "inputs": snapshot(work / "inputs"),
                    "candidate_zip_sha256": digest(artifact),
                    "approved_zip_sha256": digest(approved),
                    "native_sha256": digest(payload / native),
                    "production_policy_verification": args.production,
                    "script_sha256": script_hashes,
                    "candidate_zip": str(artifact),
                    "approved_zip": str(approved),
                },
                indent=2,
            )
            + "\n"
        )
    baseline = json.loads((work / "baseline.json").read_text())
    assert baseline.get("production_policy_verification", False) is args.production, (
        "Never reuse a historical spike workspace for production verification"
    )
    if args.production:
        assert baseline["script_sha256"] == script_hashes
        assert digest(artifact) == baseline["candidate_zip_sha256"]
        assert digest(approved) == baseline["approved_zip_sha256"]
        assert baseline["native_sha256"] == NATIVE_SHA256
    assert baseline["protected"] == protected and baseline["originals"] == before
    assert snapshot(work / "payload") == baseline["payload"]
    assert snapshot(work / "inputs") == baseline["inputs"]
    trial_id = f"nospill-{args.case}-{'production-' if args.production else ''}trial{args.trial}"
    trial = work / "trials" / trial_id
    trial.mkdir(parents=True)
    (trial / "scripts-sha256.json").write_text(
        json.dumps(script_hashes, indent=2) + "\n"
    )
    objects = trial / "objects/nyc311/published"
    objects.mkdir(parents=True)
    source = work / "inputs/published"
    keys = set()
    for day in range(1, 31):
        key = f"control/silver/2026-09-{day:02d}.json"
        manifest = json.loads((source / key).read_text())
        keys.update([key, manifest["source_manifest_key"]])
        keys.update(
            manifest[field]
            for field in ("partition_key", "quality_report_key", "quarantine_key")
        )
    assert len(keys) == 150
    for key in sorted(keys):
        target = objects / key
        target.parent.mkdir(parents=True, exist_ok=True)
        shutil.copyfile(source / key, target)
    if args.case.startswith("fail"):
        for key in baseline["inputs"]:
            if key.startswith("reference/"):
                relative = key.removeprefix("reference/")
                target = objects / relative
                target.parent.mkdir(parents=True, exist_ok=True)
                shutil.copyfile(work / "inputs" / key, target)
    name = f"nyc311-spike-{work.name}-{args.case}-{args.trial}"
    command = DOCKER + [
        "run",
        "-d",
        "--pull=never",
        "--name",
        name,
        "--network=none",
        "--read-only",
        "--cap-drop=ALL",
        "--security-opt=no-new-privileges",
        "--user",
        f"{os.getuid()}:{os.getgid()}",
        "--memory=2048M",
        "--memory-swap=2048M",
        "--cpus=1",
        "--pids-limit=256",
        "--tmpfs",
        "/tmp:rw,nosuid,nodev,size=536870912,mode=1777",
        "--mount",
        f"type=bind,src={work / 'payload'},dst=/var/task,readonly",
        "--mount",
        f"type=bind,src={work / 'inputs'},dst=/inputs,readonly",
        "--mount",
        f"type=bind,src={trial},dst=/evidence",
    ]
    for script in (
        "spike_duckdb_no_spill.py",
        "benchmark_lambda.py",
        "check_lambda_runtime.py",
    ):
        command.extend(
            [
                "--mount",
                f"type=bind,src={ROOT / 'scripts' / script},dst=/checks/{script},readonly",
            ]
        )
    command.extend(
        [
            "--entrypoint",
            "/usr/bin/env",
            IMAGE,
            "-i",
            "PATH=/var/lang/bin:/usr/bin:/bin",
            "HOME=/tmp/home",
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
            "/checks/spike_duckdb_no_spill.py",
            "--case",
            args.case,
            "--run-id",
            trial_id,
            "--observed",
        ]
    )
    if args.production:
        command.append("--production")
    tracer = None
    observer = {
        "command": command,
        "status": "failed",
        "production_policy_verification": args.production,
        "script_sha256": script_hashes,
    }
    trace_stderr = (trial / "strace-stderr.txt").open("w")
    try:
        subprocess.run(command, check=True, capture_output=True, text=True, timeout=120)
        until = time.monotonic() + 90
        while not (trial / "ready.json").exists():
            if time.monotonic() >= until:
                raise TimeoutError("Probe did not reach READY")
            if (trial / "report.json").exists():
                raise RuntimeError("Probe failed before READY; inspect evidence")
            time.sleep(0.05)
        pid = int(
            subprocess.run(
                DOCKER + ["inspect", "--format", "{{.State.Pid}}", name],
                check=True,
                capture_output=True,
                text=True,
            ).stdout
        )
        trace_command = [
            "strace",
            "-f",
            "-ttt",
            "-yy",
            "--decode-pids=pidns",
            "-s",
            "256",
            "-o",
            str(trial / "trace.log"),
            "-e",
            "trace=%file,%process,write,writev,pwrite64,pwritev,pwritev2,ftruncate,truncate,fallocate,fsync,fdatasync,close,dup,dup2,dup3,fcntl,mmap,msync,memfd_create,io_uring_setup,io_uring_enter,io_uring_register,io_setup,io_submit,io_destroy,io_getevents,sendfile,copy_file_range,splice,tee,vmsplice",
            "-e",
            "raw=write,writev,pwrite64,pwritev,pwritev2,io_submit",
            "-p",
            str(pid),
        ]
        tracer = subprocess.Popen(
            trace_command, stdout=subprocess.DEVNULL, stderr=trace_stderr
        )
        until = time.monotonic() + 10
        while True:
            if tracer.poll() is not None:
                raise RuntimeError("Host strace could not attach; no privileged retry")
            status = Path(f"/proc/{pid}/status").read_text()
            actual = int(
                next(
                    line.split()[1]
                    for line in status.splitlines()
                    if line.startswith("TracerPid:")
                )
            )
            if actual == tracer.pid:
                break
            if time.monotonic() >= until:
                raise TimeoutError("Tracer attachment not confirmed")
            time.sleep(0.05)
        observer.update(
            host_pid=pid,
            tracer_pid=tracer.pid,
            trace_command=trace_command,
            tracer_confirmed_before_go=True,
        )
        census = {}
        for task in Path(f"/proc/{pid}/task").iterdir():
            state = (task / "status").read_text()
            traced_by = int(
                next(
                    line.split()[1]
                    for line in state.splitlines()
                    if line.startswith("TracerPid:")
                )
            )
            if traced_by != tracer.pid:
                raise RuntimeError("A pre-GO thread is not attached to the observer")
            census[task.name] = traced_by
        observer["before_go_threads"] = census
        (trial / "go").touch()
        exitcode = int(
            subprocess.run(
                DOCKER + ["wait", name],
                check=True,
                capture_output=True,
                text=True,
                timeout=900,
            ).stdout
        )
        observer["container_exitcode"] = exitcode
        tracer.wait(timeout=30)
        observer["tracer_exitcode"] = tracer.returncode
        observer["container_state"] = json.loads(
            subprocess.run(
                DOCKER + ["inspect", "--format", "{{json .State}}", name],
                check=True,
                capture_output=True,
                text=True,
            ).stdout
        )
        observer["status"] = (
            "captured" if exitcode == 0 and tracer.returncode == 0 else "failed"
        )
    finally:
        subprocess.run(DOCKER + ["kill", name], capture_output=True, check=False)
        if tracer is not None and tracer.poll() is None:
            tracer.terminate()
            tracer.wait(timeout=30)
        trace_stderr.close()
        logs = subprocess.run(DOCKER + ["logs", name], capture_output=True, text=True)
        (trial / "stdout.txt").write_text(logs.stdout)
        (trial / "stderr.txt").write_text(logs.stderr)
        subprocess.run(DOCKER + ["rm", name], capture_output=True, check=False)
        observer["originals_preserved"] = snapshot(original) == before
        observer["protected_code_preserved"] = all(
            digest(Path(path)) == value for path, value in protected.items()
        )
        observer["payload_preserved"] = (
            snapshot(work / "payload") == baseline["payload"]
        )
        observer["inputs_preserved"] = snapshot(work / "inputs") == baseline["inputs"]
        observer["scripts_preserved"] = all(
            digest(ROOT / "scripts" / name) == value
            for name, value in script_hashes.items()
        )
        if args.production:
            observer["original_zips_preserved"] = (
                digest(artifact) == baseline["candidate_zip_sha256"]
                and digest(approved) == baseline["approved_zip_sha256"]
            )
            observer["native_sha256"] = baseline["native_sha256"]
        (trial / "observer.json").write_text(json.dumps(observer, indent=2) + "\n")
        print(
            json.dumps(
                {
                    "trial": trial_id,
                    "observer_status": observer["status"],
                    "evidence": str(trial),
                }
            )
        )
    if observer["status"] != "captured":
        raise RuntimeError(
            "Experimental probe or observation failed; stop and inspect retained evidence"
        )


if __name__ == "__main__":
    main()
