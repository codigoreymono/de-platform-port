"""TEST ONLY: inspect extracted ZIP ELF dependencies inside the pinned Lambda image."""

import argparse
import hashlib
import json
import os
import platform
import subprocess
import sys
from pathlib import Path


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--stage", choices=("ingest", "curate", "gold"), required=True)
    args = parser.parse_args()
    report = {
        "stage": args.stage,
        "status": "passed",
        "libc": platform.libc_ver(),
        "python": sys.version,
        "ld_library_path": os.environ.get("LD_LIBRARY_PATH"),
        "elf_files": [],
        "static_archives": [],
    }
    for path in sorted(Path("/var/task").rglob("*")):
        if not path.is_file():
            continue
        with path.open("rb") as stream:
            magic = stream.read(8)
            if magic.startswith(b"!<arch>\n"):
                report["static_archives"].append(str(path.relative_to("/var/task")))
            if not magic.startswith(b"\x7fELF"):
                continue
            stream.seek(0)
            checksum = hashlib.file_digest(stream, "sha256").hexdigest()
        result = subprocess.run(
            ["/usr/bin/ldd", str(path)], capture_output=True, text=True, timeout=30
        )
        output = result.stdout + result.stderr
        passed = (
            result.returncode == 0
            and "not found" not in output
            and "/nix/" not in output
        )
        report["elf_files"].append(
            {
                "path": str(path.relative_to("/var/task")),
                "sha256": checksum,
                "passed": passed,
                "returncode": result.returncode,
                "ldd": output,
            }
        )
        if not passed:
            report["status"] = "failed"
            break
    Path(f"/evidence/{args.stage}-linkage.json").write_text(
        json.dumps(report, indent=2) + "\n"
    )
    print(
        json.dumps(
            {
                "stage": args.stage,
                "status": report["status"],
                "elf_checked": len(report["elf_files"]),
                "static_archives_not_dynamically_linked": len(
                    report["static_archives"]
                ),
                "failures": [
                    item for item in report["elf_files"] if not item["passed"]
                ],
            },
            indent=2,
        )
    )
    return 0 if report["status"] == "passed" else 1


if __name__ == "__main__":
    raise SystemExit(main())
