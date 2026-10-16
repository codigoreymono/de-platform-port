"""Retained-strace audit; --production requires unmodified production policy.

Usage: python scripts/audit_no_spill_trace.py TRIALPATH
Audits each control independently; callers must require a positive pass BEFORE
using a negative/Gold pass. No containers, native imports, or trace recapture.
"""

import argparse
import ast
import hashlib
import json
import posixpath
import re
from collections import Counter
from pathlib import Path, PurePosixPath

WRITES = {"write", "writev", "pwrite64", "pwritev", "pwritev2"}
OPEN_FLAGS = set(
    "O_RDONLY O_WRONLY O_RDWR O_CREAT O_EXCL O_NOCTTY O_TRUNC O_APPEND "
    "O_NONBLOCK O_DSYNC FASYNC O_DIRECT O_LARGEFILE O_DIRECTORY O_NOFOLLOW "
    "O_NOATIME O_CLOEXEC O_SYNC O_PATH O_TMPFILE O_NDELAY O_RSYNC".split()
)
MMAP_FLAGS = set(
    "MAP_SHARED MAP_SHARED_VALIDATE MAP_PRIVATE MAP_FIXED MAP_FIXED_NOREPLACE "
    "MAP_ANONYMOUS MAP_ANON MAP_32BIT MAP_GROWSDOWN MAP_DENYWRITE MAP_EXECUTABLE "
    "MAP_LOCKED MAP_NORESERVE MAP_POPULATE MAP_NONBLOCK MAP_STACK MAP_HUGETLB "
    "MAP_HUGE_2MB MAP_HUGE_1GB MAP_SYNC".split()
)
FD_MUTATIONS = {"ftruncate", "fsync", "fdatasync"}
TRANSFERS = {
    "sendfile": 0,
    "sendfile64": 0,
    "copy_file_range": 2,
    "splice": 2,
    "tee": 1,
    "vmsplice": 0,
}
ASYNC_IO = {
    "io_uring_setup",
    "io_uring_enter",
    "io_uring_register",
    "io_setup",
    "io_submit",
    "io_destroy",
    "io_getevents",
}
EXPANDED_IO = ASYNC_IO | (set(TRANSFERS) - {"sendfile64"}) | {"truncate", "fallocate"}
CLONE_FLAGS = set(
    "CLONE_VM CLONE_FS CLONE_FILES CLONE_SIGHAND CLONE_PIDFD CLONE_PTRACE "
    "CLONE_VFORK CLONE_PARENT CLONE_THREAD CLONE_NEWNS CLONE_SYSVSEM CLONE_SETTLS "
    "CLONE_PARENT_SETTID CLONE_CHILD_CLEARTID CLONE_DETACHED CLONE_UNTRACED "
    "CLONE_CHILD_SETTID CLONE_NEWCGROUP CLONE_NEWUTS CLONE_NEWIPC CLONE_NEWUSER "
    "CLONE_NEWPID CLONE_NEWNET CLONE_IO CLONE_CLEAR_SIGHAND CLONE_INTO_CGROUP "
    "CLONE_NEWTIME".split()
)
READ_ONLY = {
    "access",
    "faccessat",
    "faccessat2",
    "stat",
    "lstat",
    "newfstatat",
    "statx",
    "statfs",
    "statfs64",
    "readlink",
    "readlinkat",
    "getcwd",
    "fstat",
    "fstatfs",
}
PATH_OPS = {
    "unlink": (0,),
    "rmdir": (0,),
    "mkdir": (0,),
    "truncate": (0,),
    "chmod": (0,),
    "chown": (0,),
    "lchown": (0,),
    "utime": (0,),
    "utimes": (0,),
    "mknod": (0,),
    "setxattr": (0,),
    "lsetxattr": (0,),
    "removexattr": (0,),
    "lremovexattr": (0,),
    "rename": (0, 1),
    "link": (0, 1),
    "symlink": (0, 1),
    "unlinkat": (1,),
    "mkdirat": (1,),
    "mknodat": (1,),
    "fchmodat": (1,),
    "fchmodat2": (1,),
    "fchownat": (1,),
    "futimesat": (1,),
    "utimensat": (1,),
    "renameat": (1, 3),
    "renameat2": (1, 3),
    "linkat": (1, 3),
    "symlinkat": (0, 2),
}
PRODUCTS = ("demand_daily", "closure_duration", "outstanding_requests", "channel_usage")
GOLD_LIMITS = {
    "memory_limit": "512MiB",
    "max_temp_directory_size": "256MiB",
    "threads": 1,
}
PRODUCTION_LIMITS = {"memory_limit": "512MiB", "threads": 1, "temp_directory": ""}
NATIVE_SHA256 = "cd857a08b9c56a9a253b475a3424383adf7a6d51e613e0db122c5760498e0b21"
QUERY_SHA256 = hashlib.sha256(b"""
        SELECT i, repeat(md5(i::VARCHAR), 4) AS value
        FROM range(350000) t(i) ORDER BY value
    """).hexdigest()
MARKER = re.compile(
    r"/evidence/phases/(setup|imports|connection(?:-[1-4]|-close)?|query|"
    r"handler|business-[1-4]|native-product-[1-4]|validation|cleanup)\.(begin|end)"
)
LIMITATIONS = [
    "Recorded syscall scope only, not a general kernel I/O guarantee. "
    "mprotect/munmap, close_range/unshare and untraced FD/metadata operations "
    "remain outside the required filter. Successful async I/O is unsupported.",
    "One shared FD table: successful clones must be CLONE_THREAD|CLONE_FILES "
    "when observed; processes/execs are unsupported. Pre-GO thread membership "
    "is trusted from the host census, not reconstructed before attachment.",
    "Initial FD 1/2 are assumed Docker stdout/stderr pipes until closed/replaced. "
    "Paths use container -yy annotations and READY cwd; no host prefix rewriting.",
    "Trace overhead and CPU/elapsed statistics are diagnostic, not AWS performance.",
]


def number(text):
    match = re.match(r"(-?0x[0-9a-fA-F]+|-?\d+)(?=$|[<\s,])", text.strip())
    if not match:
        raise ValueError("Unresolved numeric result/descriptor")
    value = match[1]
    return int(value, 16 if "x" in value else 10)


def clone_child(returned):
    """Use only the return annotation, never parent_tid/other argument comments."""
    match = re.fullmatch(
        r"(?:[1-9]\d*|0x[0-9a-fA-F]+)"
        r"(?:\s+/\*\s+([1-9]\d*) in strace's PID NS\s+\*/)?",
        returned.strip(),
    )
    if not match:
        raise ValueError("Unresolved/conflicting clone return PID annotation")
    return int(match[1]) if match[1] else number(returned)


def setting_bytes(value):
    match = (
        re.fullmatch(r"(\d+)(?:\.0+)?\s*(B|bytes?|KiB|MiB|GiB)", value)
        if isinstance(value, str)
        else None
    )
    return (
        int(match[1])
        * {"B": 1, "byte": 1, "bytes": 1, "KiB": 1024, "MiB": 1024**2, "GiB": 1024**3}[
            match[2]
        ]
        if match
        else None
    )


def arguments(text):
    """Split only top-level commas, including openat2 structs and -yy brackets."""
    result, start, stack = [], 0, []
    quoted = escaped = False
    for index, char in enumerate(text):
        if quoted:
            if escaped:
                escaped = False
            elif char == "\\":
                escaped = True
            elif char == '"':
                quoted = False
        elif char == '"':
            quoted = True
        elif char in "([{<":
            stack.append(char)
        elif char in ")]}>" and stack:
            stack.pop()
        elif char == "," and not stack:
            result.append(text[start:index].strip())
            start = index + 1
    if quoted or stack:
        raise ValueError("Truncated/unbalanced syscall arguments")
    return result + [text[start:].strip()]


class TraceAudit:
    def __init__(self, report, cwd="/tmp/work"):
        self.report = report
        self.cwd = cwd
        self.fds = {1: ("STDIO_PIPE", 0), 2: ("STDIO_PIPE", 0)}
        self.generation = 0
        self.pending = {}
        self.active = []
        self.completed = set()
        self.strict_windows = []
        self.strict_context_valid = True
        self.events = []
        self.anomalies = []
        self.counts = Counter()
        self.seen = set()
        self.exits = {}
        self.exit_calls = {}
        self.children = {}
        self.mechanisms = []
        self.shared_fd_valid = True
        self.mappings = []
        self.line = 0
        self.targets = set()
        self.local = set()
        case, target = report.get("case"), report.get("run_id", "")
        if case not in {"positive", "negative", "gold", "fail-first", "fail-late"}:
            raise ValueError("Unknown probe case")
        if not re.fullmatch(rf"nospill-{case}-[A-Za-z0-9][A-Za-z0-9_-]*", target):
            raise ValueError("Invalid target run ID")
        if case not in {"positive", "negative"}:
            keys = {f"control/gold/{target}.json"} | {
                f"gold/run={target}/{name}/part.parquet" for name in PRODUCTS
            }
            self.targets = {f"/evidence/objects/nyc311/published/{k}" for k in keys}
            seeds = report.get("seed_sha256", {})
            for key in seeds:
                if key.startswith("/") or ".." in PurePosixPath(key).parts:
                    raise ValueError("Unsafe seed key")
            # Old Gold seeds remain read-only, even in the local workspace.
            sources = {k for k in seeds if not k.startswith(("gold/", "control/gold/"))}
            for root in report.get("workspaces", []):
                if not re.fullmatch(r"/tmp/nyc311-[A-Za-z0-9_-]+", root):
                    raise ValueError("Unsafe workspace")
                self.local.update(f"{root}/published/{key}" for key in sources | keys)

    def issue(self, reason, kind="UNRESOLVED", **details):
        self.anomalies.append(
            {"line": self.line, "kind": kind, "reason": reason} | details
        )

    def fd_path(self, token, table=None):
        table = self.fds if table is None else table
        fd = number(token)
        annotation = re.search(r"<(/.*?)>", token)
        if annotation:
            return posixpath.normpath(annotation[1].removesuffix(" (deleted)"))
        return table.get(fd, (None, 0))[0]

    def path(self, token, base=None, table=None):
        if not re.fullmatch(r'"(?:[^"\\]|\\.)*"', token):
            raise ValueError("Unresolved/truncated pathname")
        path = ast.literal_eval(token)
        if not path or "\x00" in path:
            raise ValueError("Empty/invalid pathname")
        if not path.startswith("/"):
            directory = self.cwd
            if base is not None:
                if base.startswith("AT_FDCWD"):
                    match = re.fullmatch(r"AT_FDCWD<(/.*?)>", base)
                    if not match and base != "AT_FDCWD":
                        raise ValueError("Unresolved cwd annotation")
                    directory = match[1] if match else self.cwd
                else:
                    directory = self.fd_path(base, table)
            if not directory or not directory.startswith("/"):
                raise ValueError("Unresolved relative directory")
            path = posixpath.join(directory, path)
        return posixpath.normpath(path)

    def allowed(self, path, op, phases):
        if path is None:
            return "UNRESOLVED"
        if path == "STDIO_PIPE":
            return "stdio" if op in WRITES or op in TRANSFERS else "FORBIDDEN"
        if MARKER.fullmatch(path):
            return (
                "marker"
                if op in {"open", "openat", "openat2", "creat", "unlink"}
                else "FORBIDDEN"
            )
        if any(phase.startswith("native-product-") for phase in phases):
            return "FORBIDDEN"
        if "setup" in phases and path in {
            "/evidence/writable",
            "/tmp/writable",
            "/tmp/work/.writable",
            "/dev/shm/writable",
        }:
            return "setup_sentinel"
        if path == "/evidence/report.json":
            return (
                "final_report"
                if not phases and "cleanup" in self.completed
                else "FORBIDDEN"
            )
        if self.report["case"] == "positive" and (
            path == "/tmp/positive-spill" or path.startswith("/tmp/positive-spill/")
        ):
            return "positive_spill"
        if "query" in phases or any(p.startswith("connection") for p in phases):
            return "FORBIDDEN"
        directory_op = op in {"mkdir", "mkdirat", "rmdir"}
        if path in self.targets and not directory_op:
            return "target_object"
        if path in self.local and not directory_op:
            return "workspace_data"
        candidate = PurePosixPath(path)
        for destination in self.local:
            dest = PurePosixPath(destination)
            if (
                not directory_op
                and candidate.parent == dest.parent
                and re.fullmatch(
                    rf"\.{re.escape(dest.name)}\.[A-Za-z0-9_-]+\.tmp", candidate.name
                )
            ):
                return "workspace_atomic"
        if (
            op in {"mkdir", "mkdirat", "rmdir", "unlinkat"}
            and any(
                path in (str(p) for p in PurePosixPath(dest).parents)
                for dest in self.local | self.targets
            )
            and path not in {"/", "/tmp", "/evidence", "/evidence/objects"}
        ):
            return "application_directory"
        return "FORBIDDEN"

    def event(self, op, paths, result, tid, phases, byte_count=0, force=None):
        for path in paths:
            category = force or self.allowed(path, op, phases)
            entry = {
                "line": self.line,
                "tid": tid,
                "op": op,
                "path": path,
                "phases": list(phases)
                or ["FINAL" if "cleanup" in self.completed else "OUTSIDE"],
                "category": category,
                "success": result >= 0,
                "bytes": byte_count,
            }
            self.events.append(entry)
            self.counts[f"category:{category}"] += 1
            for phase in entry["phases"]:
                self.counts[f"phase:{phase}:{category}"] += 1
            if category in {"FORBIDDEN", "UNRESOLVED"}:
                self.issue("File mutation/write not authorized", category, event=entry)

    def syscall(self, tid, text, table, phases):
        match = re.fullmatch(r"(\w+)\((.*)\)\s+=\s+(.*)", text)
        if not match:
            raise ValueError("Unparsed syscall/result")
        op, args, returned = match.groups()
        args = arguments(args)
        self.counts[f"syscall:{op}"] += 1
        if op in {"exit", "exit_group"}:
            code = number(args[0])
            self.exit_calls[tid] = code
            if code != 0 or returned != "?":
                self.issue("Nonzero/unknown thread exit syscall", tid=tid)
            return
        result = number(returned)
        if op in {
            "clone",
            "clone3",
            "fork",
            "vfork",
            "execve",
            "execveat",
            "wait4",
            "waitid",
            "waitpid",
        } | ASYNC_IO | set(TRANSFERS):
            errno = re.search(r"^-1\s+([A-Z][A-Z0-9_]+)\b", returned)
            denied = result == -1 and errno and errno[1] in {"ENOSYS", "EPERM"}
            observation = {
                "line": self.line,
                "tid": tid,
                "op": op,
                "result": result,
                "errno": errno[1] if errno else None,
                "outcome": "denied" if denied else "observed",
            }
            self.mechanisms.append(observation)
            if op in {"wait4", "waitid", "waitpid"}:
                if result > 0 or (
                    op == "waitid"
                    and result == 0
                    and not re.search(r"\bsi_pid=0\b", match[2])
                ):
                    self.shared_fd_valid = False
                    self.issue(
                        "Successful wait indicates unsupported process lifecycle",
                        tid=tid,
                    )
                return
            if op in {"execve", "execveat"}:
                self.shared_fd_valid = False
                observation["outcome"] = "unsupported"
                self.issue(
                    "Exec after READY invalidates shared-FD/native scope", tid=tid
                )
                return
            if denied:
                return
            if op in {"clone", "clone3", "fork", "vfork"}:
                flags = re.search(r"\bflags=([A-Z0-9_|]+)(?=[,}]|$)", match[2])
                if (
                    op not in {"clone", "clone3"}
                    or result <= 0
                    or not flags
                    or "..." in match[2]
                    or not {"CLONE_THREAD", "CLONE_FILES"} <= set(flags[1].split("|"))
                    or not set(flags[1].split("|")) <= CLONE_FLAGS
                ):
                    self.shared_fd_valid = False
                    observation["outcome"] = "unsupported"
                    self.issue(
                        "Unsupported process/non-shared clone or clone failure", tid=tid
                    )
                else:
                    child = clone_child(returned)
                    if child in self.children or child == tid:
                        self.issue("Duplicate/invalid cloned TID", tid=tid, child=child)
                    self.children[child] = tid
                    observation.update(
                        child=child, flags=flags[1].split("|"), outcome="shared_thread"
                    )
                return
            if op in ASYNC_IO:
                observation["outcome"] = "unsupported"
                self.issue(
                    "Async kernel I/O requires unsupported completion/buffer audit",
                    tid=tid,
                    op=op,
                )
                return
            self.event(
                op,
                [self.fd_path(args[TRANSFERS[op]], table)],
                result,
                tid,
                phases,
                max(result, 0),
            )
            return
        if op in READ_ONLY:
            if op == "getcwd" and result >= 0 and self.path(args[0]) != self.cwd:
                raise ValueError("Cwd changed after READY")
            return
        if op in {"open", "openat", "openat2", "creat"}:
            at = op in {"openat", "openat2"}
            path = self.path(args[1 if at else 0], args[0] if at else None, table)
            flags = (
                args[2 if at else 1] if op != "creat" else "O_WRONLY|O_CREAT|O_TRUNC"
            )
            if op == "openat2":
                flag_field = re.search(r"\bflags=([^,}]+)", flags)
                if not flag_field or "..." in flags:
                    raise ValueError("Unresolved openat2 flags")
                flags = flag_field[1].strip()
            if not set(flags.split("|")) <= OPEN_FLAGS or not re.search(
                r"\bO_(RDONLY|RDWR|WRONLY|PATH)\b", flags
            ):
                raise ValueError("Unresolved open flags")
            writable = op == "creat" or bool(
                re.search(r"O_(RDWR|WRONLY|CREAT|TRUNC|TMPFILE)\b", flags)
            )
            if writable:
                self.event(
                    op,
                    [path],
                    result,
                    tid,
                    phases,
                    force="FORBIDDEN" if "O_TMPFILE" in flags else None,
                )
            if result >= 0:
                decoded = self.fd_path(returned, {})
                if writable and decoded and decoded != path:
                    self.event(op, [decoded], result, tid, phases)
                self.generation += 1
                self.fds[result] = (decoded or path, self.generation)
            marker = MARKER.fullmatch(path)
            if marker and writable and result >= 0:
                label, boundary = marker.groups()
                if boundary == "begin":
                    if label in self.active or label in self.completed:
                        if label.startswith("native-product-"):
                            self.strict_context_valid = False
                        raise ValueError("Duplicate phase begin")
                    if label.startswith("native-product-"):
                        if "handler" not in self.active or any(
                            p.startswith(
                                ("native-product-", "connection-", "business-")
                            )
                            for p in self.active
                        ):
                            self.strict_context_valid = False
                            self.issue(
                                "Strict native window outside handler or overlaps another product"
                            )
                        self.strict_windows.append(label)
                        # A pending syscall overlaps this window even if it resumes later.
                        for pending_tid, (
                            prefix,
                            pending_table,
                            pending_phases,
                        ) in self.pending.items():
                            self.pending[pending_tid] = (
                                prefix,
                                pending_table,
                                pending_phases + (label,),
                            )
                    elif (
                        "strict_compute_windows" in self.report
                        and re.fullmatch(r"(?:connection|business)-[1-4]", label)
                        and f"native-product-{label[-1]}" not in self.active
                    ):
                        self.strict_context_valid = False
                        self.issue(
                            "Connection/business begin outside matching strict native window"
                        )
                    self.active.append(label)
                else:
                    # business-N extends beyond native return through Python staging.
                    crossing_business = (
                        label.startswith("native-product-")
                        and label in self.active
                        and (
                            self.active[self.active.index(label) + 1 :]
                            == [f"business-{label[-1]}"]
                        )
                    )
                    if not self.active or (
                        self.active[-1] != label and not crossing_business
                    ):
                        if label.startswith("native-product-"):
                            self.strict_context_valid = False
                        raise ValueError("Unmatched/non-nested phase end")
                    self.active.remove(label)
                    self.completed.add(label)
            return
        if op == "close":
            fd = number(args[0])
            # A resumed close must not erase a newer reuse of that descriptor.
            if result >= 0 and self.fds.get(fd) == table.get(fd):
                self.fds.pop(fd, None)
            return
        if op in {"dup", "dup2", "dup3", "fcntl"}:
            if op == "fcntl" and args[1] not in {"F_DUPFD", "F_DUPFD_CLOEXEC"}:
                if args[1] not in {"F_GETFD", "F_SETFD", "F_GETFL", "F_SETFL"}:
                    raise ValueError("Unrecognized fcntl operation")
                return
            if result >= 0:
                self.generation += 1
                self.fds[result] = (self.fd_path(args[0], table), self.generation)
            return
        if op in WRITES | FD_MUTATIONS | {"fallocate"}:
            path = self.fd_path(args[0], table)
            self.event(
                op, [path], result, tid, phases, max(result, 0) if op in WRITES else 0
            )
            if op in WRITES and any('"' in arg for arg in args[1:]):
                self.issue("Write buffers were decoded; raw capture required")
            return
        if op == "mmap":
            private = "MAP_PRIVATE" in args[3]
            shared = "MAP_SHARED" in args[3]
            anonymous = bool({"MAP_ANONYMOUS", "MAP_ANON"} & set(args[3].split("|")))
            if (not private and not shared) or not set(
                args[3].split("|")
            ) <= MMAP_FLAGS:
                raise ValueError("Unknown mmap flags")
            if not set(args[2].split("|")) <= {
                "PROT_READ",
                "PROT_WRITE",
                "PROT_EXEC",
                "PROT_NONE",
                "PROT_GROWSDOWN",
                "PROT_GROWSUP",
            }:
                raise ValueError("Unknown mmap protections")
            path = None if anonymous else self.fd_path(args[4], table)
            if shared and "PROT_WRITE" in args[2] and not anonymous:
                self.event(op, [path], result, tid, phases)
            if result >= 0:
                self.mappings.append(
                    (result, number(args[1]), path, shared and not anonymous)
                )
            return
        if op == "msync":
            start, size = number(args[0]), number(args[1])
            for address, length, path, shared in reversed(self.mappings):
                if address <= start and start + size <= address + length:
                    if shared:
                        self.event(op, [path], result, tid, phases)
                    return
            raise ValueError("Unresolved msync mapping")
        if op in PATH_OPS:
            paths = []
            for index in PATH_OPS[op]:
                base = args[index - 1] if "at" in op and index > 0 else None
                paths.append(self.path(args[index], base, table))
            self.event(op, paths, result, tid, phases)
            # Never authorize creation of aliases, even between allowed paths.
            if op in {"link", "linkat", "symlink", "symlinkat"}:
                self.issue("Alias creation invalidates path allowlist", "FORBIDDEN")
            return
        if op == "memfd_create":
            self.event(op, [None], result, tid, phases, force="FORBIDDEN")
            if result >= 0:
                self.fds[result] = (None, -1)
            return
        raise ValueError(f"Unrecognized syscall: {op}")

    def consume(self, lines):
        for self.line, line in enumerate(lines, 1):
            match = re.fullmatch(r"\s*(\d+)\s+(\d+\.\d+)\s+(.*?)\s*", line)
            if not match:
                self.issue("Malformed/unidentified trace line")
                continue
            tid, _, text = match.groups()
            tid = int(tid)
            self.seen.add(tid)
            if tid in self.exits:
                self.issue("Trace event after TID exit", tid=tid)
            if text.startswith("+++ exited with "):
                exit_match = re.fullmatch(r"\+\+\+ exited with (\d+) \+\+\+", text)
                if exit_match:
                    self.exits[tid] = int(exit_match[1])
                    pending = self.pending.get(tid)
                    if pending and re.match(r"exit(?:_group)?\(", pending[0]):
                        self.exit_calls[tid] = number(
                            pending[0].split("(", 1)[1].split(")", 1)[0]
                        )
                        self.pending.pop(tid)
                    if (
                        tid in self.exit_calls
                        and self.exit_calls[tid] != self.exits[tid]
                    ):
                        self.issue("Exit syscall/record status mismatch", tid=tid)
                else:
                    self.issue("Malformed exit line")
                continue
            if text.startswith("--- SIG"):
                self.issue("Signal event requires inspection")
                continue
            table, phases = self.fds.copy(), tuple(self.active)
            if text.endswith("<unfinished ...>"):
                if tid in self.pending:
                    self.issue("Overlapping pending syscall", tid=tid)
                self.pending[tid] = (
                    text.removesuffix("<unfinished ...>").rstrip(),
                    table,
                    phases,
                )
                continue
            if text.startswith("<... "):
                resumed = re.fullmatch(r"<\.\.\. (\w+) resumed>(.*)", text)
                pending = self.pending.pop(tid, None)
                if (
                    not resumed
                    or not pending
                    or not pending[0].startswith(resumed[1] + "(")
                ):
                    self.issue("Unmatched resumed syscall", tid=tid)
                    continue
                prefix, table, phases = pending
                text = prefix + resumed[2]
            try:
                self.syscall(tid, text, table, phases)
            except (ValueError, IndexError, SyntaxError) as error:
                self.issue(str(error), tid=tid)
        if self.pending:
            self.issue("Unfinished syscalls at EOF", tids=sorted(self.pending))
        if self.active:
            self.issue("Unclosed phases", phases=self.active)
        return self

    def verdict(self):
        if not self.shared_fd_valid:
            return "inconclusive"
        kinds = {a["kind"] for a in self.anomalies}
        return "fail" if "FORBIDDEN" in kinds else "inconclusive" if kinds else "pass"


def audit_trial(trial, production=False):
    trial = Path(trial)
    output = {
        "trial": str(trial),
        "experimental_offline_86_only": True,
        "capture_scope": "unresolved",
        "strict_compute_coverage": False,
        "limitations": list(LIMITATIONS),
        "positive_control_required_separately": True,
        "production_policy_verification": production,
    }
    audit = None
    try:
        report, observer, ready = (
            json.loads((trial / name).read_text())
            for name in ("report.json", "observer.json", "ready.json")
        )
        if not all(isinstance(item, dict) for item in (report, observer, ready)):
            raise ValueError("Evidence JSON roots must be objects")
        audit = TraceAudit(report, ready["cwd"])

        def require(condition, reason):
            if not condition:
                audit.issue(reason)

        output.update(case=report["case"], run_id=report["run_id"])
        require(
            all(
                item.get("production_policy_verification", False) is production
                for item in (report, observer, ready)
            ),
            "Production flag must match report/observer/READY; legacy rewrites are not production evidence",
        )
        if production:
            work = trial.parent.parent
            baseline = json.loads((work / "baseline.json").read_text())
            sidecar = json.loads((trial / "scripts-sha256.json").read_text())
            require(
                baseline.get("production_policy_verification") is True
                and observer.get("scripts_preserved") is True
                and observer.get("original_zips_preserved") is True
                and sidecar
                == baseline.get("script_sha256")
                == observer.get("script_sha256")
                and set(sidecar)
                == {
                    "spike_duckdb_no_spill.py",
                    "run_no_spill_spike.py",
                    "audit_no_spill_trace.py",
                    "benchmark_lambda.py",
                    "check_lambda_runtime.py",
                }
                and all(
                    isinstance(value, str) and re.fullmatch(r"[0-9a-f]{64}", value)
                    for value in sidecar.values()
                )
                and baseline.get("native_sha256")
                == observer.get("native_sha256")
                == report.get("native", {}).get("actual_binary_sha256")
                == NATIVE_SHA256,
                "Production isolated baseline/native/script SHA-256 evidence missing",
            )
            provenance = report.get("policy_provenance", {})
            require(
                provenance.get("declared_config") == PRODUCTION_LIMITS
                and provenance.get("config_rewritten_at_native_connect") is False,
                "Production policy provenance must not rewrite native config",
            )
        require(
            ready.get("native_imports_started") is False
            and ready["cwd"] == "/tmp/work"
            and ready.get("case") == report["case"]
            and ready.get("run_id") == report["run_id"],
            "READY must precede native imports and match trial",
        )
        require(
            observer.get("tracer_confirmed_before_go") is True,
            "Main tracer attachment before GO not confirmed",
        )
        command = observer.get("trace_command", [])
        required = (
            WRITES
            | FD_MUTATIONS
            | {
                "%file",
                "close",
                "dup",
                "dup2",
                "dup3",
                "fcntl",
                "mmap",
                "msync",
                "memfd_create",
            }
        )
        filters = [
            command[i + 1] for i, token in enumerate(command[:-1]) if token == "-e"
        ]
        traced = set().union(
            *(
                set(f.removeprefix("trace=").split(","))
                for f in filters
                if f.startswith("trace=")
            )
        )
        raw = set().union(
            *(
                set(f.removeprefix("raw=").split(","))
                for f in filters
                if f.startswith("raw=")
            )
        )
        census = observer.get("before_go_threads")
        census_valid = (
            isinstance(census, dict)
            and bool(census)
            and type(observer.get("tracer_pid")) is int
            and observer["tracer_pid"] > 0
            and all(
                isinstance(tid, str)
                and re.fullmatch(r"[1-9]\d*", tid)
                and type(pid) is int
                and pid == observer["tracer_pid"]
                for tid, pid in census.items()
            )
            and str(observer.get("host_pid")) in census
        )
        initial_tids = {int(tid) for tid in census} if census_valid else set()
        if "before_go_threads" in observer:
            require(
                census_valid,
                "Invalid pre-GO census: expected host TID -> observer tracer_pid",
            )
        process_capture = "%process" in traced
        effective_traced = traced | ({"truncate"} if "%file" in traced else set())
        kernel_capture = EXPANDED_IO <= effective_traced
        require(
            not process_capture or census_valid,
            "Process lifecycle capture requires a confirmed pre-GO thread census",
        )
        output["coverage"] = {
            "process_lifecycle": process_capture,
            "expanded_kernel_io": kernel_capture,
            "before_go_thread_census": bool(census_valid),
        }
        if production:
            require(
                process_capture
                and kernel_capture
                and census_valid
                and "--decode-pids=pidns" in command,
                "Production requires full process/kernel I/O capture, census and PID namespace decoding",
            )
        output["capture_scope"] = (
            "expanded_process_kernel_io"
            if process_capture and kernel_capture and census_valid
            else "partial_expanded"
            if process_capture or kernel_capture or census is not None
            else "legacy_limited"
        )
        missing = sorted(EXPANDED_IO - effective_traced)
        if missing:
            output["limitations"].append(
                "Not captured by this filter: " + ", ".join(missing)
            )
        if not process_capture:
            output["limitations"].append(
                "Legacy/partial scope: clone/fork/exec lifecycle not fully captured; "
                "shared FD table relies on this probe's no-subprocess assumption."
            )
        if not census_valid:
            output["limitations"].append(
                "No valid pre-GO thread census; only main attachment is confirmed."
            )
        require(
            required <= traced
            and WRITES <= raw
            and all(flag in command for flag in ("-f", "-ttt", "-yy", "-p"))
            and command[command.index("-p") + 1] == str(observer.get("host_pid")),
            "Trace command lacks required file/FD/raw capture or target",
        )
        require(
            observer.get("status") == "captured"
            and observer.get("container_exitcode")
            == observer.get("tracer_exitcode")
            == 0,
            "Observer/container/tracer did not complete successfully",
        )
        state = observer.get("container_state", {})
        require(
            state.get("ExitCode") == 0
            and state.get("OOMKilled") is False
            and state.get("Running") is False
            and not state.get("Error"),
            "Container state is not a clean native completion",
        )
        require(
            all(
                observer.get(k) is True
                for k in (
                    "originals_preserved",
                    "protected_code_preserved",
                    "payload_preserved",
                    "inputs_preserved",
                )
            ),
            "Host protected inputs changed or were not checked",
        )
        require(
            report.get("status") == "passed"
            and report.get("protected_inputs_and_payload_unchanged") is True
            and report.get("seeds_and_old_gold_unchanged") is True,
            "Functional/protected-input report failed (not itself a no-spill verdict)",
        )
        stderr = (trial / "strace-stderr.txt").read_text().splitlines()
        attached = set()
        for line in stderr:
            match = re.fullmatch(r"strace: Process (\d+) attached", line)
            if match:
                attached.add(int(match[1]))
            elif line.strip():
                audit.issue(
                    "Tracer stderr contains error/drop/detach or unknown diagnostic"
                )
        require(observer.get("host_pid") in attached, "Missing main attachment message")
        require(
            initial_tids <= attached,
            "Pre-GO census TID missing from tracer attachment messages",
        )
        require(
            not (trial / "stderr.txt").read_text().strip(),
            "Native stderr requires inspection",
        )
        with (trial / "trace.log").open() as stream:
            audit.consume(stream)
        require(
            attached == audit.seen == set(audit.exits)
            and all(code == 0 for code in audit.exits.values()),
            "Every attached/observed TID must have a zero-status exit line",
        )
        if process_capture:
            known = set(initial_tids)
            for _ in audit.children:
                known.update(
                    child for child, parent in audit.children.items() if parent in known
                )
            require(
                known == audit.seen and not initial_tids.intersection(audit.children),
                "Observed TIDs not explained by census and verified shared-thread clones",
            )
        synthetic = report["case"] in {"positive", "negative"}
        if production and synthetic:
            positive = report["case"] == "positive"
            config = PRODUCTION_LIMITS | {"memory_limit": "32MiB"}
            if positive:
                config |= {
                    "temp_directory": "/tmp/positive-spill",
                    "max_temp_directory_size": "256MiB",
                }
            effective = report.get("effective", {})
            require(
                report.get("config") == config
                and type(report.get("config", {}).get("threads")) is int
                and set(effective) == set(config)
                and effective.get("temp_directory") == config["temp_directory"]
                and type(effective.get("threads")) is int
                and effective["threads"] == 1
                and setting_bytes(effective.get("memory_limit")) == 32 * 1024**2
                and (
                    not positive
                    or setting_bytes(effective.get("max_temp_directory_size"))
                    == 256 * 1024**2
                )
                and report.get("query_sha256") == QUERY_SHA256
                and report["policy_provenance"].get("control_overrides")
                == {
                    key: value
                    for key, value in config.items()
                    if PRODUCTION_LIMITS.get(key) != value
                },
                "Production synthetic control policy/effective settings/query hash mismatch",
            )
        expected_phases = {"setup", "imports", "cleanup"}
        if synthetic:
            expected_phases |= {"connection", "query", "connection-close"}
            require(
                "strict_compute_windows" not in report,
                "Synthetic controls must use their existing query phase, not Gold windows",
            )
        else:
            count = {"gold": 4, "fail-first": 1, "fail-late": 2}[report["case"]]
            expected_phases |= {"handler", "validation"} | {
                f"connection-{i}" for i in range(1, count + 1)
            }
            expected_phases |= {
                f"business-{i}"
                for i in range(1, count + 1)
                if production or not report["case"].startswith("fail") or i < count
            }
            if "strict_compute_windows" in report:
                expected_windows = [
                    {"phase": f"native-product-{i}", "product": product}
                    for i, product in enumerate(PRODUCTS[:count], 1)
                ]
                labels = [window["phase"] for window in expected_windows]
                windows_valid = (
                    report["strict_compute_windows"] == expected_windows
                    and audit.strict_windows == labels
                    and {p for p in audit.completed if p.startswith("native-product-")}
                    == set(labels)
                    and {f"connection-{i}" for i in range(1, count + 1)}
                    <= audit.completed
                    and audit.strict_context_valid
                )
                require(
                    windows_valid,
                    "Strict compute windows/product order or trace boundaries not corroborated",
                )
                output["strict_compute_coverage"] = windows_valid
                expected_phases.update(labels)
            else:
                output["limitations"].append(
                    "strict_compute_coverage=false: legacy Gold/business windows include Python "
                    "staging writes; writes to allowlisted application paths cannot be isolated "
                    "from native compute. No strict native-product windows were declared."
                )
            if production:
                require(
                    output["strict_compute_coverage"],
                    "Production requires strict native-product coverage",
                )
        require(
            audit.completed == expected_phases,
            "Missing/unexpected completed native phases",
        )
        spill = [e for e in audit.events if e["category"] == "positive_spill"]
        opens = sum(e["op"] in {"open", "openat", "openat2", "creat"} for e in spill)
        writes = sum(e["bytes"] for e in spill if e["op"] in WRITES)
        if report["case"] == "positive":
            require(
                report.get("outcome") == "completed_with_physical_spill"
                and report.get("rows") == 350000
                and report.get("physical_spill_peak_bytes_lower_bound", 0) > 0
                and opens > 0
                and writes > 0
                and any(e["bytes"] > 0 and "query" in e["phases"] for e in spill),
                "Positive lacks mapped actual spill writes, physical blocks or 350000 rows",
            )
        elif synthetic or report["case"].startswith("fail"):
            error = report.get("expected_error", {})
            message = error.get("message", "").lower()
            require(
                error.get("type") == "OutOfMemoryException"
                and not any(
                    word in message
                    for word in (
                        "permission",
                        "read-only",
                        "read only",
                        "readonly",
                        "errno 13",
                        "errno 30",
                        "errno13",
                        "errno30",
                    )
                ),
                "Expected native OOM missing or permission-related",
            )
            if synthetic:
                require(
                    report.get("outcome") == "expected_native_resource_rejection"
                    and report.get("physical_spill_peak_bytes_lower_bound") == 0
                    and report.get("effective", {}).get("temp_directory") == "",
                    "Negative report lacks native no-temp resource rejection",
                )
        if not synthetic:
            connections = report.get("connections", [])
            require(bool(report.get("workspaces")), "Gold workspace inventory missing")
            failure_closed = False
            if report["case"] == "gold":
                require(
                    report.get("outcome") == "complete_four_products_323627_rows"
                    and report.get("validation", {}).get("original_reference_compared")
                    is True,
                    "Gold functional/reference validation missing",
                )
            else:
                target = f"nyc311/published/control/gold/{report['run_id']}.json"
                controls = [
                    p for p in report.get("put_attempts", []) if p.get("key") == target
                ]
                progress = report.get("progress", [])
                failure_closed = bool(
                    report.get("outcome") == "expected_native_failure_closed"
                    and report.get("failed_target_load_rejected") is True
                    and controls
                    and controls[-1].get("persisted") is True
                    and controls[-1].get("status") == "failed"
                    and controls[-1].get("products") == count - 1
                    and progress
                    and progress[-1].get("status") == "failed"
                    and progress[-1].get("products") == count - 1
                    and all(
                        p.get("status") != "complete"
                        for p in report.get("put_attempts", []) + progress
                    )
                )
                require(
                    failure_closed,
                    "Failure trial lacks failed target/no-complete authorization evidence",
                )
            require(
                len(connections) == count,
                "Gold native connection count not corroborated",
            )
            if production:
                source = report.get("production_source", {})
                source_path = work / "payload/nyc311/gold.py"
                source_bytes = source_path.read_bytes()
                source_hash = hashlib.sha256(source_bytes).hexdigest()
                tree = ast.parse(source_bytes)
                execute_lines = [
                    node.lineno
                    for function in tree.body
                    if isinstance(function, ast.FunctionDef)
                    and function.name == "_compute_query"
                    for node in ast.walk(function)
                    if isinstance(node, ast.Call)
                    and isinstance(node.func, ast.Attribute)
                    and isinstance(node.func.value, ast.Name)
                    and node.func.value.id == "connection"
                    and node.func.attr == "execute"
                    and len(node.args) == 1
                    and isinstance(node.args[0], ast.Name)
                    and node.args[0].id == "query"
                ]
                require(
                    source.get("file") == "/var/task/nyc311/gold.py"
                    and source.get("sha256")
                    == source_hash
                    == baseline.get("payload", {}).get("nyc311/gold.py")
                    and source.get("business_execute_lines") == execute_lines
                    and len(execute_lines) == 1,
                    "Original Gold business execute(query) source/hash not corroborated",
                )
                queries = report.get("business_query_sha256", {})
                require(
                    set(queries) == set(PRODUCTS[:count]),
                    "Original business SQL inventory mismatch",
                )
                for product in PRODUCTS[:count]:
                    key = f"sql/{product}.sql"
                    value = hashlib.sha256(
                        (work / "payload" / key).read_bytes()
                    ).hexdigest()
                    require(
                        queries.get(product)
                        == value
                        == baseline.get("payload", {}).get(key),
                        "Original business SQL SHA-256 mismatch",
                    )
                if report["case"].startswith("fail"):
                    frames = report.get("expected_error", {}).get("traceback", [])
                    require(
                        report.get("native_oom_boundary") == "business_execute_query"
                        and bool(frames)
                        and frames[-1].get("file") == source.get("file")
                        and frames[-1].get("function") == "_compute_query"
                        and frames[-1].get("line") in execute_lines
                        and report.get("fault_injection")
                        == {
                            "product_index": count,
                            "product": PRODUCTS[count - 1],
                            "boundary": "before original gold_store._compute_query validation",
                            "original_config": PRODUCTION_LIMITS,
                            "declared_config": PRODUCTION_LIMITS
                            | {"memory_limit": "1B"},
                        },
                        "Production OOM must be from original business execute(query), not settings/register/connect",
                    )
                    products = [
                        p
                        for p in report.get("put_attempts", [])
                        if p.get("key", "").startswith(
                            f"nyc311/published/gold/run={report['run_id']}/"
                        )
                        and p.get("persisted") is True
                    ]
                    require(
                        len(products) == count - 1,
                        "Production failure prior prepared product count mismatch",
                    )
                    require(
                        sum(
                            key.startswith(("gold/", "control/gold/"))
                            for key in report.get("seed_sha256", {})
                        )
                        == 5,
                        "Production failure requires all five old Gold objects",
                    )
            for index, connection in enumerate(connections, 1):
                injected = report["case"].startswith("fail") and index == count
                requested = connection.get("requested_config", {})
                native = connection.get("native_config", {})
                if production:
                    expected = PRODUCTION_LIMITS | (
                        {"memory_limit": "1B"} if injected else {}
                    )
                    require(
                        connection.get("injected") is injected
                        and requested == native == expected
                        and type(requested.get("threads")) is int
                        and type(native.get("threads")) is int
                        and connection.get("config_rewritten") is False
                        and connection.get("native_kwargs_unchanged") is True
                        and "error" not in connection,
                        f"Production connection {index} must forward exact native args without rewriting",
                    )
                    if injected:
                        require(
                            "effective" not in connection
                            and connection.get("effective_unavailable")
                            == "fault connection: readback deliberately skipped; declared config only",
                            "Production fault live settings must not be fabricated or queried",
                        )
                        output.setdefault(
                            "live_settings_skipped_fault_connections", []
                        ).append(index)
                    else:
                        effective = connection.get("effective", {})
                        require(
                            set(effective) == set(PRODUCTION_LIMITS)
                            and effective.get("temp_directory") == ""
                            and type(effective.get("threads")) is int
                            and effective["threads"] == 1
                            and setting_bytes(effective.get("memory_limit"))
                            == 512 * 1024**2
                            and "effective_unavailable" not in connection,
                            f"Production connection {index} actual three-field readback mismatch",
                        )
                    continue
                config_valid = (
                    connection.get("injected") is injected
                    and requested
                    == GOLD_LIMITS | {"temp_directory": requested.get("temp_directory")}
                    and requested.get("temp_directory")
                    in {root + "/duckdb-spill" for root in report.get("workspaces", [])}
                    and native
                    == GOLD_LIMITS
                    | {
                        "temp_directory": "",
                        "memory_limit": "1B" if injected else "512MiB",
                    }
                    and type(requested.get("threads")) is int
                    and type(native.get("threads")) is int
                    and (injected or "error" not in connection)
                )
                require(
                    config_valid,
                    f"Gold connection {index} requested/native settings not corroborated",
                )
                if "effective" in connection:
                    effective = connection["effective"]
                    require(
                        isinstance(effective, dict)
                        and set(effective) == set(native)
                        and effective.get("temp_directory") == ""
                        and type(effective.get("threads")) is int
                        and effective["threads"] == 1
                        and setting_bytes(effective.get("max_temp_directory_size"))
                        == 256 * 1024**2
                        and setting_bytes(effective.get("memory_limit"))
                        in ({0, 1} if injected else {512 * 1024**2}),
                        f"Gold connection {index} live settings not corroborated",
                    )
                else:
                    errors = [
                        connection.get("error", {}),
                        report.get("expected_error", {}),
                    ]
                    unavailable_oom = (
                        injected
                        and len(connections) == count
                        and config_valid
                        and failure_closed
                        and all(
                            error.get("type") == "OutOfMemoryException"
                            and not re.search(
                                r"permission|read.?only|errno\s*(?:13|30)",
                                error.get("message", ""),
                                re.I,
                            )
                            and error.get("traceback")
                            and error["traceback"][-1].get("file")
                            == "/checks/spike_duckdb_no_spill.py"
                            and error["traceback"][-1].get("function")
                            == "live_settings"
                            for error in errors
                        )
                    )
                    require(
                        unavailable_oom,
                        f"Gold connection {index} missing live settings without corroborated injected OOM",
                    )
                    if unavailable_oom:
                        output.setdefault(
                            "live_settings_unavailable_expected_native_oom", []
                        ).append(index)
                        output["limitations"].append(
                            f"live_settings_unavailable_expected_native_oom: connection {index}; "
                            "exact injected config and native OOM corroborated, but live values "
                            "were not read. Failure occurred in live_settings before product SQL."
                        )
        # Independently rehash all seeds, including every old Gold object.
        for key, expected in report.get("seed_sha256", {}).items():
            path = PurePosixPath(key)
            require(
                not path.is_absolute() and ".." not in path.parts,
                "Unsafe seed hash path",
            )
            if path.is_absolute() or ".." in path.parts:
                continue
            with (trial / "objects/nyc311/published" / key).open("rb") as stream:
                require(
                    hashlib.file_digest(stream, "sha256").hexdigest() == expected,
                    f"Retained seed changed: {key}",
                )
        output.update(
            positive_spill_writable_open_attempts=opens,
            positive_spill_write_bytes=writes,
            unexpected_mutation_events=sum(
                e["category"] == "FORBIDDEN" for e in audit.events
            ),
            unresolved_events=sum(a["kind"] == "UNRESOLVED" for a in audit.anomalies),
            physical_spill_peak_bytes_lower_bound=report.get(
                "physical_spill_peak_bytes_lower_bound"
            ),
            trace_lines=audit.line,
            attached_tids=sorted(attached),
            before_go_tids=sorted(initial_tids),
            thread_parents=audit.children,
            exit_calls=audit.exit_calls,
            exits=audit.exits,
            completed_phases=sorted(audit.completed),
            strict_compute_phases=audit.strict_windows,
            counts=dict(sorted(audit.counts.items())),
        )
    except (
        OSError,
        ValueError,
        KeyError,
        IndexError,
        TypeError,
        AttributeError,
    ) as error:
        if audit is not None:
            audit.issue(f"Evidence could not be fully audited: {error}")
        else:
            output["anomalies"] = [{"kind": "UNRESOLVED", "reason": str(error)}]
    if audit is not None:
        output.update(
            verdict=audit.verdict(),
            anomalies=audit.anomalies,
            events=audit.events,
            mechanism_events=audit.mechanisms,
        )
    else:
        output.update(verdict="inconclusive", events=[])
    return output


def main(argv=None):
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("trial", type=Path)
    parser.add_argument("--production", action="store_true")
    args = parser.parse_args(argv)
    result = audit_trial(args.trial, production=args.production)
    try:
        (args.trial / "trace-audit.json").write_text(
            json.dumps(result, indent=2) + "\n"
        )
    except OSError as error:
        result["verdict"] = "inconclusive"
        result["anomalies"].append(
            {"kind": "UNRESOLVED", "reason": f"Cannot write audit: {error}"}
        )
    print(
        json.dumps(
            {
                k: v
                for k, v in result.items()
                if k not in {"events", "mechanism_events"}
            },
            separators=(",", ":"),
        )
    )
    return 0 if result["verdict"] == "pass" else 1


if __name__ == "__main__":
    raise SystemExit(main())
