# Data Lab

A reproducible Linux development environment for data projects using Nix, devenv, Python, uv, PostgreSQL, and DuckDB.

Data Lab provides a small and predictable foundation for Data Engineering, Data Analysis, and related data workflows without forcing a large framework or unnecessary tooling.

## Current Stack

- Nix Flakes
- devenv
- Python 3.12
- uv
- Ruff
- pytest
- PostgreSQL 17
- DuckDB
- JupyterLab (optional)

## Platform

Data Lab is designed for Linux systems using the Nix package manager.

Currently validated on:

- `x86_64-linux`

This includes distributions such as:

- NixOS
- Ubuntu + Nix
- Fedora + Nix
- Arch Linux + Nix
- Debian + Nix

NixOS is not required.

The full Data Lab environment requires Nix. The Python project itself can still be used without Nix if Python 3.12 and uv are installed separately.

## Project Structure

```text
.
├── data/
│   └── samples/
├── notebooks/
│   └── 00_quickstart.ipynb
├── sql/
├── src/
│   └── data_lab/
├── tests/
├── devenv.nix
├── flake.nix
├── flake.lock
├── pyproject.toml
├── uv.lock
├── .gitignore
└── README.md
```

### `src/`

Contains the Python application code.

The project uses a `src/` layout to keep imports explicit and avoid accidentally importing code directly from the repository root.

### `tests/`

Contains automated tests.

### `sql/`

Contains SQL used by data workflows.

Its internal structure can evolve as the project grows.

### `data/samples/`

Contains small sample datasets that are safe to commit to Git.

Large, raw, processed, generated, or sensitive datasets should not be committed.

### `notebooks/`

Contains Jupyter notebooks for interactive exploration, experimentation, prototyping, and analysis.

The included:

```text
00_quickstart.ipynb
```

provides a minimal example for verifying the project environment and accessing local project paths.

Notebooks are intended for exploratory work.

Reusable application and pipeline logic should live under:

```text
src/data_lab/
```

This keeps notebooks lightweight while reusable code remains modular and testable.

## Tool Responsibilities

The environment intentionally keeps responsibilities separated.

### Nix / Flake

Nix provides the reproducible system environment.

The flake pins the versions of:

- nixpkgs
- devenv
- system-level development tools

`flake.lock` records the exact resolved Nix inputs.

### devenv

devenv defines the development environment on top of Nix.

Currently it is responsible for:

- configuring the development shell
- selecting the Python 3.12 runtime
- exposing development tools such as uv, Ruff, and DuckDB
- providing native runtime libraries required by Python packages
- managing local development services
- managing PostgreSQL initialization and local state

devenv keeps its internal state under:

```text
.devenv/
```

This directory is ignored by Git.

### uv

uv manages the Python project environment.

It is responsible for:

- `.venv`
- runtime dependencies
- development dependencies
- optional dependency groups
- dependency resolution
- `uv.lock`

The Python virtual environment remains independent from devenv:

```text
.devenv/  → devenv state and local service state
.venv/    → Python project environment
```

### `pyproject.toml`

`pyproject.toml` defines:

- project metadata
- supported Python version
- Python dependencies
- development dependencies
- optional dependency groups
- build configuration
- Ruff configuration

## Requirements

You need:

- Linux
- Nix
- Flakes enabled

You do not need to install Python, uv, Ruff, devenv, PostgreSQL, or DuckDB globally.

They are provided by the development environment.

## Quick Start

Clone the repository and enter it:

```bash
git clone https://github.com/codigoreymono/data-lab.git
cd data-lab
```

Enter the development environment:

```bash
nix develop --no-pure-eval
```

Synchronize the Python environment:

```bash
uv sync
```

The virtual environment will be created at:

```text
.venv/
```

Start local development services:

```bash
devenv up
```

`devenv up` opens a process manager interface and keeps the services running in that terminal.

Open another terminal for normal development work.

## Python

Check the Python runtime:

```bash
python --version
```

Run Python through the project environment:

```bash
uv run python
```

For example:

```bash
uv run python -c "import data_lab"
```

## Dependencies

Add a runtime dependency:

```bash
uv add <package>
```

For example:

```bash
uv add pandas
```

Add a development dependency:

```bash
uv add --dev <package>
```

Add a dependency to a dedicated optional group:

```bash
uv add --group <group> <package>
```

For example:

```bash
uv add --group notebooks <package>
```

`uv.lock` records the exact resolved Python dependencies and should be committed to Git.

## Notebooks

Jupyter support is available through the optional `notebooks` dependency group.

Install the notebook dependencies with:

```bash
uv sync --group notebooks
```

Start JupyterLab from the project root:

```bash
uv run --group notebooks jupyter lab
```

The repository includes:

```text
notebooks/
└── 00_quickstart.ipynb
```

The quickstart notebook can be used to verify the Python environment and project paths.

When JupyterLab is started from the repository root, the quickstart notebook can work with project directories such as:

```python
from pathlib import Path

PROJECT_ROOT = Path.cwd()

DATA_DIR = PROJECT_ROOT / "data"
SAMPLES_DIR = DATA_DIR / "samples"
```

Notebooks are intended for:

- exploration
- experimentation
- interactive analysis
- prototyping

Reusable project logic should live under:

```text
src/data_lab/
```

This keeps exploratory work separate from reusable application and pipeline code.

Jupyter checkpoint directories are ignored by Git.

### VS Code

The same notebook environment can be used from Visual Studio Code.

If VS Code is installed, open it from the development shell:

```bash
code .
```

Then select the project Python interpreter as the notebook kernel:

```text
.venv/bin/python
```

There is no need to start JupyterLab separately when using notebooks directly from VS Code.

## Tests

Run the test suite:

```bash
uv run pytest
```

## Linting

Check the project with Ruff:

```bash
ruff check .
```

## Formatting

Check formatting without modifying files:

```bash
ruff format --check .
```

Format the project:

```bash
ruff format .
```

## PostgreSQL

PostgreSQL 17 is managed as a local development service by devenv.

Start the service with:

```bash
devenv up
```

The local PostgreSQL instance is configured with:

- Host: `127.0.0.1`
- Port: `5433`
- Database: `data_lab`

Connect with:

```bash
psql -h 127.0.0.1 -p 5433 -d data_lab
```

devenv manages PostgreSQL initialization and local state automatically.

The database state persists between service restarts and is stored under devenv's local state in:

```text
.devenv/
```

Because `.devenv/` is ignored by Git, local PostgreSQL data is not committed to the repository.

No manual `initdb` or `pg_ctl` workflow is required for normal development.

## DuckDB

DuckDB is available directly from the development environment as a local analytical SQL engine.

Check the installed version:

```bash
duckdb --version
```

Run a simple query:

```bash
duckdb -c "SELECT 42 AS answer;"
```

DuckDB can query files such as CSV and Parquet directly without requiring a separate database service.

For example:

```bash
duckdb -c "SELECT * FROM read_csv_auto('data/samples/example.csv');"
```

DuckDB is provided as a command-line tool by the development environment.

It is not added as a Python project dependency by default.

## Data

Only small and reproducible sample datasets should be committed under:

```text
data/samples/
```

Other files under `data/` are ignored by default.

Do not commit:

- large datasets
- raw production data
- processed datasets
- generated outputs
- sensitive or private data

## Environment Variables

Local environment files are ignored by Git:

```text
.env
.env.*
```

An example configuration can be committed as:

```text
.env.example
```

Never commit passwords, API keys, database credentials, tokens, or other secrets.

## devenv Binary Cache

Data Lab works without the devenv binary cache.

However, the first Nix build may take significantly longer because some dependencies may need to be built locally.

Using the devenv binary cache is optional but recommended.

### NixOS

Add the following to your NixOS configuration:

```nix
nix.settings = {
  extra-substituters = [
    "https://devenv.cachix.org"
  ];

  extra-trusted-public-keys = [
    "devenv.cachix.org-1:w1cLUi8dv3hnoSPGAuibQv+f9TZLr6cv/Hm9XgU50cw="
  ];
};
```

Then rebuild your system.

### Other multi-user Nix installations

Configure the Nix daemon to use:

```text
extra-substituters = https://devenv.cachix.org
extra-trusted-public-keys = devenv.cachix.org-1:w1cLUi8dv3hnoSPGAuibQv+f9TZLr6cv/Hm9XgU50cw=
```

The exact configuration location depends on how Nix was installed.

The cache is an optimization and is not required for the project to function.

## Using the Python Project Without Nix

The full Data Lab environment is designed around Linux and Nix.

The Python project can still be used independently if you provide:

- Python 3.12
- uv

Then run:

```bash
uv sync
uv run pytest
```

To include notebook support:

```bash
uv sync --group notebooks
uv run --group notebooks jupyter lab
```

In this mode, system tools and services normally provided by the Nix/devenv environment must be installed and managed separately.

This includes PostgreSQL, DuckDB, and any native runtime libraries required by Python packages.

## Philosophy

Data Lab aims to provide a small, reproducible foundation rather than a complete data platform.

Tools and services are added only when they solve a real requirement.

The current separation is intentionally simple:

```text
Nix / Flake
    ↓
system reproducibility

devenv
    ├── development environment
    ├── Python runtime
    ├── native runtime libraries
    ├── development tools
    │       └── DuckDB
    └── local services
            └── PostgreSQL

uv
    ├── Python dependencies
    ├── optional dependency groups
    └── .venv

src / sql / tests / data
    ↓
project code and resources

notebooks
    ↓
exploration and interactive analysis
```

The environment should remain understandable, reproducible, and easy to extend as data projects grow.
