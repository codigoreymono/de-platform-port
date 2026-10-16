# AWS Deployment Runbook

This runbook describes the **manual, one-off** deployment and operation of the
September 2026 NYC 311 pipeline on AWS. It is written so that every step that
reads, changes, or invokes an AWS resource is called out as requiring explicit
operator authorization.

Nothing in this repository reads, modifies, deploys, or invokes AWS resources by
itself. The offline test suite, packaging, benchmarks, and Terraform static
checks (`fmt`, `validate`) never contact AWS. Deployment and execution are
operator actions taken only after the predeployment gate below passes.

## Scope And Non-Goals

The stack provisions only:

- One project-owned S3 bucket/prefix.
- Three Lambda functions: `ingest`, `curate`, `gold`.
- One Step Functions Standard state machine, started manually.
- One Glue Catalog database with the Silver table (30 finite partitions) and the
  four Gold product tables.
- One Athena workgroup and result location.
- The IAM roles and policies needed by those resources.

It does **not** configure a scheduler, EventBridge rule, VPC, NAT, queue,
crawler, Glue ETL job, Spark runtime, database, or any additional application
service. There is no automatic retry orchestration and no overlapping execution
support. Lambda reaches the public internet outside a VPC.

## Predeployment Gate

Do not deploy until all of the following are true:

1. The offline suite passes: `uv run pytest` (no network, no credentials).
2. Stage ZIPs are built and reproducible: `scripts/build_lambda_zip.py`.
3. The Lambda compatibility checks pass (`scripts/check_lambda_runtime.py`,
   `scripts/check_lambda_linkage.py`) and each stage's aggregate uncompressed
   ZIP is within 250 MB.
4. The Lambda benchmarks pass within the configured memory, `/tmp`, and
   900-second timeout headroom (`docs/deployment-readiness.md`).
5. `terraform fmt -check` and `terraform validate` pass.

Record the outcome in `docs/deployment-readiness.md`. Any oversized or
incompatible package or insufficient task headroom **blocks** deployment. Do not
introduce a container/ECR or other-service fallback without new approval.

## Build And Upload The Stage ZIPs

Build locally (no AWS access). Each build needs a stage and a new output
directory; each directory receives `nyc311-<stage>.zip`:

```bash
uv run --offline --frozen --no-python-downloads python scripts/build_lambda_zip.py ingest --output dist/ingest
uv run --offline --frozen --no-python-downloads python scripts/build_lambda_zip.py curate --output dist/curate
uv run --offline --frozen --no-python-downloads python scripts/build_lambda_zip.py gold   --output dist/gold
```

`docs/lambda-packaging.md` lists the exact per-stage inventory and the cache
prerequisites. Compute the base64 SHA-256 used for deployment change detection:

```bash
openssl dgst -sha256 -binary dist/ingest/nyc311-ingest.zip | base64
openssl dgst -sha256 -binary dist/curate/nyc311-curate.zip | base64
openssl dgst -sha256 -binary dist/gold/nyc311-gold.zip  | base64
```

> **Requires AWS authorization.** Upload the three ZIPs to the code prefix of the
> bucket (`lambda-code/` by default) with the AWS CLI or console. Terraform
> references these objects by key; it does not build or upload them. The bucket
> must exist first, so follow the bootstrap ordering below on a fresh account.

## First-Time Bootstrap Ordering

The Lambda functions reference the ZIPs by key, but Terraform creates the bucket
that holds them. On a fresh account, apply in two phases so the bucket exists
before the first function is created:

> **Requires AWS authorization for every phase below.**

1. Create only the bucket and its hardening (targeted apply):

   ```bash
   terraform -chdir=infra apply \
     -target=aws_s3_bucket.data \
     -target=aws_s3_bucket_public_access_block.data \
     -target=aws_s3_bucket_ownership_controls.data \
     -target=aws_s3_bucket_server_side_encryption_configuration.data
   ```

2. Upload the built ZIPs to the bucket (from "Build And Upload" above).
3. Apply the remainder with the normal plan/apply flow in "Manual Deployment".

If the bucket already exists and is imported into Terraform state, skip step 1.
Do not remove the bucket from Terraform management; targeted apply is only a
one-time bootstrap convenience.

## Terraform Configuration

Terraform files live in `infra/`. Copy the example variables file and edit it:

```bash
cp infra/terraform.tfvars.example infra/terraform.tfvars
```

Key variables:

| Variable | Meaning |
|---|---|
| `bucket_name` | Globally unique project-owned bucket. |
| `data_prefix` | Prefix scoping every pipeline object (default `nyc311`). |
| `mode` | Must stay `published`; AWS writes only the official namespace. |
| `glue_database_name` | Glue Catalog database (default `nyc311`). |
| `athena_result_prefix` | Athena result prefix, outside every table location. |
| `*_zip_key` | S3 keys of the uploaded stage ZIPs. |
| `*_source_hash` | Base64 SHA-256 of each ZIP for redeploy detection. |

The state machine, Glue schemas, Silver partitions, and Gold table locations are
derived from a fixed September 2026 cohort and the four product schemas. The
Gold tables are created pointing at `run=unpublished`; the Gold task repoints
them to the selected run after a successful publication.

### Static Checks Without AWS Access

These never contact AWS and are safe to run any time:

```bash
terraform -chdir=infra fmt -check -recursive
terraform -chdir=infra init -backend=false -input=false
terraform -chdir=infra validate
```

`init -backend=false` downloads the provider only; it creates no backend and
reads no resource. **These checks do not authorize a plan, apply, or invocation.**

### Least-Privilege Scope

- `nyc311-ingest` and `nyc311-curate`: read/write objects only under
  `<data_prefix>/published/*`, and write logs to their own CloudWatch group.
- `nyc311-gold`: the same S3 scope plus `glue:GetTable`, `glue:GetTables`, and
  `glue:UpdateTable` on the catalog database and its tables.
- `nyc311-states`: `lambda:InvokeFunction` on the three stage functions only.

The Curate function sets `PYTHONTZPATH=/var/task/timezone` so the packaged
timezone database resolves inside Lambda. Credentials are never embedded in code
or committed to Terraform variables.

## Manual Deployment

> **Requires AWS authorization.** The operator must explicitly approve the named
> AWS account and region and the planned resource changes before any of these
> steps.

1. Inspect the plan and confirm it contains only the approved services:

   ```bash
   terraform -chdir=infra plan -out=tfplan
   ```

2. Apply only after reviewing the plan:

   ```bash
   terraform -chdir=infra apply tfplan
   ```

3. Note the outputs: `bucket_name`, `mode_namespace`, `function_names`,
   `state_machine_arn`, `glue_database`, `athena_workgroup`.

Do not add a scheduler, VPC, or extra service at apply time.

## Manual Execution

The state machine expects a single required input field. Everything else (bucket,
prefix, mode, the 30 daily windows, the catalog database) is baked into the
definition at deploy time:

```json
{
  "run_id": "september-2026-001"
}
```

Start one execution manually. Use a new `run_id` for every attempt, including
recovery attempts.

> **Requires AWS authorization.** Starting an execution invokes the three Lambda
> functions and reads/writes the project bucket.

```bash
aws stepfunctions start-execution \
  --state-machine-arn "<state_machine_arn>" \
  --input '{"run_id": "september-2026-001"}'
```

The state machine runs strictly sequentially:

1. `DailyIngestCurate` (Map, `MaxConcurrency=1`) iterates the 30 days: Ingest
   writes Bronze + a complete daily manifest; Curate writes the Silver partition
   + quality/quarantine + a complete daily manifest.
2. `Gold` verifies full-month coverage and uniqueness, computes the four
   products under `gold/run=<run_id>/`, and then repoints the four Glue Gold
   tables to that run.

Any unrecovered task failure fails the execution. There is no catch/retry
orchestration: repair and restart manually. Gold is not published if any day is
missing, incomplete, or has a cross-day identifier conflict.

### Non-Overlap Check

Before starting, confirm no earlier execution is running:

```bash
aws stepfunctions list-executions \
  --state-machine-arn "<state_machine_arn>" --status-filter RUNNING
```

One execution at a time is an operating precondition. There is no lock service.

### Catalog Publication And Recovery

The Gold task updates all four Glue table locations after the artifacts are
written and verified. A catalog failure raises the task error: the execution is
unsuccessful, but the already-published Gold artifacts remain and the **previous**
table locations are untouched for recovery.

To recover a failed catalog update, do not delete Gold. Re-run the Gold task
(with the same coverage) or repoint the four tables manually to the prior
successful run prefix, then re-run the catalog update. Readers must wait for a
successful run marker before consuming a new publication.

## Querying With Athena

> **Requires AWS authorization.** Athena reads S3 and the Glue catalog.

Use the workgroup from the outputs, and query only the four Gold tables:

```sql
SELECT * FROM nyc311.demand_daily ORDER BY request_count DESC LIMIT 100;
```

The analytical definitions live in `docs/analytics.md`. Athena and DuckDB SQL
dialects are not interchangeable; the documented Athena queries use the same
grains and denominators as the local products. Never glob across Gold run
prefixes: the tables already select one published run.

## Costs

The stack is deliberately small and bounded:

- **S3**: storage for Bronze/Silver/Gold/control objects plus Athena results.
- **Lambda**: billed per request and GB-second for 30 daily Ingest+Curate pairs
  and one Gold invocation per run.
- **Step Functions Standard**: billed per state transition for ~30 map
  iterations plus the Gold step.
- **Glue Catalog, Athena, CloudWatch Logs**: catalog storage is negligible;
  Athena is billed per bytes scanned by manual queries; logs are retained for
  `log_retention_days`.

There is no always-on compute. Costs accrue only while an authorized run or
query is executed.

## Rollback

Rollback is manual and non-atomic (there is no cross-object transaction):

1. Repoint the four Gold Glue tables to the prior successful run prefix.
2. Optionally restore affected Silver partitions from a prior complete input by
   re-running Ingest/Curate for those days or replaying saved Bronze.
3. Rebuild Gold only after coverage is verified.

Never present a failed refresh as a successful publication. Retain the failed
run's manifests and artifacts for diagnosis.

## Cleanup

> **Requires AWS authorization and explicit confirmation.** Cleanup removes
> project-owned resources and data only.

1. Delete the S3 bucket contents and bucket.
2. Delete the three Lambda functions, log groups, and the state machine.
3. Delete the Glue database/tables and the Athena workgroup.

`terraform -chdir=infra destroy` performs the same removal for resources
Terraform manages. Cleanup must never remove unrelated resources or the local
`data/local/` outputs. If cleanup is not authorized, leave it documented as
pending rather than deleting anything.

## Operator Handoff Checklist

Everything below is a manual, authorized operator action. Nothing here is
executed by the repository or its tests.

Local, no AWS (already verified in this repository):

- [ ] `uv run pytest`, `ruff check .`, `ruff format --check .` pass.
- [ ] Stage ZIPs are built and repeat builds are byte-identical.
- [ ] Compatibility/linkage checks and the benchmark gate pass; the readiness
      report records the results.
- [ ] `terraform -chdir=infra fmt -check -recursive` and `validate` pass.

AWS, requires explicit authorization:

- [ ] Confirm the named account/region and the planned resource changes.
- [ ] Bootstrap the bucket, then upload the three ZIPs (see above).
- [ ] Review `terraform -chdir=infra plan`, then apply only project-owned
      resources; confirm no scheduler, VPC or extra service is added.
- [ ] Confirm no execution is already `RUNNING` (non-overlap check).
- [ ] Start one execution with a fresh `run_id` and watch it complete.
- [ ] Verify all 30 daily manifests, the complete Gold run, and the four Glue
      table locations pointing at that run.
- [ ] Query all four products through Athena using the documented definitions.
- [ ] Optionally demonstrate a selected-day refresh or Bronze replay.
- [ ] Record the observed AWS measurements and any deviation in the readiness
      report; if a run fails, keep the manifests and prior Gold locations and
      recover manually instead of presenting the failure as a publication.

The authorized run was completed and the deployment-evidence
[test report](test-evidence.md) section is filled in; the operator checklist above
is satisfied, and the project resources were subsequently cleaned up.
