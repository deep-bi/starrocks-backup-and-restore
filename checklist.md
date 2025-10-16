## Full Implementation Checklist for StarRocks Backup & Restore

This checklist is organized into key areas, from initial setup and strategy to automation development, operational procedures, and ongoing governance.

### Phase 1: Strategy & Configuration

* ` ` **Define Policies**:
* ` ` Formalize the backup schedule: Daily Incremental (Mon-Sat), Weekly Full (Sun), and Monthly Full Baseline (1st Sunday).
* ` ` Set the data retention periods for each backup type: 14 days for incrementals, 90 days for weekly fulls, and 365 days for monthly fulls.
* `X` **Repository Setup**:
* `X` Provision the primary backup repository (e.g., S3 bucket or NFS mount).
* `X` Configure S3 lifecycle rules to automatically purge expired backups according to the retention policy.
* ` ` **Security Controls**:
* ` ` Enforce HTTPS-only communication for the repository.
* ` ` Create a dedicated IAM role with the least-privilege policy for bucket access.
* ` ` Enable S3 bucket versioning and consider using Object Lock for critical backups.
* ` ` Configure networking to use private S3 endpoints if applicable.
* `X` **Automation Control Repo** (IF NOT EXISTS):
* `X` Create the `ops` database or schema to hold control tables.
* `X` Create and populate the `table_inventory` to classify tables as partitioned fact tables, dimension tables, etc., and mark their eligibility for different backups.
* `X` Create the `backup_history` and `restore_history` tables to log every job attempt, success, and failure.
* `X` Create the `ops.run_status` table to manage job concurrency and prevent overlapping backups.

---

### Phase 2: Automation & Tooling Development

* ` ` **Pre-Backup Checks**: Before initiating any backup, the script must perform these checks:
* `X` Verify FE and BE cluster health is nominal.
* `X` Query `ops.run_status` to ensure no conflicting backup or restore job is active.
* `X` Test repository connectivity and write permissions.
* ` ` Check that the `/starrocks/data/snapshot` directory has more than 15% free disk space.
* `X` If all checks pass, mark the job as `ACTIVE` in `ops.run_status`.
* ` ` **Backup Execution Logic**:
* `X` **For Daily Incrementals**: Dynamically query `information_schema.partitions` to find partitions updated in the last N days and build the `BACKUP ... PARTITION (...)` command .
* ` ` **For Weekly Fulls**: Query the `table_inventory` to get the list of dimension and non-partitioned tables to include in the `BACKUP ... ON (TABLE ...)` command.
* ` ` **For Monthly Baselines**: Generate a simple `BACKUP DATABASE ...` command for the entire database.
* `X` Generate a unique, standardized snapshot label for each job (e.g., `dbvrmd1_20250914_inc`).
* ` ` **Post-Backup Logic**:
* ` ` Poll `SHOW BACKUP` until the job state is `FINISHED` or `FAILED`.
* ` ` On completion, write the final status, byte count, and object count to the `ops.backup_history` table.
* ` ` Clear the job's `ACTIVE` status from `ops.run_status`.
* ` ` **Failure Handling**: The script must gracefully handle common errors:
* ` ` Immediately fail and alert on repository connectivity issues.
* ` ` Implement an exponential backoff retry mechanism for active job conflicts.
* ` ` Automatically add a `_r#` suffix to the label if a label collision occurs.
* ` ` Halt the job without creating a partial backup if storage is insufficient.

---

### Phase 3: Operational Procedures & Drills

* ` ` **Documentation**:
* ` ` Create a formal runbook for restoring a single partition.
* ` ` Create a formal runbook for restoring a full table or an entire database.
* ` ` Document the process for retrieving the mandatory `backup_timestamp` property needed for any restore operation.
* ` ` **Disaster Recovery (DR) Drills**:
* ` ` Schedule and perform **monthly** restores of single partitions from incremental backups.
* ` ` Schedule and perform **quarterly** restores of full tables from the weekly backups.
* ` ` Schedule and perform **semi-annual** full database DR tests from the monthly baseline.
* ` ` Log the start time, finish time, and verification checksums for every drill into the `restore_history` table for auditing.

---

### Phase 4: Monitoring, Alerting & Governance

* ` ` **Monitoring & KPIs**:
* ` ` Set up a dashboard to track key metrics: backup success rate, GB/minute throughput, and job completion latency.
* ` ` **Alerting**:
* ` ` Configure critical alerts for immediate action:
* Any backup job that fails or is cancelled.
* A backup job running longer than its allowed time window.
* Any repository access failure (e.g., S3 5xx errors).
* The snapshot path disk space dropping below 15% free.
* ` ` **Ongoing Governance**:
* ` ` Integrate the final, robust script with a production scheduler (e.g., Airflow, Cron) to automate the entire workflow.
* ` ` Implement a secondary "safety" script that periodically cross-checks the `backup_history` table against the objects in the repository to identify and alert on any orphaned backups missed by lifecycle policies.
* ` ` Set up regular audits of system and application logs for all backup and restore activities.

### Phase 5: REFACTORING EXISTING CODE (if time permits, do this only after all other phases are completed)

* ` ` Convert string (and other primitive types) to validated value-objects to improve type safety and readability following the principles of Domain-Driven Design.