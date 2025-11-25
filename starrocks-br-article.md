# Introducing starrocks-br: Production-Grade Incremental Backups for StarRocks

As the StarRocks ecosystem matures and adoption grows, the need for reliable, production-grade operational tooling has never been more critical. StarRocks delivers exceptional speed for OLAP workloads, but managing lifecycle operations—specifically backups and disaster recovery—often falls to manual SQL scripts or fragile cron jobs cobbled together by each team independently.

We built `starrocks-br` to change that.

## The Problem: Backup Complexity at Scale

StarRocks provides powerful built-in SQL commands for backup and restore operations through `BACKUP SNAPSHOT` and `RESTORE SNAPSHOT`. For small datasets or development environments, these work fine. But as organizations scale to terabytes and petabytes of data, the limitations become painfully apparent.

The core issue is this: while StarRocks supports backing up specific partitions, **there's no built-in mechanism to automatically track which partitions have changed**. You can manually craft `BACKUP` commands for individual partitions, but identifying which partitions changed since your last backup requires custom tooling.

When you're managing a 100TB dataset that grows by 500GB daily, the math becomes unsustainable without automation. Manually identifying changed partitions across dozens of tables is error-prone and time-consuming. Most teams end up running full backups by default, copying 100TB even though only a fraction changed. The time, storage, and network bandwidth requirements make this approach impractical for many production environments.

Beyond the storage problem, there are operational challenges that emerge in production:

**State Management**: Who tracks which snapshot was successful? Where is the latest backup stored? When did it run? If you need to restore to yesterday at 3 PM, which snapshot label corresponds to that time?

**Automation**: Writing custom bash scripts to wrap SQL commands is error-prone, hard to maintain, and difficult to monitor. Every team ends up building their own fragile automation layer, reinventing the same wheels.

**Organization**: Not all tables are created equal. Some change constantly and need frequent backups. Others are reference data that rarely changes. But there's no built-in way to organize tables into groups with different backup strategies.

These aren't hypothetical concerns. They're the daily reality for teams running StarRocks in production.

## The Solution: deep-bi/starrocks-br

`starrocks-br` is a lightweight, metadata-driven CLI tool designed to solve these exact problems. It wraps the native StarRocks backup primitives into a clean, automatable Python interface that fits naturally into modern data infrastructure.

The philosophy is straightforward: leverage what StarRocks does well, add the missing pieces that production deployments need, and keep it simple enough that teams can adopt it without extensive training or infrastructure changes.

### How Incremental Backups Work

The key innovation is partition-level incremental backups. StarRocks organizes large tables into partitions, typically by date or another dimension. While StarRocks can back up individual partitions, there's no built-in mechanism to track which partitions have changed since the last backup.

`starrocks-br` fills this gap by maintaining a metadata database that records exactly which partitions were backed up, when, and under what label. When you run an incremental backup, the tool:

1. Identifies the most recent successful full backup as the baseline
2. Queries the current partition metadata from StarRocks
3. Compares it against the baseline stored in metadata
4. Backs up only the new or modified partitions

For a daily partitioned table with a year of historical data, this means backing up one partition instead of 365. The time and storage savings compound rapidly at scale.

### Metadata-Driven State

Unlike scripts that "fire and forget," `starrocks-br` maintains complete operational state in a dedicated database. Every backup operation is recorded with its label, timestamp, status, error messages, and a manifest of exactly which partitions were included.

This metadata serves multiple purposes. It enables intelligent restore operations where the tool can automatically resolve backup chains—determining which full backup and which incremental backups are needed to restore to a specific point in time. It provides a queryable audit trail for compliance and debugging. And it prevents concurrent operations from conflicting through job slot management.

The metadata is stored in a separate `ops` database within StarRocks itself, keeping everything in one place while isolating operational data from business data.

### Inventory Groups: Flexible Organization

Rather than treating all tables identically, `starrocks-br` introduces the concept of inventory groups. These are named collections of tables that share the same backup strategy.

You might group fast-changing transactional tables separately from slow-changing reference tables. Or organize by business criticality—mission-critical tables that need hourly backups versus less critical tables that can be backed up weekly. The grouping strategy depends entirely on your operational needs.

Once groups are defined, you simply reference them by name when running backups. The tool handles the rest, ensuring consistency and reducing the chance of human error.

### Surgical Restore Operations

One of the most valuable features for production environments is single-table point-in-time restore. In real-world scenarios, you rarely lose an entire cluster. More commonly, someone accidentally truncates a table, or a bad ETL job corrupts specific data.

`starrocks-br` allows you to restore just one table to a specific backup timestamp, minimizing downtime and data loss for the rest of your warehouse. The restore process uses temporary tables and atomic rename operations, so if anything goes wrong, your production data remains untouched.

This capability transforms disaster recovery from a nuclear option—restore everything and lose hours of data—into a precise surgical tool.

## Why This Matters

Data infrastructure at scale requires operational maturity that goes beyond query performance. When a restore operation determines whether your business meets SLAs or loses revenue, ad-hoc scripts aren't sufficient.

The challenge isn't just technical. It's organizational. Every team running StarRocks at scale faces the same backup problems, and most end up building custom solutions. That's wasted effort—reinventing the same wheels, debugging the same edge cases, maintaining the same fragile scripts.

`starrocks-br` consolidates this operational knowledge into a shared tool. It brings enterprise-grade backup discipline to StarRocks without enterprise-grade complexity. It's the missing piece that makes StarRocks viable for mission-critical workloads at petabyte scale.

## Design Principles

We made deliberate choices about what this tool should and shouldn't be:

**Minimal dependencies**: It's a Python CLI with a handful of standard dependencies. No complex infrastructure required. Install via pip and run immediately.

**Leverage native capabilities**: We don't reimplement backup mechanics. We wrap StarRocks's native commands with intelligence, organization, and state management.

**Fit existing workflows**: Whether you're using Airflow, cron jobs, or CI/CD pipelines, `starrocks-br` integrates naturally. It's designed to be automated.

**Opinionated but flexible**: The tool has opinions about metadata structure and operational patterns, but it doesn't force a specific backup schedule or organizational model. You define inventory groups and backup strategies that match your needs.

## Getting Started

Let's walk through a practical example to see how the tool works in practice.

### Installation

First, install the tool via pip or download the standalone executable:

```bash
python3 -m venv .venv
source .venv/bin/activate
pip install starrocks-br
```

### Initial Setup

Before using `starrocks-br`, you need a backup repository in StarRocks. This is where the actual backup data gets stored (S3, HDFS, Azure, etc.):

```sql
CREATE REPOSITORY `s3_backups`
WITH S3
ON LOCATION "s3://your-backup-bucket/starrocks-backups/"
PROPERTIES (
    "aws.s3.access_key" = "your-access-key",
    "aws.s3.secret_key" = "your-secret-key",
    "aws.s3.endpoint" = "https://s3.amazonaws.com"
);
```

Create a configuration file pointing to your StarRocks cluster:

```yaml
# config.yaml
host: "starrocks-fe.example.com"
port: 9030
user: "root"
database: "sales_db"
repository: "s3_backups"
```

Set your password as an environment variable:

```bash
export STARROCKS_PASSWORD="your_password"
```

### Initialize the Tool

Run the init command to create the `ops` database and metadata tables:

```bash
$ starrocks-br init --config config.yaml

Initializing ops schema...

Next steps:
1. Insert your table inventory records:
   INSERT INTO ops.table_inventory
   (inventory_group, database_name, table_name)
   VALUES ('my_daily_incremental', 'your_db', 'your_fact_table');
```

### Define Your Inventory Groups

Now define which tables you want to back up. Connect to StarRocks and insert rows into the inventory table:

```sql
-- Group critical transactional tables together
INSERT INTO ops.table_inventory (inventory_group, database_name, table_name)
VALUES
  ('critical', 'sales_db', 'orders'),
  ('critical', 'sales_db', 'payments'),
  ('critical', 'sales_db', 'customers');

-- Group less critical tables separately
INSERT INTO ops.table_inventory (inventory_group, database_name, table_name)
VALUES
  ('analytics', 'sales_db', 'daily_metrics'),
  ('analytics', 'sales_db', 'user_sessions');
```

### Run Your First Backup

Start with a full backup to establish the baseline:

```bash
$ starrocks-br backup full --config config.yaml --group critical

✓ Cluster health: All nodes healthy
✓ Repository 's3_backups' verified
✓ Generated label: sales_db_20251125_143022_full
✓ Job slot reserved
Starting full backup for group 'critical'...
✓ Backup completed successfully: FINISHED
```

The tool automatically generates a label, submits the backup job to StarRocks, polls for completion, and records all metadata.

### Run an Incremental Backup

After some time passes and data changes, run an incremental backup:

```bash
$ starrocks-br backup incremental --config config.yaml --group critical

✓ Cluster health: All nodes healthy
✓ Repository 's3_backups' verified
✓ Using latest full backup as baseline: sales_db_20251125_143022_full (full)
✓ Generated label: sales_db_20251125_160000_incremental
✓ Found 12 partition(s) to backup
✓ Job slot reserved
Starting incremental backup for group 'critical'...
✓ Backup completed successfully: FINISHED
```

Notice how it automatically detected the baseline, compared partition timestamps, and backed up only the 12 changed partitions.

### Restore a Table

Later, if you need to restore a table, the tool handles the complexity:

```bash
$ starrocks-br restore --config config.yaml --target-label sales_db_20251125_160000_incremental --table orders

=== RESTORE PLAN ===
Repository: s3_backups
Restore sequence: sales_db_20251125_143022_full -> sales_db_20251125_160000_incremental
Tables to restore: orders
Temporary table suffix: _restored

This will restore data to temporary tables and then perform atomic rename.
⚠ WARNING: This operation will replace existing tables!

Proceed? [y/N]: y

Step 1: Restoring base backup 'sales_db_20251125_143022_full'...
✓ Base restore completed successfully

Step 2: Applying incremental backup 'sales_db_20251125_160000_incremental'...
✓ Incremental restore completed successfully
```

The tool automatically resolved that it needs both the full backup and the incremental, restored them in the correct order, and used a temporary table so you can verify the data before swapping it into production.

### Check Backup History

You can query the metadata database at any time to see your backup history:

```sql
SELECT label, backup_type, status, finished_at
FROM ops.backup_history
WHERE label LIKE 'sales_db%'
ORDER BY finished_at DESC
LIMIT 5;
```

This gives you complete visibility into all backup operations, their status, and when they completed.

### What You've Accomplished

In just a few commands, you've:
- Set up automated backup infrastructure
- Organized tables into logical groups
- Run full and incremental backups
- Performed a surgical restore of a single table
- Established a queryable audit trail

For teams already running StarRocks in production, the migration path is straightforward. Create inventory groups for your existing tables, run a full backup to establish a baseline, then switch to incremental backups going forward. The metadata database tracks everything from that point on.

For new deployments, incorporate `starrocks-br` from day one. Define your inventory groups alongside your data models, schedule backups as part of your operational playbooks, and build confidence in your disaster recovery processes before you need them.


## Conclusion

StarRocks is an exceptional OLAP engine, but production deployments need more than fast queries. They need operational resilience. They need confidence that when something goes wrong—and something always goes wrong—you can recover quickly and precisely.

`starrocks-br` transforms StarRocks's basic backup primitives into a production-ready system. Incremental backups reduce storage and time costs. Metadata-driven state provides visibility and control. Intelligent restores minimize downtime and data loss.

If you're running StarRocks in production, we invite you to give `starrocks-br` a try. It's open source, lightweight, and built to make data reliability standard for everyone—not just teams with dedicated infrastructure engineering resources.

Check out the repository, read the documentation, run a backup, restore a table. That's the confidence your data platform should provide.

---

**Project Links**

Repository: https://github.com/deep-bi/starrocks-backup-and-restore
Documentation: https://github.com/deep-bi/starrocks-backup-and-restore/tree/main/docs
Issues & Discussions: https://github.com/deep-bi/starrocks-backup-and-restore/issues
