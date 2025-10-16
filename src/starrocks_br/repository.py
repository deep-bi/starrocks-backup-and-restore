from __future__ import annotations

from typing import Dict


def ensure_repository(db, cfg: Dict[str, object]) -> None:
    """Ensure a StarRocks repository exists and matches config.

    Minimal approach: relies entirely on StarRocks SQL. We treat the repo as matching
    if endpoint, region, bucket, and prefix align based on SHOW output projection.
    """
    name = str(cfg.get("name", "br_repo"))
    repo_type = str(cfg.get("type", "s3"))
    endpoint = str(cfg.get("endpoint"))
    bucket = str(cfg.get("bucket"))
    prefix = str(cfg.get("prefix", "/"))
    access_key = str(cfg.get("access_key"))
    secret_key = str(cfg.get("secret_key"))
    force_https = bool(cfg.get("force_https", True))

    if force_https and endpoint.startswith("http://"):
        raise ValueError("Repository endpoint must be HTTPS when force_https is True")

    existing = _find_repository(db, name)
    if not existing:
        _create_repository(db, name, repo_type, endpoint, bucket, prefix, access_key, secret_key)
        return

    _, exist_type, location, props = existing
    if exist_type.lower() != repo_type.lower():
        raise RuntimeError(f"Repository type mismatch: expected {repo_type}, got {exist_type}")

    if not location.startswith(f"s3://{bucket}") or not location.endswith(prefix.strip("/")):
        raise RuntimeError("Repository location mismatch (bucket/prefix)")

    # Validate endpoint; region may be omitted depending on setup
    if f"endpoint={endpoint}" not in props:
        raise RuntimeError("Repository properties mismatch (endpoint)")


def _find_repository(db, name: str):
    rows = db.query("SHOW REPOSITORIES")
    for row in rows:
        if row and row[0] == name:
            return row
    return None


def _create_repository(db, name: str, repo_type: str, endpoint: str, bucket: str, prefix: str, access_key: str, secret_key: str) -> None:
    # StarRocks S3 via BROKER (MinIO compatible):
    # CREATE REPOSITORY repo_name
    # WITH BROKER
    # ON LOCATION "s3://bucket/prefix"
    # PROPERTIES(
    #   "aws.s3.access_key" = "...",
    #   "aws.s3.secret_key" = "...",
    #   "aws.s3.endpoint"  = "http://minio:9000"
    # );

    if repo_type.lower() != "s3":
        raise NotImplementedError("Only S3 repository type is supported for now")

    location = f"s3://{bucket}/{prefix.strip('/')}" if prefix.strip('/') else f"s3://{bucket}"
    sql = (
        f"CREATE REPOSITORY {name}\n"
        f"WITH BROKER\n"
        f"ON LOCATION '{location}'\n"
        f"PROPERTIES(\n"
        f"  'aws.s3.access_key' = '{access_key}',\n"
        f"  'aws.s3.secret_key' = '{secret_key}',\n"
        f"  'aws.s3.endpoint'  = '{endpoint}'\n"
        f")"
    )
    db.execute(sql)


