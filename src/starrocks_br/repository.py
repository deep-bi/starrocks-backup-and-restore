# Copyright 2025 deep-bi
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

from __future__ import annotations


def ensure_repository(db, name: str) -> None:
    """Verify that the specified repository exists and is accessible.

    Args:
        db: Database connection
        name: Repository name to verify

    Raises:
        RuntimeError: If repository doesn't exist or has errors
    """
    existing = _find_repository(db, name)
    if not existing:
        raise RuntimeError(
            f"Repository '{name}' not found. Please create it first using:\n"
            f"  CREATE REPOSITORY {name} WITH BROKER ON LOCATION '...' PROPERTIES(...)\n"
            f"For examples, see: https://docs.starrocks.io/docs/sql-reference/sql-statements/backup_restore/CREATE_REPOSITORY/"
        )

    # SHOW REPOSITORIES returns: RepoId, RepoName, CreateTime, IsReadOnly, Location, Broker, ErrMsg
    err_msg = existing[6]
    if err_msg and str(err_msg).strip().upper() not in {"", "NULL", "NONE"}:
        raise RuntimeError(f"Repository '{name}' has errors: {err_msg}")


def _find_repository(db, name: str):
    """Find a repository by name in SHOW REPOSITORIES output."""
    rows = db.query("SHOW REPOSITORIES")
    for row in rows:
        if row and row[1] == name:
            return row
    return None
