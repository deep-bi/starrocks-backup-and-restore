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

from datetime import datetime
from typing import Literal


def determine_backup_label(
    db,
    backup_type: Literal["incremental", "full"],
    database_name: str,
    custom_name: str | None = None,
    ops_database: str = "ops",
) -> str:
    """Determine a unique backup label for the given parameters.

    This is the single entry point for all backup label generation. It handles both
    custom names and auto-generated date-based labels, ensuring uniqueness by checking
    the backup_history table in the configured ops database.

    Args:
        db: Database connection
        backup_type: Type of backup (incremental, full)
        database_name: Name of the database being backed up
        custom_name: Optional custom name for the backup. If provided, this becomes
                    the base label. If None, generates a date-based label.
        ops_database: Name of the database containing operational tables. Defaults to "ops".

    Returns:
        Unique label string that doesn't conflict with existing backups
    """
    if custom_name:
        base_label = custom_name
    else:
        today = datetime.now().strftime("%Y%m%d")
        base_label = f"{database_name}_{today}_{backup_type}"

    query = f"""
    SELECT label
    FROM {ops_database}.backup_history
    WHERE label LIKE %s
    ORDER BY label
    """

    pattern = f"{base_label}%"

    try:
        rows = db.query(query, (pattern,))
        existing_labels = [row[0] for row in rows] if rows else []
    except Exception:
        existing_labels = []

    if base_label not in existing_labels:
        return base_label

    retry_count = 1
    while True:
        candidate_label = f"{base_label}_r{retry_count}"
        if candidate_label not in existing_labels:
            return candidate_label
        retry_count += 1
