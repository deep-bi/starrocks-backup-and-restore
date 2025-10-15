from typing import Tuple


def check_cluster_health(db) -> Tuple[bool, str]:
    """Check FE/BE health via SHOW FRONTENDS/BACKENDS.

    Returns (ok, message).
    """
    fe_rows = db.query("SHOW FRONTENDS")
    be_rows = db.query("SHOW BACKENDS")

    def is_alive(value: str) -> bool:
        return str(value).upper() in {"ALIVE", "TRUE", "YES", "1"}

    any_dead = False
    for row in fe_rows:
        status = str(row[1]).upper() if len(row) > 1 else "ALIVE"
        ready = str(row[2]).upper() if len(row) > 2 else "TRUE"
        if status != "ALIVE" or ready not in {"TRUE", "YES", "1"}:
            any_dead = True
            break

    if not any_dead:
        for row in be_rows:
            status = str(row[1]).upper() if len(row) > 1 else "ALIVE"
            ready = str(row[2]).upper() if len(row) > 2 else "TRUE"
            if status not in {"ALIVE"} or ready not in {"TRUE", "YES", "1"}:
                any_dead = True
                break

    if any_dead:
        return False, "Cluster unhealthy: some FE/BE are DEAD or not READY"
    return True, "Cluster healthy: all FE/BE are ALIVE and READY"


