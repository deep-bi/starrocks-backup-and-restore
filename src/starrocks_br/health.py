def check_cluster_health(db) -> tuple[bool, str]:
    """Check FE/BE health via SHOW FRONTENDS/BACKENDS.

    Returns (ok, message).
    """
    fe_rows = db.query("SHOW FRONTENDS")
    be_rows = db.query("SHOW BACKENDS")

    def is_alive(value: str) -> bool:
        return str(value).upper() in {"ALIVE", "TRUE", "YES", "1"}

    any_dead = False
    for row in fe_rows:
        fe_joined_cluster = str(row[9]).upper() if len(row) > 9 else "TRUE"
        fe_is_alive = str(row[10]).upper() if len(row) > 10 else "TRUE"
        if not is_alive(fe_joined_cluster) or not is_alive(fe_is_alive):
            any_dead = True
            break

    if not any_dead:
        for row in be_rows:
            be_is_alive = str(row[8]).upper() if len(row) > 8 else "TRUE"
            if not is_alive(be_is_alive):
                any_dead = True
                break

    if any_dead:
        return False, "Cluster unhealthy: some FE/BE are DEAD or not READY"
    return True, "Cluster healthy: all FE/BE are ALIVE and READY"
