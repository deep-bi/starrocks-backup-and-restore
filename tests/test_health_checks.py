import starrocks_br.health as health


def should_report_healthy_when_all_nodes_alive_and_ready(mocker):

    db = mocker.Mock()
    db.query.side_effect = [
        [
            ("fe1", "ALIVE", "TRUE", "TRUE"),
            ("fe2", "ALIVE", "TRUE", "FALSE"),
        ],
        [
            ("be1", "ALIVE", "TRUE"),
            ("be2", "ALIVE", "TRUE"),
        ],
    ]

    ok, msg = health.check_cluster_health(db)
    assert ok is True
    assert "healthy" in msg.lower()


def should_report_unhealthy_when_any_node_dead_or_not_ready(mocker):

    db = mocker.Mock()
    db.query.side_effect = [
        [
            ("fe1", "ALIVE", "TRUE", "TRUE"),
            ("fe2", "DEAD", "FALSE", "FALSE"),
        ],
        [
            ("be1", "ALIVE", "TRUE"),
            ("be2", "DECOMMISSION", "FALSE"),
        ],
    ]

    ok, msg = health.check_cluster_health(db)
    assert ok is False
    assert "dead" in msg.lower() or "decommission" in msg.lower()


def should_report_unhealthy_when_be_not_ready(mocker):
    db = mocker.Mock()
    db.query.side_effect = [
        [("fe1", "ALIVE", "TRUE", "TRUE")],
        [("be1", "ALIVE", "FALSE")],
    ]

    ok, msg = health.check_cluster_health(db)
    assert ok is False
    assert "unhealthy" in msg.lower()
