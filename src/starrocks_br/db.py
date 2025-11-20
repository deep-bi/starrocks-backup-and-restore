from typing import Any, Optional

import mysql.connector


class StarRocksDB:
    """Database connection wrapper for StarRocks."""

    def __init__(
        self,
        host: str,
        port: int,
        user: str,
        password: str,
        database: str,
        tls_config: Optional[dict[str, Any]] = None,
    ):
        """Initialize database connection.

        Args:
            host: Database host
            port: Database port
            user: Database user
            password: Database password
            database: Default database name
        """
        self.host = host
        self.port = port
        self.user = user
        self.password = password
        self.database = database
        self._connection = None
        self.tls_config = tls_config or {}
        self._timezone: Optional[str] = None

    def connect(self) -> None:
        """Establish database connection."""
        conn_args: dict[str, Any] = {
            "host": self.host,
            "port": self.port,
            "user": self.user,
            "password": self.password,
            "database": self.database,
        }

        if self.tls_config.get("enabled"):
            ssl_args: dict[str, Any] = {
                "ssl_ca": self.tls_config.get("ca_cert"),
                "ssl_cert": self.tls_config.get("client_cert"),
                "ssl_key": self.tls_config.get("client_key"),
                "ssl_verify_cert": self.tls_config.get("verify_server_cert", True),
            }

            tls_versions = self.tls_config.get("tls_versions", ["TLSv1.2", "TLSv1.3"])
            if tls_versions:
                ssl_args["tls_versions"] = tls_versions

            conn_args.update({key: value for key, value in ssl_args.items() if value is not None})

        try:
            self._connection = mysql.connector.connect(**conn_args)
        except mysql.connector.Error as e:
            if self.tls_config.get("enabled") and "SSL is required" in str(e):
                raise mysql.connector.Error(
                    f"TLS is enabled in configuration but StarRocks server doesn't support it. "
                    f"Error: {e}. "
                    f"To fix this, you need to enable TLS/SSL in your StarRocks server configuration. "
                    f"Alternatively, set 'enabled: false' in the tls section of your config file."
                ) from e
            raise

    def close(self) -> None:
        """Close database connection."""
        if self._connection:
            self._connection.close()
            self._connection = None

    def execute(self, sql: str) -> None:
        """Execute a SQL statement that doesn't return results.

        Args:
            sql: SQL statement to execute
        """
        if not self._connection:
            self.connect()

        cursor = self._connection.cursor()
        try:
            cursor.execute(sql)
            self._connection.commit()
        finally:
            cursor.close()

    def query(self, sql: str, params: tuple = None) -> list[tuple]:
        """Execute a SQL query and return results.

        Args:
            sql: SQL query to execute
            params: Optional tuple of parameters for parameterized queries

        Returns:
            List of tuples containing query results
        """
        if not self._connection:
            self.connect()

        cursor = self._connection.cursor()
        try:
            if params:
                cursor.execute(sql, params)
            else:
                cursor.execute(sql)
            return cursor.fetchall()
        finally:
            cursor.close()

    def __enter__(self):
        """Context manager entry."""
        self.connect()
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        """Context manager exit."""
        self.close()

    @property
    def timezone(self) -> str:
        """Get the StarRocks cluster timezone.

        Queries the cluster timezone on first access and caches it for subsequent use.
        If the query fails (e.g., database unavailable, connection error, permissions),
        defaults to 'UTC' to ensure the property always returns a valid timezone string.

        Returns:
            Timezone string (e.g., 'Asia/Shanghai', 'UTC', '+08:00')
            Defaults to 'UTC' if query fails or returns no results.
        """
        if self._timezone is None:
            try:
                query = "SHOW VARIABLES LIKE 'time_zone'"
                rows = self.query(query)

                if not rows:
                    self._timezone = "UTC"
                else:
                    row = rows[0]
                    if isinstance(row, dict):
                        self._timezone = row.get("Value", "UTC")
                    else:
                        self._timezone = row[1] if len(row) > 1 else "UTC"
            except Exception:
                self._timezone = "UTC"

        return self._timezone
