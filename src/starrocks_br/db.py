from __future__ import annotations

import logging
from contextlib import contextmanager
from dataclasses import dataclass
from typing import Any, Iterable, Iterator, List, Tuple

import mysql.connector
from mysql.connector import Error as MySQLError

logger = logging.getLogger(__name__)


@dataclass(frozen=True)
class Database:
    host: str
    port: int
    user: str
    password: str
    database: str

    @contextmanager
    def connect(self):
        try:
            conn = mysql.connector.connect(
                host=self.host,
                port=self.port,
                user=self.user,
                password=self.password,
                database=self.database,
            )
        except MySQLError as e:
            raise ConnectionError(
                f"failed to connect to StarRocks at {self.host}:{self.port}\n"
                f"  error: {e}\n"
                f"  help: verify that:\n"
                f"        - StarRocks is running\n"
                f"        - host and port are correct in config.yaml\n"
                f"        - user credentials are valid\n"
                f"        - database '{self.database}' exists"
            ) from e
        
        cursor = conn.cursor()
        try:
            yield conn
        finally:
            conn.commit()
            cursor.close()
            conn.close()

    def _cursor(self):
        try:
            conn = mysql.connector.connect(
                host=self.host,
                port=self.port,
                user=self.user,
                password=self.password,
                database=self.database,
            )
        except MySQLError as e:
            raise ConnectionError(
                f"failed to connect to StarRocks at {self.host}:{self.port}\n"
                f"  error: {e}\n"
                f"  help: verify that:\n"
                f"        - StarRocks is running\n"
                f"        - host and port are correct in config.yaml\n"
                f"        - user credentials are valid\n"
                f"        - database '{self.database}' exists"
            ) from e
        return conn, conn.cursor()

    def execute(self, sql: str, params: Tuple[Any, ...] | None = None) -> None:
        conn, cur = self._cursor()
        try:
            cur.execute(sql, params or ())
            conn.commit()
        except MySQLError as e:
            logger.error(f"SQL execution failed: {e}")
            logger.error(f"  query: {sql[:200]}{'...' if len(sql) > 200 else ''}")
            raise RuntimeError(
                f"SQL execution failed\n"
                f"  error: {e}\n"
                f"  query: {sql[:200]}{'...' if len(sql) > 200 else ''}"
            ) from e
        finally:
            cur.close()
            conn.close()

    def executemany(self, sql: str, seq_params: Iterable[Tuple[Any, ...]]) -> None:
        conn, cur = self._cursor()
        try:
            cur.executemany(sql, list(seq_params))
            conn.commit()
        finally:
            cur.close()
            conn.close()

    def query(self, sql: str, params: Tuple[Any, ...] | None = None) -> List[Tuple[Any, ...]]:
        conn, cur = self._cursor()
        try:
            cur.execute(sql, params or ())
            rows = cur.fetchall()
            return rows
        except MySQLError as e:
            logger.error(f"SQL query failed: {e}")
            logger.error(f"  query: {sql[:200]}{'...' if len(sql) > 200 else ''}")
            raise RuntimeError(
                f"SQL query failed\n"
                f"  error: {e}\n"
                f"  query: {sql[:200]}{'...' if len(sql) > 200 else ''}"
            ) from e
        finally:
            cur.close()
            conn.close()
