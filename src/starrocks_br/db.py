import mysql.connector
from typing import List


class StarRocksDB:
    """Database connection wrapper for StarRocks."""
    
    def __init__(self, host: str, port: int, user: str, password: str, database: str):
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
    
    def connect(self) -> None:
        """Establish database connection."""
        self._connection = mysql.connector.connect(
            host=self.host,
            port=self.port,
            user=self.user,
            password=self.password,
            database=self.database
        )
    
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
    
    def query(self, sql: str) -> List[tuple]:
        """Execute a SQL query and return results.
        
        Args:
            sql: SQL query to execute
            
        Returns:
            List of tuples containing query results
        """
        if not self._connection:
            self.connect()
        
        cursor = self._connection.cursor()
        try:
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

