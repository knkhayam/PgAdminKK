"""Database connection and query handling with psycopg3"""

import json
import re
from pathlib import Path
from typing import Optional, Any
from dataclasses import dataclass, asdict

import psycopg
from psycopg import sql
from psycopg.rows import dict_row

CONNECTIONS_FILE = Path(__file__).parent / "connections.json"


@dataclass
class ConnectionInfo:
    name: str
    host: str = "localhost"
    port: int = 5432
    dbname: str = "postgres"
    user: str = "postgres"
    password: str = ""
    last_connected_at: Optional[str] = None  # ISO timestamp
    
    def to_dict(self) -> dict:
        return asdict(self)
    
    @classmethod
    def from_dict(cls, data: dict) -> "ConnectionInfo":
        # Handle old connections without last_connected_at
        if "last_connected_at" not in data:
            data["last_connected_at"] = None
        return cls(**data)
    
    def connection_string(self) -> str:
        return f"host={self.host} port={self.port} dbname={self.dbname} user={self.user} password={self.password}"


def load_connections() -> list[ConnectionInfo]:
    """Load saved connections from JSON file."""
    if not CONNECTIONS_FILE.exists():
        return []
    try:
        with open(CONNECTIONS_FILE, "r") as f:
            data = json.load(f)
        return [ConnectionInfo.from_dict(c) for c in data]
    except (json.JSONDecodeError, KeyError):
        return []


def save_connections(connections: list[ConnectionInfo]) -> None:
    """Save connections to JSON file."""
    with open(CONNECTIONS_FILE, "w") as f:
        json.dump([c.to_dict() for c in connections], f, indent=2)


def get_last_connection() -> Optional[ConnectionInfo]:
    """Get the most recently used connection."""
    connections = load_connections()
    if not connections:
        return None
    
    # Sort by last_connected_at descending, None values last
    def sort_key(c):
        return c.last_connected_at or ""
    
    sorted_conns = sorted(connections, key=sort_key, reverse=True)
    return sorted_conns[0] if sorted_conns else None


def update_connection_timestamp(name: str) -> None:
    """Update the last_connected_at timestamp for a connection."""
    from datetime import datetime
    connections = load_connections()
    
    for conn in connections:
        if conn.name == name:
            conn.last_connected_at = datetime.now().isoformat()
            break
    
    save_connections(connections)


class Database:
    """Manages PostgreSQL connection and queries."""
    
    def __init__(self):
        self.conn: Optional[psycopg.Connection] = None
        self.info: Optional[ConnectionInfo] = None
    
    def connect(self, info: ConnectionInfo) -> None:
        """Connect to database."""
        self.disconnect()
        self.conn = psycopg.connect(
            info.connection_string(),
            autocommit=False,
            row_factory=dict_row
        )
        self.info = info
    
    def disconnect(self) -> None:
        """Close connection if open."""
        if self.conn:
            try:
                self.conn.close()
            except Exception:
                pass
            self.conn = None
            self.info = None
    
    def is_connected(self) -> bool:
        return self.conn is not None and not self.conn.closed
    
    def commit(self) -> None:
        """Commit current transaction."""
        if self.conn:
            self.conn.commit()
    
    def rollback(self) -> None:
        """Rollback current transaction."""
        if self.conn:
            self.conn.rollback()
    
    def get_databases(self) -> list[str]:
        """Get list of databases on the server."""
        if not self.conn:
            return []
        with self.conn.cursor() as cur:
            cur.execute("""
                SELECT datname FROM pg_database
                WHERE datistemplate = false
                ORDER BY datname
            """)
            return [row["datname"] for row in cur.fetchall()]
    
    def switch_database(self, dbname: str) -> None:
        """Switch to a different database on the same server."""
        if not self.info:
            return
        new_info = ConnectionInfo(
            name=self.info.name,
            host=self.info.host,
            port=self.info.port,
            dbname=dbname,
            user=self.info.user,
            password=self.info.password
        )
        self.connect(new_info)
    
    def get_schemas(self) -> list[str]:
        """Get list of schemas."""
        if not self.conn:
            return []
        with self.conn.cursor() as cur:
            cur.execute("""
                SELECT schema_name FROM information_schema.schemata
                WHERE schema_name NOT IN ('pg_catalog', 'information_schema', 'pg_toast')
                ORDER BY schema_name
            """)
            return [row["schema_name"] for row in cur.fetchall()]
    
    def get_tables(self, schema: str) -> list[str]:
        """Get list of tables in schema."""
        if not self.conn:
            return []
        with self.conn.cursor() as cur:
            cur.execute("""
                SELECT table_name FROM information_schema.tables
                WHERE table_schema = %s AND table_type = 'BASE TABLE'
                ORDER BY table_name
            """, (schema,))
            return [row["table_name"] for row in cur.fetchall()]
    
    def get_all_tables(self) -> list[tuple[str, str]]:
        """Get all tables in current database as (schema, table) pairs."""
        if not self.conn:
            return []
        with self.conn.cursor() as cur:
            cur.execute("""
                SELECT table_schema, table_name 
                FROM information_schema.tables
                WHERE table_schema NOT IN ('pg_catalog', 'information_schema', 'pg_toast')
                  AND table_type = 'BASE TABLE'
                ORDER BY table_schema, table_name
            """)
            return [(row["table_schema"], row["table_name"]) for row in cur.fetchall()]
    
    def get_columns(self, schema: str, table: str) -> list[str]:
        """Get column names for a table."""
        if not self.conn:
            return []
        with self.conn.cursor() as cur:
            cur.execute("""
                SELECT column_name FROM information_schema.columns
                WHERE table_schema = %s AND table_name = %s
                ORDER BY ordinal_position
            """, (schema, table))
            return [row["column_name"] for row in cur.fetchall()]
    
    def get_all_columns(self) -> list[str]:
        """Get all unique column names in current database."""
        if not self.conn:
            return []
        with self.conn.cursor() as cur:
            cur.execute("""
                SELECT DISTINCT column_name 
                FROM information_schema.columns
                WHERE table_schema NOT IN ('pg_catalog', 'information_schema', 'pg_toast')
                ORDER BY column_name
            """)
            return [row["column_name"] for row in cur.fetchall()]

    def get_primary_keys(self, schema: str, table: str) -> list[str]:
        """Get primary key columns for a table."""
        if not self.conn:
            return []
        with self.conn.cursor() as cur:
            cur.execute("""
                SELECT a.attname
                FROM pg_index i
                JOIN pg_attribute a ON a.attrelid = i.indrelid AND a.attnum = ANY(i.indkey)
                JOIN pg_class c ON c.oid = i.indrelid
                JOIN pg_namespace n ON n.oid = c.relnamespace
                WHERE i.indisprimary AND c.relname = %s AND n.nspname = %s
                ORDER BY array_position(i.indkey, a.attnum)
            """, (table, schema))
            return [row["attname"] for row in cur.fetchall()]
    
    def execute_query(self, query: str) -> tuple[list[dict], list[str], list[int], Optional[str], int]:
        """
        Execute SQL query.
        Returns (rows, columns, column_types, error_message, rowcount).
        Auto-appends LIMIT 1000 to SELECT queries without LIMIT.
        """
        if not self.conn:
            return [], [], [], "Not connected", 0
        
        # Auto-append LIMIT 1000 for SELECT without LIMIT
        query_stripped = query.strip().rstrip(";")
        query_upper = query_stripped.upper()
        
        is_select = query_upper.startswith("SELECT")
        
        if is_select and "LIMIT" not in query_upper:
            query_stripped += " LIMIT 1000"
        
        try:
            if is_select:
                # Use server-side cursor for large SELECT results
                with self.conn.cursor(name="pgcustom_cursor") as cur:
                    cur.execute(query_stripped)
                    if cur.description:
                        columns = [desc.name for desc in cur.description]
                        # Get OID type codes for each column
                        column_types = [desc.type_code for desc in cur.description]
                    else:
                        columns = []
                        column_types = []
                    rows = cur.fetchall()
                    return rows, columns, column_types, None, len(rows)
            else:
                # Use regular cursor for DML (UPDATE/INSERT/DELETE)
                with self.conn.cursor() as cur:
                    cur.execute(query_stripped)
                    rowcount = cur.rowcount
                    return [], [], [], None, rowcount
                    
        except psycopg.Error as e:
            self.conn.rollback()
            return [], [], [], str(e), 0
    
    def execute_update(self, schema: str, table: str, pk_columns: list[str],
                       pk_values: list[Any], column: str, new_value: Any) -> Optional[str]:
        """
        Execute UPDATE for a single cell edit.
        Returns error message or None on success.
        """
        if not self.conn:
            return "Not connected"
        
        if not pk_columns:
            return "No primary key - cannot update"
        
        try:
            # Build WHERE clause from primary keys
            where_parts = [
                sql.SQL("{} = {}").format(sql.Identifier(pk), sql.Placeholder())
                for pk in pk_columns
            ]
            where_clause = sql.SQL(" AND ").join(where_parts)
            
            query = sql.SQL("UPDATE {}.{} SET {} = {} WHERE {}").format(
                sql.Identifier(schema),
                sql.Identifier(table),
                sql.Identifier(column),
                sql.Placeholder(),
                where_clause
            )
            
            with self.conn.cursor() as cur:
                cur.execute(query, [new_value] + pk_values)
            
            return None
            
        except psycopg.Error as e:
            self.conn.rollback()
            return str(e)

