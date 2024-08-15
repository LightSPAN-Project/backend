import sqlite3
import datetime
from typing import Optional, Tuple

def connect_to_db(db_name: str) -> sqlite3.Connection:
    """Connect to the SQLite database and return the connection object."""
    return sqlite3.connect(db_name)


def create_table_with_timestamp(connection: sqlite3.Connection, base_name: str) -> str:
    """Create a table with a custom name that includes a timestamp."""
    timestamp = datetime.datetime.now().strftime('%Y%m%d_%H%M%S')
    table_name = f"{base_name}_{timestamp}"
    cursor = connection.cursor()
    cursor.execute(f'''
    CREATE TABLE IF NOT EXISTS {table_name} (
        row_id INTEGER PRIMARY KEY AUTOINCREMENT,
        user_id INTEGER NOT NULL,
        starting_after TEXT NOT NULL,
        data_size INTEGER NOT NULL
    )
    ''')
    connection.commit()
    return table_name

def create_table(connection: sqlite3.Connection, table_name: str) -> str:
    """Create a table with a custom name."""
    cursor = connection.cursor()
    cursor.execute(f'''
    CREATE TABLE IF NOT EXISTS {table_name} (
        row_id INTEGER PRIMARY KEY AUTOINCREMENT,
        user_id INTEGER NOT NULL,
        starting_after TEXT,
        data_size INTEGER NOT NULL
    )
    ''')
    connection.commit()
    return table_name


def insert_last_place_data(connection: sqlite3.Connection, table_name: str, user_id: int, starting_after: str, data_size: int) -> None:
    """Insert data into the table."""
    cursor = connection.cursor()
    cursor.execute(f'''
    INSERT INTO {table_name} (user_id, starting_after, data_size) VALUES (?, ?, ?)
    ''', (user_id, starting_after, data_size))
    connection.commit()


def insert_or_update_user_last_place(connection: sqlite3.Connection, table_name: str, user_id: int, starting_after: str, data_size: int) -> None:
    """Insert a new user or update the existing user's starting_after and data_size."""
    cursor = connection.cursor()

    # Check if the user_id exists
    cursor.execute(f"SELECT 1 FROM {table_name} WHERE user_id = ?", (user_id,))
    exists = cursor.fetchone()

    if exists:
        # Update the existing user's starting_after and data_size
        cursor.execute(f'''
        UPDATE {table_name}
        SET starting_after = ?, data_size = ?
        WHERE user_id = ?
        ''', (starting_after, data_size, user_id))
    else:
        # Insert a new user with the provided user_id, starting_after, and data_size
        cursor.execute(f'''
        INSERT INTO {table_name} (user_id, starting_after, data_size)
        VALUES (?, ?, ?)
        ''', (user_id, starting_after, data_size))
    
    connection.commit()

def get_user_data(connection: sqlite3.Connection, table_name: str, user_id: int) -> Optional[Tuple[Optional[str], Optional[int]]]:
    """Retrieve 'starting_after' and 'data_size' values for a given user_id."""
    cursor = connection.cursor()

    # Execute query to get 'starting_after' and 'data_size' for the given user_id
    cursor.execute(f'''
    SELECT starting_after, data_size
    FROM {table_name}
    WHERE user_id = ?
    ''', (user_id,))
    
    result = cursor.fetchone()
    
    if result:
        return result
    else:
        return None
    
def create_measurement_logs_table(connection: sqlite3.Connection, logs_table_name: str, users_table_name: str) -> None:
    """Create a table for measurement logs associated with users."""
    cursor = connection.cursor()
    
    # Create the measurement logs table with a foreign key reference to the users table
    cursor.execute(f'''
    CREATE TABLE IF NOT EXISTS {logs_table_name} (
        log_id INTEGER PRIMARY KEY,
        user_id INTEGER,
        timestamp TIMESTAMP,
        FOREIGN KEY (user_id) REFERENCES {users_table_name} (user_id)
    )
    ''')
    
    connection.commit()
    return logs_table_name

def add_measurement_column(connection: sqlite3.Connection, logs_table_name: str, column_name: str) -> None:
    """Dynamically add a new measurement column to the logs table if it does not exist."""
    cursor = connection.cursor()
    cursor.execute(f"PRAGMA table_info({logs_table_name})")
    columns = [info[1] for info in cursor.fetchall()]
    if column_name not in columns:
        cursor.execute(f"ALTER TABLE {logs_table_name} ADD COLUMN {column_name} REAL")
        connection.commit()

def insert_measurement_log(connection: sqlite3.Connection, logs_table_name: str,  measurements: dict, user_id: int, timestamp: datetime) -> None:
    """Insert a new measurement log entry for a user."""
    for measurement_name, measurement_value in measurements.items():
        add_measurement_column(connection, logs_table_name, measurement_name)

    cursor = connection.cursor()

    # Prepare the columns and their corresponding values
    columns = ", ".join(measurements.keys())
    placeholders = ", ".join(["?"] * len(measurements))
    values = list(measurements.values())
    
    cursor.execute(f'''
    INSERT INTO {logs_table_name} (user_id, timestamp, {columns})
    VALUES (?, ?, {placeholders})
    ''', (user_id, timestamp, *values))
    
    connection.commit()

def close_connection(connection: sqlite3.Connection) -> None:
    """Close the connection to the database."""
    connection.close()