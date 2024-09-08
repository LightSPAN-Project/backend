import sqlite3
import datetime
from typing import Optional, Tuple
from firebase_admin import firestore


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


async def get_user_data_firestore(db: firestore.AsyncClient, collection_name: str, user_id: int) -> Optional[Tuple[Optional[str], Optional[int]]]:
    """
    Retrieve 'starting_after' and 'data_size' values for a given user_id from Firestore.
    
    Args:
        db (firestore.Client): The Firestore client.
        collection_name (str): The name of the Firestore collection.
        user_id (int): The user's ID.
    
    Returns:
        Optional[Tuple[Optional[str], Optional[int]]]: A tuple containing 'starting_after' and 'data_size', or None if not found.
    """
    # Reference to the user's document within the collection
    user_doc_ref = db.collection(collection_name).document(str(user_id))
    
    # Get the document
    doc = await user_doc_ref.get()
    
    if doc.exists:
        # Extract 'starting_after' and 'data_size' from the document
        data = doc.to_dict()
        starting_after = data.get('starting_after')
        data_size = data.get('data_size')
        return starting_after, data_size
    else:
        return None
    

async def insert_or_update_user_last_place_firestore(db: firestore.AsyncClient, collection_name: str, user_id: int, starting_after: str, data_size: int) -> None:
    """
    Insert a new user or update the existing user's 'starting_after' and 'data_size' in Firestore.
    
    Args:
        db (firestore.Client): The Firestore client.
        collection_name (str): The name of the Firestore collection.
        user_id (int): The user's ID.
        starting_after (str): The value of 'starting_after' to be inserted or updated.
        data_size (int): The value of 'data_size' to be inserted or updated.
    """
    # Reference to the user's document within the collection
    user_doc_ref = db.collection(collection_name).document(str(user_id))
    
    # Check if the document exists
    doc = await user_doc_ref.get()
    
    if doc.exists:
        # Update the existing user's 'starting_after' and 'data_size'
        await user_doc_ref.update({
            'starting_after': starting_after,
            'data_size': data_size
        })
    else:
        # Insert a new document for the user
        await user_doc_ref.set({
            'starting_after': starting_after,
            'data_size': data_size
        })


async def insert_measurement_log_firestore(db: firestore.AsyncClient, measurements: dict, user_id: int, timestamp: datetime) -> None:
    """
    Insert a new measurement log entry for a user into Firestore, organized under 'user_id' documents in 'actigraphy_data' collection.

    Args:
        db (firestore.Client): The Firestore client.
        measurements (dict): A dictionary of measurement names and values.
        user_id (int): The user's ID.
        timestamp (datetime): The timestamp of the log entry.
    """

    # Check if timestamp is a string, convert it to datetime if necessary
    if isinstance(timestamp, str):
        # Parse the timestamp string into a datetime object
        timestamp = datetime.datetime.fromisoformat(timestamp)

    # Format the timestamp as a string to use as a document ID (Firestore doesn't support datetime directly as IDs)
    timestamp_mil = int(timestamp.timestamp() * 1000)

    # Structure: actigraphy_data -> user_id -> measurements -> timestamp
    user_doc_ref = db.collection('actigraphy_data').document(str(user_id))
    measurement_doc_ref = user_doc_ref.collection('measurements').document(str(timestamp_mil))

    # Create the document data
    log_data = {
        'timestamp': timestamp_mil,
        **measurements  # Unpack the measurements dictionary into the log data
    }

    # Add or update the document with the timestamp under the user's measurements collection
    await measurement_doc_ref.set(log_data)

    # Add or update the 'userId' field in the user's document
    await user_doc_ref.set({'userId': user_id}, merge=True)  # merge=True to avoid overwriting existing fields