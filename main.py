# main.py
import os
from get_patient_actigraphy_data import run_example
from utils import database_utils
from server import run
from datetime import datetime
import aiohttp
import asyncio
import firebase_admin
from firebase_admin import firestore, credentials

BACKEND_DIR_NAME = 'backend_data'
DATABASE_NAME = 'backend.db'
main_directory = os.path.dirname(os.path.abspath(__file__))
backend_dir = os.path.join(main_directory, BACKEND_DIR_NAME)

credentials_path = 'lightspan-513da-ceeca0168093.json'
os.environ['GOOGLE_APPLICATION_CREDENTIALS'] = credentials_path

DB_PATH = os.path.join(backend_dir, DATABASE_NAME)

def get_main_directory() -> str:
    """Get the directory where the main.py file is located."""
    return os.path.dirname(os.path.abspath(__file__))

def create_directory(directory_name: str) -> str:
    """Create a new directory at the same level as the main.py file."""
    main_directory = get_main_directory()
    new_directory_path = os.path.join(main_directory, directory_name)
    if not os.path.exists(new_directory_path):
        os.makedirs(new_directory_path)
    return new_directory_path


def create_backend_directory():
    new_dir_path = create_directory(BACKEND_DIR_NAME)
    print(f"Directory created at: {new_dir_path}")

def get_db_path(db_name: str, directory_name: str) -> str:
    """Get the full path for the database file in the specified directory."""
    new_directory_path = create_directory(directory_name)
    return os.path.join(new_directory_path, db_name)

def create_backend_db():
    db_path = get_db_path(DATABASE_NAME, BACKEND_DIR_NAME)
    connection = database_utils.connect_to_db(db_path)


async def main():
    print("main")
    #run()

    create_backend_directory()
    create_backend_db()

    cred = credentials.Certificate('lightspan-513da-ceeca0168093.json')

    app = firebase_admin.initialize_app(cred)
    db = firestore.AsyncClient()

    base_name = 'last_place_table'
    connection = database_utils.connect_to_db(DB_PATH)
    last_place_table = database_utils.create_table(connection, base_name)
    measurement_logs_table = database_utils.create_measurement_logs_table(connection, 'sensor_logs', last_place_table)
    print(f"Table created: {last_place_table}")

    start_time = datetime.now()
    print(start_time)

    async with aiohttp.ClientSession() as session:
        tasks = []

        task1 = asyncio.create_task(run_example(db, session, connection, 1723, last_place_table, measurement_logs_table))
        task2 = asyncio.create_task(run_example(db, session, connection, 1724, last_place_table, measurement_logs_table))
        tasks.append(task1)
        tasks.append(task2)
        # for i in range(1720, 1870):
        #     task = asyncio.create_task(run_example(db, session, connection, i, last_place_table, measurement_logs_table))
        #     tasks.append(task)

        # for i in range(1887, 2136):
        #     task = asyncio.create_task(run_example(db, session, connection, i, last_place_table, measurement_logs_table))
        #     tasks.append(task)

        await asyncio.gather(*tasks)

    end_time = datetime.now()

    elapsed_time = end_time - start_time
    print(end_time)
    print(f"total elapsed_time {elapsed_time}")

    #run_example(connection, 1638, last_place_table, measurement_logs_table)
    #run_example(connection, 1637, last_place_table, measurement_logs_table)
    
if __name__ == "__main__":
    asyncio.run(main())

