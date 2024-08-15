import sys
import inspect
import os
import utils
import sqlite3
from datetime import datetime
import random
import aiohttp

folder = os.path.dirname(os.path.abspath(inspect.getfile(inspect.currentframe())))
root = folder[0 : (len(folder) - len("examples"))]
sys.path.insert(0, root)
 
from utils.api_requests import get_user_actigraphy_data
from utils.actigraphy_utils import create_plots
from utils.database_utils import insert_or_update_user_last_place, get_user_data, insert_measurement_log


def assemble_data(connection: sqlite3.Connection, measurement_log_table_name: str, user_id: int, data: list):
    measurement_name = 'lux_melanopic'
    for log in data:
        insert_measurement_log(connection, measurement_log_table_name, measurement_name, user_id, log["log_time"], log[measurement_name])


async def run_example(session: aiohttp.ClientSession, connection: sqlite3.Connection, user_id: int, user_table_name: str, measurement_log_table_name: str):
    start_time = datetime.now()
    limit = 480

    latest_user_data = get_user_data(connection, user_table_name, user_id)

    if latest_user_data is not None:
        latest_block = latest_user_data[0]
        latest_data_size = latest_user_data[1]
    else:
        latest_block = None
        latest_data_size = 0

    response = await get_user_actigraphy_data(session, user_id, limit, latest_block)
    
    data = response["data"]

    next_block_name = response["starting_after"]

    if len(data) > latest_data_size:
        starting_index = latest_data_size - len(data)
        assemble_data(connection, measurement_log_table_name, user_id, data[starting_index:])
        latest_data_size = len(data)

        while next_block_name is not None and len(data) > 0:
            print(next_block_name)
            print(len(data))

            response = get_user_actigraphy_data(user_id, limit, next_block_name)
            data = response["data"]
            if len(data) == 0:
                break

            latest_block = next_block_name
            latest_data_size = len(data)

            next_block_name = response["starting_after"]

            assemble_data(connection, measurement_log_table_name, user_id, data)

    end_time = datetime.now()
    elapsed_time = end_time - start_time

    print(f"api elapsed time: {elapsed_time}")

    start_time = datetime.now()
    insert_or_update_user_last_place(connection, user_table_name, user_id, latest_block, latest_data_size)

    end_time = datetime.now()
    elapsed_time = end_time - start_time
    print(f"db elapsed time: {elapsed_time}")
