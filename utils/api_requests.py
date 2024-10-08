import os
import aiohttp
import asyncio
from typing import List
from requests import get, post

CREDENTIALS_FILE = 'credentials.txt'

URL = "https://condorcloudapi.condorapps.net"

def read_api_credentials(credentials):
    file_path = os.path.join(os.path.dirname(__file__), '..', credentials)
    with open(file_path, 'r') as file:
        api_key = file.readline().strip()
        token = file.readline().strip()
    return api_key, token

def get_headers():
    api_key, token = read_api_credentials(CREDENTIALS_FILE)
    headers = {"x-api-key": api_key, "Authorization": f"Bearer {token}"}
    return headers


def get_user_by_id(user_id: int):
    return get(f"{URL}/v1/users/{user_id}", headers=get_headers()).json()


async def get_user_actigraphy_data(session: aiohttp.ClientSession, user_id: int, limit: int = 10, starting_after: str = None):
    parameters = {
            "limit": limit,
            "starting_after": starting_after,
        }
    async with session.get(
        f"{URL}/v1/users/{user_id}/actigraphy_data",
        headers=get_headers(),
        params={k: v for k, v in parameters.items() if v is not None},
    ) as response:
        return await response.json()


def create_patient(payload: dict) -> dict:
    return post(f"{URL}/v1/users", headers=get_headers(), json=payload).json()


def associate_devices(user_id: int, devices: List[dict]) -> dict:
    return post(
        f"{URL}/v1/users/{user_id}/associate_devices",
        headers=get_headers(),
        json={"devices": devices},
    ).json()


def disassociate_devices(user_id: int, devices_ids: list[int]) -> dict:
    return post(
        f"{URL}/v1/users/{user_id}/disassociate_devices",
        headers=get_headers(),
        json={"devices_ids": devices_ids},
    ).json()