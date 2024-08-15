import os
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
    print(f"headers: {headers}")
    return headers


def get_user_by_id(user_id: int):
    return get(f"{URL}/v1/users/{user_id}", headers=get_headers()).json()


def get_user_actigraphy_data(user_id: int, limit: int = 10, starting_after: str = None):
    return get(
        f"{URL}/v1/users/{user_id}/actigraphy_data",
        headers=get_headers(),
        params={
            "limit": limit,
            "starting_after": starting_after,
        },
    ).json()


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