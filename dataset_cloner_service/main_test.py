import base64
import json
import asyncio
import aiohttp
import requests
import time
from config import LUZMO_API_KEY, LUZMO_TOKEN, luzmo_endpoints

MAX_RETRIES = 5  # Maximum number of retries
INITIAL_RETRY_DELAY = 2  # Initial delay for retries in seconds
MAX_EXECUTION_TIME = 540  # Cloud function execution limit in seconds

start_time = time.time()

def getDatasetJSON(dataset_id):
    dataset_url = luzmo_endpoints["securable_url"]

    get_dataset_json_payload = {
        "action": "get",
        "version": "0.1.0",
        "find": {"where": {"type": "dataset"}, "include": [{"model": "Column"}]},
    }

    headers = {
        "Content-Type": "application/json",
    }

    get_dataset_json_payload["key"] = LUZMO_API_KEY
    get_dataset_json_payload["token"] = LUZMO_TOKEN
    get_dataset_json_payload["find"]["where"]["id"] = dataset_id

    retry_delay = INITIAL_RETRY_DELAY

    for attempt in range(MAX_RETRIES):
        if (time.time() - start_time) >= MAX_EXECUTION_TIME:
            print("Execution time limit exceeded, stopping retries.")
            break

        try:
            response = requests.post(dataset_url, json=get_dataset_json_payload, headers=headers)
            response.raise_for_status()  # Raise an exception for bad status codes

            if 200 <= response.status_code < 300:
                dataset_response_json = response.json()
                print("API call successful, Got Dataset JSON for :", dataset_id)
                result_dict = {}
                heirarchy_dict = {}

                for item in dataset_response_json["rows"][0]["columns"]:
                    name_en = item.get("name", {}).get("en", None)
                    id_value = item.get("id", None)
                    if item.get("type") == "hierarchy":
                        heirarchy_dict[id_value] = name_en
                    result_dict[id_value] = name_en

                return result_dict, heirarchy_dict
            else:
                print(f"getDatasetJSON API call failed for dataset_id: {dataset_id} with status_code: {response.status_code}")

        except requests.exceptions.RequestException as e:
            print(f"Dataset Error making request: {e}")
            if response.status_code == 429:  # Too Many Requests
                print(f"Rate limit exceeded, retrying after {retry_delay} seconds...")
                time.sleep(retry_delay)
                retry_delay = min(retry_delay * 2, MAX_EXECUTION_TIME - (time.time() - start_time))
            else:
                return False
    return False

async def fetch_column_data(session, column_id, column_name):
    dataset_url = luzmo_endpoints["column_url"]

    get_column_json_payload = {
        "action": "get",
        "version": "0.1.0",
        "find": {"where": {"id": column_id}},
        "key": LUZMO_API_KEY,
        "token": LUZMO_TOKEN,
    }

    headers = {
        "Content-Type": "application/json",
    }

    retry_delay = INITIAL_RETRY_DELAY

    for attempt in range(MAX_RETRIES):
        if (time.time() - start_time) >= MAX_EXECUTION_TIME:
            print("Execution time limit exceeded, stopping retries.")
            break

        try:
            async with session.post(dataset_url, json=get_column_json_payload, headers=headers) as response:
                if response.status == 429:  # Too Many Requests
                    print(f"Rate limit exceeded, retrying after {retry_delay} seconds...")
                    await asyncio.sleep(retry_delay)
                    retry_delay = min(retry_delay * 2, MAX_EXECUTION_TIME - (time.time() - start_time))
                    continue

                try:
                    response.raise_for_status()  # Raise an exception for HTTP error responses
                    if 200 <= response.status < 300:
                        json_response = await response.json()
                        return column_id, column_name, json_response
                    else:
                        print(f"fetch_column_data API call failed for column_id: {column_id}, column_name: {column_name} with status_code: {response.status}")
                        break
                except aiohttp.ClientResponseError as e:
                    error_details = await response.text()  # Capture the error details from the response
                    print(f"fetch_column_data API call failed with status: {response.status}, message: {e.message}, error details: {error_details}")
                    break  # No point in retrying for client errors like 400 Bad Request
        except aiohttp.ClientConnectionError as e:
            print(f"Connection error occurred for column_id: {column_id}, column_name: {column_name} on attempt {attempt + 1}/{MAX_RETRIES}: {str(e)}")
            if attempt < MAX_RETRIES - 1:
                await asyncio.sleep(retry_delay)
                retry_delay = min(retry_delay * 2, MAX_EXECUTION_TIME - (time.time() - start_time))
            else:
                print(f"Failed to fetch column data for column_id: {column_id}, column_name: {column_name} after {MAX_RETRIES} attempts.")
        except Exception as e:
            print(f"An unexpected error occurred for column_id: {column_id}, column_name: {column_name} with exception: {str(e)}")
            break

    return None

async def update_column_data(session, column_id, column_name, column_data):
    update_column_json_payload = {
        "action": "update",
        "version": "0.1.0",
        "key": LUZMO_API_KEY,
        "token": LUZMO_TOKEN,
        "id": column_id,
        "properties": column_data,
    }

    excluded_fields = [
        "id", "name", "source_name", "minBound", "maxBound", "cardinality", "highestLevel", 
        "minimum", "maximum", "version", "created_at", "updated_at", "securable_id",
    ]

    for field in excluded_fields:
        update_column_json_payload["properties"].pop(field, None)

    update_url = luzmo_endpoints["column_url"]

    headers = {
        "Content-Type": "application/json",
    }

    retry_delay = INITIAL_RETRY_DELAY

    for attempt in range(MAX_RETRIES):
        if (time.time() - start_time) >= MAX_EXECUTION_TIME:
            print("Execution time limit exceeded, stopping retries.")
            break

        try:
            async with session.post(update_url, json=update_column_json_payload, headers=headers) as response:
                if response.status == 429:  # Too Many Requests
                    print(f"Rate limit exceeded, retrying after {retry_delay} seconds...")
                    await asyncio.sleep(retry_delay)
                    retry_delay = min(retry_delay * 2, MAX_EXECUTION_TIME - (time.time() - start_time))
                    continue

                try:
                    response.raise_for_status()  # Raise an exception for HTTP error responses
                    if 200 <= response.status < 300:
                        return await response.json()
                    else:
                        print(f"update_column_data API call failed for column_id: {column_id}, column_name: {column_name} with status_code: {response.status}")
                        break
                except aiohttp.ClientResponseError as e:
                    error_details = await response.text()  # Capture the error details from the response
                    print(f"update_column_data API call failed with status: {response.status}, message: {e.message}, error details: {error_details}")
                    break  # No point in retrying for client errors like 400 Bad Request
        except aiohttp.ClientConnectionError as e:
            print(f"Connection error occurred for column_id: {column_id}, column_name: {column_name} on attempt {attempt + 1}/{MAX_RETRIES}: {str(e)}")
            if attempt < MAX_RETRIES - 1:
                await asyncio.sleep(retry_delay)
                retry_delay = min(retry_delay * 2, MAX_EXECUTION_TIME - (time.time() - start_time))
            else:
                print(f"Failed to update column data for column_id: {column_id}, column_name: {column_name} after {MAX_RETRIES} attempts.")
        except Exception as e:
            print(f"An unexpected error occurred for column_id: {column_id}, column_name: {column_name} with exception: {str(e)}")
            break

    return None

async def fetch_hierarchy_data(session, column_id, dataset_id):
    hierarchy_url = luzmo_endpoints["hierarchy_url"]

    get_hierarchy_json_payload = {
        "action": "get",
        "version": "0.1.0",
        "find": {
            "where": {
                "column_id": column_id,
                "securable_id": dataset_id,
            }
        },
        "key": LUZMO_API_KEY,
        "token": LUZMO_TOKEN,
    }

    headers = {
        "Content-Type": "application/json",
    }

    retry_delay = INITIAL_RETRY_DELAY

    for attempt in range(MAX_RETRIES):
        if (time.time() - start_time) >= MAX_EXECUTION_TIME:
            print("Execution time limit exceeded, stopping retries.")
            break

        try:
            async with session.post(hierarchy_url, json=get_hierarchy_json_payload, headers=headers) as response:
                if response.status == 429:  # Too Many Requests
                    print(f"Rate limit exceeded, retrying after {retry_delay} seconds...")
                    await asyncio.sleep(retry_delay)
                    retry_delay = min(retry_delay * 2, MAX_EXECUTION_TIME - (time.time() - start_time))
                    continue

                try:
                    response.raise_for_status()  # Raise an exception for HTTP error responses
                    if 200 <= response.status < 300:
                        return await response.json()
                    else:
                        print(f"fetch_hierarchy_data API call failed for column_id: {column_id}, dataset_id: {dataset_id} with status_code: {response.status}")
                        break
                except aiohttp.ClientResponseError as e:
                    error_details = await response.text()  # Capture the error details from the response
                    print(f"fetch_hierarchy_data API call failed with status: {response.status}, message: {e.message}, error details: {error_details}")
                    break  # No point in retrying for client errors like 400 Bad Request
        except aiohttp.ClientConnectionError as e:
            print(f"Connection error occurred for column_id: {column_id}, dataset_id: {dataset_id} on attempt {attempt + 1}/{MAX_RETRIES}: {str(e)}")
            if attempt < MAX_RETRIES - 1:
                await asyncio.sleep(retry_delay)
                retry_delay = min(retry_delay * 2, MAX_EXECUTION_TIME - (time.time() - start_time))
            else:
                print(f"Failed to fetch hierarchy data for column_id: {column_id}, dataset_id: {dataset_id} after {MAX_RETRIES} attempts.")
        except Exception as e:
            print(f"An unexpected error occurred for column_id: {column_id}, dataset_id: {dataset_id} with exception: {str(e)}")
            break

    return None
