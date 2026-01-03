import base64
import json
import asyncio
import aiohttp
import requests
import time
from itertools import cycle

# import nest_asyncio
from config import LUZMO_KnT_CREDENTIALS, luzmo_endpoints

luzmo_cred_cycle = cycle(LUZMO_KnT_CREDENTIALS)

LUZMO_KnT_call_count = 0
KEYnTOKEN_SWAP_LIMIT = 100
MAX_RETRIES = 5  # Maximum number of retries
INITIAL_RETRY_DELAY = 2  # Initial delay for retries in seconds
MAX_EXECUTION_TIME = 540  # Cloud function execution limit in seconds
start_time = time.time()

# nest_asyncio.apply()


def get_api_credentials():
    global LUZMO_KnT_call_count
    LUZMO_KnT_call_count += 1
    if LUZMO_KnT_call_count % KEYnTOKEN_SWAP_LIMIT == 0:
        return next(luzmo_cred_cycle)
    else:
        return LUZMO_KnT_CREDENTIALS[LUZMO_KnT_call_count % len(LUZMO_KnT_CREDENTIALS)]


async def getDatasetJSON(session, dataset_id):
    dataset_url = luzmo_endpoints["securable_url"]

    get_dataset_json_payload = {
        "action": "get",
        "version": "0.1.0",
        "find": {"where": {"type": "dataset"}, "include": [{"model": "Column"}]},
    }

    headers = {
        "Content-Type": "application/json",
    }

    LUZMO_API_KEY, LUZMO_TOKEN = get_api_credentials()

    get_dataset_json_payload["key"] = LUZMO_API_KEY
    get_dataset_json_payload["token"] = LUZMO_TOKEN
    get_dataset_json_payload["find"]["where"]["id"] = dataset_id

    retry_delay = INITIAL_RETRY_DELAY
    start_time = time.time()

    for attempt in range(MAX_RETRIES):
        if (time.time() - start_time) >= MAX_EXECUTION_TIME:
            print("Execution time limit exceeded, stopping retries.")
            break

        try:
            async with session.post(
                dataset_url, json=get_dataset_json_payload, headers=headers
            ) as response:
                if 200 <= response.status < 300:
                    dataset_response_json = await response.json()
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
                    print(
                        f"getDatasetJSON API call failed for dataset_id: {dataset_id} with status_code: {response.status}"
                    )

        except aiohttp.ClientError as e:
            print(f"Dataset Error making request: {e}")
            if response.status in [429, 504]:  # Too Many Requests
                print(
                    f"Rate limit exceeded, retrying after {retry_delay} seconds... Status : {response.status}"
                )
                await asyncio.sleep(retry_delay)
                retry_delay = min(
                    retry_delay * 2, MAX_EXECUTION_TIME - (time.time() - start_time)
                )
            else:
                return False
    return False


async def fetch_column_data(session, column_id, column_name):
    dataset_url = luzmo_endpoints["column_url"]

    LUZMO_API_KEY, LUZMO_TOKEN = get_api_credentials()

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
        try:
            async with session.post(
                dataset_url, json=get_column_json_payload, headers=headers
            ) as response:
                if response.status in [429, 504]:  # Too Many Requests
                    print(
                        f"Rate limit exceeded, retrying after {retry_delay} seconds... Status : {response.status}"
                    )
                    await asyncio.sleep(retry_delay)
                    retry_delay = min(
                        retry_delay * 2, MAX_EXECUTION_TIME - (time.time() - start_time)
                    )
                    if (time.time() - start_time) + retry_delay >= MAX_EXECUTION_TIME:
                        print("Execution time limit exceeded, stopping retries.")
                        break
                    continue
                try:
                    response.raise_for_status()
                    json_response = await response.json()
                    return column_id, column_name, json_response
                except aiohttp.ClientResponseError as e:
                    error_details = (
                        await response.text()
                    )  # Capture the error details from the response
                    print(
                        f"fetch_column_data API call failed for session_id: {session}, "
                        f"column_id: {column_id}, column_name: {column_name} "
                        f"with status: {response.status}, message: {e.message}, url: {e.request_info.url}, "
                        f"error details: {error_details}"
                    )
                    break  # No point in retrying for client errors like 400 Bad Request
        except aiohttp.ClientConnectionError as e:
            print(
                f"Connection error occurred for session_id: {session}, column_id: {column_id}, "
                f"column_name: {column_name} on attempt {attempt + 1}/{MAX_RETRIES}: {str(e)}"
            )
            if attempt < MAX_RETRIES - 1:
                await asyncio.sleep(retry_delay)
                retry_delay = min(
                    retry_delay * 2, MAX_EXECUTION_TIME - (time.time() - start_time)
                )
                if (time.time() - start_time) + retry_delay >= MAX_EXECUTION_TIME:
                    print("Execution time limit exceeded, stopping retries.")
                    break
            else:
                print(
                    f"Failed to fetch column data for session_id: {session}, column_id: {column_id}, "
                    f"column_name: {column_name} after {MAX_RETRIES} attempts."
                )
        except Exception as e:
            print(
                f"An unexpected error occurred for session_id: {session}, column_id: {column_id}, "
                f"column_name: {column_name} with exception: {str(e)}"
            )
            break

    # If we reach this point, either all retries have failed, or execution time was exceeded
    return None


async def update_column_data(session, column_id, column_name, column_data):

    LUZMO_API_KEY, LUZMO_TOKEN = get_api_credentials()

    update_column_json_payload = {
        "action": "update",
        "version": "0.1.0",
        "key": LUZMO_API_KEY,
        "token": LUZMO_TOKEN,
        "id": column_id,
        "properties": column_data,
    }

    # Exclude fields which are commented out in the provided API call
    excluded_fields = [
        "id",
        "name",
        "source_name",
        "minBound",
        "maxBound",
        "cardinality",
        "highestLevel",
        "minimum",
        "maximum",
        "version",
        "created_at",
        "updated_at",
        "securable_id",
    ]

    # Remove excluded fields from the payload
    for field in excluded_fields:
        update_column_json_payload["properties"].pop(field, None)

    for field in excluded_fields:
        if field in update_column_json_payload["properties"]:
            del update_column_json_payload["properties"][field]

    update_url = luzmo_endpoints["column_url"]

    headers = {
        "Content-Type": "application/json",
    }
    # print(update_column_json_payload)
    retry_delay = INITIAL_RETRY_DELAY

    for attempt in range(MAX_RETRIES):
        try:
            async with session.post(
                update_url, json=update_column_json_payload, headers=headers
            ) as response:
                if response.status in [429, 504]:  # Too Many Requests
                    print(
                        f"Rate limit exceeded, retrying after {retry_delay} seconds... Status : {response.status}"
                    )
                    await asyncio.sleep(retry_delay)
                    retry_delay = min(
                        retry_delay * 2, MAX_EXECUTION_TIME - (time.time() - start_time)
                    )
                    if (time.time() - start_time) + retry_delay >= MAX_EXECUTION_TIME:
                        print("Execution time limit exceeded, stopping retries.")
                        break
                    continue
                try:
                    response.raise_for_status()
                    return await response.json()
                except aiohttp.ClientResponseError as e:
                    error_details = (
                        await response.text()
                    )  # Capture the error details from the response
                    print(
                        f"update_column_data API call failed for session_id: {session}, "
                        f"column_id: {column_id}, column_name: {column_name} "
                        f"with status: {response.status}, message: {e.message}, url: {e.request_info.url}, "
                        f"error details: {error_details}"
                    )
                    break  # No point in retrying for client errors like 400 Bad Request
        except aiohttp.ClientConnectionError as e:
            print(
                f"Connection error occurred for session_id: {session}, column_id: {column_id}, "
                f"column_name: {column_name} on attempt {attempt + 1}/{MAX_RETRIES}: {str(e)}"
            )
            if attempt < MAX_RETRIES - 1:
                await asyncio.sleep(retry_delay)
                retry_delay = min(
                    retry_delay * 2, MAX_EXECUTION_TIME - (time.time() - start_time)
                )
                if (time.time() - start_time) + retry_delay >= MAX_EXECUTION_TIME:
                    print("Execution time limit exceeded, stopping retries.")
                    break
            else:
                print(
                    f"Failed to update column data for session_id: {session}, column_id: {column_id}, "
                    f"column_name: {column_name} after {MAX_RETRIES} attempts."
                )
        except Exception as e:
            print(
                f"An unexpected error occurred for session_id: {session}, column_id: {column_id}, "
                f"column_name: {column_name} with exception: {str(e)}"
            )
            break

        # If we reach this point, either all retries have failed, or execution time was exceeded
        return None


async def fetch_hierarchy_data(session, column_id, dataset_id):
    hierarchy_url = luzmo_endpoints["hierarchy_url"]

    LUZMO_API_KEY, LUZMO_TOKEN = get_api_credentials()

    get_hierarchy_json_payload = {
        "action": "get",
        "version": "0.1.0",
        "find": {
            "where": {
                "column_id": column_id,
                "securable_id": dataset_id,  # Template "651407a3-7f57-446c-b514-46c2d87f473a"
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
        try:
            async with session.post(
                hierarchy_url, json=get_hierarchy_json_payload, headers=headers
            ) as response:

                if response.status in [429, 504]:  # Too Many Requests
                    print(
                        f"Rate limit exceeded, retrying after {retry_delay} seconds... Status : {response.status}"
                    )
                    await asyncio.sleep(retry_delay)
                    retry_delay = min(
                        retry_delay * 2, MAX_EXECUTION_TIME - (time.time() - start_time)
                    )
                    if (time.time() - start_time) + retry_delay >= MAX_EXECUTION_TIME:
                        print("Execution time limit exceeded, stopping retries.")
                        break
                    continue
                try:
                    response.raise_for_status()  # Raise an exception for HTTP error responses
                    json_response = await response.json()

                    return column_id, json_response
                except aiohttp.ClientResponseError as e:
                    error_details = (
                        await response.text()
                    )  # Capture the error details from the response
                    print(
                        f"fetch_hierarchy_data API call failed for session_id: {session}, "
                        f"column_id: {column_id}, dataset_data: {dataset_id} "
                        f"with status: {response.status}, message: {e.message}, url: {e.request_info.url}, "
                        f"error details: {error_details}"
                    )
                    break  # No point in retrying for client errors like 400 Bad Request

        except aiohttp.ClientConnectionError as e:
            print(
                f"Connection error occurred for session_id: {session}, column_id: {column_id}, "
                f"dataset_id: {dataset_id} on attempt {attempt + 1}/{MAX_RETRIES}: {str(e)}"
            )
            if attempt < MAX_RETRIES - 1:
                await asyncio.sleep(retry_delay)
                retry_delay = min(
                    retry_delay * 2, MAX_EXECUTION_TIME - (time.time() - start_time)
                )
                if (time.time() - start_time) + retry_delay >= MAX_EXECUTION_TIME:
                    print("Execution time limit exceeded, stopping retries.")
                    break
            else:
                print(
                    f"Failed to fetch hierarchy data for session_id: {session}, column_id: {column_id}, "
                    f"dataset_id: {dataset_id} after {MAX_RETRIES} attempts."
                )
        except Exception as e:
            print(
                f"An unexpected error occurred for session_id: {session}, column_id: {column_id}, "
                f"dataset_id: {dataset_id} with exception: {str(e)}"
            )
            break

    # If we reach this point, either all retries have failed, or execution time was exceeded
    return None


async def update_hierarchy_data(session, column_id, hierarchy_data):

    LUZMO_API_KEY, LUZMO_TOKEN = get_api_credentials()

    update_hierarchy_json_payload = {
        "action": "update",
        "version": "0.1.0",
        "key": LUZMO_API_KEY,
        "token": LUZMO_TOKEN,
        "id": column_id,
        "properties": {"updates": hierarchy_data},
    }

    update_url = luzmo_endpoints["hierarchy_url"]

    headers = {
        "Content-Type": "application/json",
    }

    retry_delay = INITIAL_RETRY_DELAY

    for attempt in range(MAX_RETRIES):
        try:
            async with session.post(
                update_url, json=update_hierarchy_json_payload, headers=headers
            ) as response:
                if response.status in [429, 504]:  # Too Many Requests
                    print(
                        f"Rate limit exceeded, retrying after {retry_delay} seconds... Status : {response.status}"
                    )
                    await asyncio.sleep(retry_delay)
                    retry_delay = min(
                        retry_delay * 2, MAX_EXECUTION_TIME - (time.time() - start_time)
                    )
                    if (time.time() - start_time) + retry_delay >= MAX_EXECUTION_TIME:
                        print("Execution time limit exceeded, stopping retries.")
                        break
                    continue

                try:
                    response.raise_for_status()  # Raise an exception for HTTP error responses
                    return await response.json()
                except aiohttp.ClientResponseError as e:
                    error_details = (
                        await response.text()
                    )  # Capture the error details from the response
                    print(
                        f"update_hierarchy_data API call failed for session_id: {session}, "
                        f"column_id: {column_id}, hierarchy_data: {hierarchy_data} "
                        f"with status: {response.status}, message: {e.message}, url: {e.request_info.url}, "
                        f"error details: {error_details}"
                    )
                    break  # No point in retrying for client errors like 400 Bad Request
        except aiohttp.ClientConnectionError as e:
            print(
                f"Connection error occurred for session_id: {session}, column_id: {column_id}, "
                f"hierarchy_data: {hierarchy_data} on attempt {attempt + 1}/{MAX_RETRIES}: {str(e)}"
                f"\n\n Payload: {json.dumps(update_hierarchy_json_payload)} \n\n"
            )
            if attempt < MAX_RETRIES - 1:
                await asyncio.sleep(retry_delay)
                retry_delay = min(
                    retry_delay * 2, MAX_EXECUTION_TIME - (time.time() - start_time)
                )
                if (time.time() - start_time) + retry_delay >= MAX_EXECUTION_TIME:
                    print("Execution time limit exceeded, stopping retries.")
                    break
            else:
                print(
                    f"Failed to update hierarchy data for session_id: {session}, column_id: {column_id}, "
                    f"hierarchy_data: {hierarchy_data} after {MAX_RETRIES} attempts."
                )
        except Exception as e:
            print(
                f"An unexpected error occurred for session_id: {session}, column_id: {column_id}, "
                f"hierarchy_data: {hierarchy_data} with exception: {str(e)}"
            )
            break

    # If we reach this point, either all retries have failed, or execution time was exceeded
    return None


async def parallelizer(session, task_info, function_to_call):
    print("Parallelizer Initiailised with Function: ", str(function_to_call))
    tasks = []
    for args in task_info:
        try:
            task = function_to_call(session, *args)
            tasks.append(task)
        except Exception as e:
            print(f"Error calling function with arguments {args}: {e}")
    # Gather the results
    results = await asyncio.gather(*tasks)
    print(f"Executed Job Count for {str(function_to_call)}:", len(results))
    return results


async def dataset_processor(session, key_dataset_id, value_dataset_id):
    print("Mapping Template from: ", key_dataset_id, "To: ", value_dataset_id)
    template_columns, temp_heirarchy_cols = await getDatasetJSON(
        session, key_dataset_id
    )
    dest_columns, dest_heirarchy_cols = await getDatasetJSON(session, value_dataset_id)

    dest_col_rev_dict = {
        value_col_nm: key_col_id for key_col_id, value_col_nm in dest_columns.items()
    }
    print("Src. Col Dict:", template_columns)
    print("Reverse Dest. Column Dict :", dest_col_rev_dict)

    template_col_resp = await parallelizer(
        session, [(k, v) for k, v in template_columns.items()], fetch_column_data
    )

    template_heirarchy_resp = await parallelizer(
        session,
        [
            (heirarchy_col_id, key_dataset_id)
            for heirarchy_col_id in temp_heirarchy_cols
        ],
        fetch_hierarchy_data,
    )

    dest_col_update_info = []
    for column_id, column_name, response in template_col_resp:
        try:
            item = (
                dest_col_rev_dict[column_name],
                column_name,
                response["rows"][0],
            )
            dest_col_update_info.append(item)
        except Exception as e:
            print(
                f"Error processing column update info for {column_id}, {column_name}: {e}"
            )

    dest_heirarchy_update_info = []
    for column_id, response in template_heirarchy_resp:
        try:
            heir_resp = [
                heir_item
                for heir_item in response[0]["children"]
                if heir_item["id"] is not None
            ]
            item = (dest_col_rev_dict[template_columns[column_id]], heir_resp)
            dest_heirarchy_update_info.append(item)
        except Exception as e:
            print(f"Error processing hierarchy update info for {column_id}: {e}")

    update_col_resp = await parallelizer(
        session, dest_col_update_info, update_column_data
    )

    update_heriarchy_resp = await parallelizer(
        session, dest_heirarchy_update_info, update_hierarchy_data
    )

    return (
        template_col_resp,
        template_heirarchy_resp,
        update_col_resp,
        update_heriarchy_resp,
    )


async def main(dataset_mapping):
    print("Main Fn : Dataset Mapping :", dataset_mapping)
    async with aiohttp.ClientSession() as session:
        results = await parallelizer(
            session,
            [
                (key_dataset_id, value_dataset_id)
                for key_dataset_id, value_dataset_id in dataset_mapping.items()
            ],
            dataset_processor,
        )
    return results


# async def parallelizer(task_info, function_to_call):
#     print("Parallelizer Initiailised with Function: ", str(function_to_call))
#     async with aiohttp.ClientSession() as session:
#         tasks = []
#         for args in task_info:
#             try:
#                 task = function_to_call(session, *args)
#                 tasks.append(task)
#             except Exception as e:
#                 print(f"Error calling function with arguments {args}: {e}")
#         # Gather the results
#         results = await asyncio.gather(*tasks)
#         print(f"Executed Job Count for {str(function_to_call)}:", len(results))
#         return results


# async def main(dataset_mapping):
#     print("Main Fn : Dataset Mapping :", dataset_mapping)
#     for key_dataset_id, value_dataset_id in dataset_mapping.items():
#         print("Mapping Template from: ", key_dataset_id, "To: ", value_dataset_id)
#         template_columns, temp_heirarchy_cols = getDatasetJSON(key_dataset_id)
#         dest_columns, dest_heirarchy_cols = getDatasetJSON(value_dataset_id)

#         dest_col_rev_dict = {
#             value_col_nm: key_col_id
#             for key_col_id, value_col_nm in dest_columns.items()
#         }
#         print("Src. Col Dict:", template_columns)
#         print("Reverse Dest. Column Dict :", dest_col_rev_dict)

#         template_col_resp = await parallelizer(
#             [(k, v) for k, v in template_columns.items()], fetch_column_data
#         )

#         template_heirarchy_resp = await parallelizer(
#             [
#                 (heirarchy_col_id, key_dataset_id)
#                 for heirarchy_col_id in temp_heirarchy_cols
#             ],
#             fetch_hierarchy_data,
#         )

#         dest_col_update_info = []
#         for column_id, column_name, response in template_col_resp:
#             try:

#                 item = (
#                     dest_col_rev_dict[column_name],
#                     column_name,
#                     response["rows"][0],
#                 )
#                 dest_col_update_info.append(item)
#             except Exception as e:
#                 # Handle exception (e.g., log it, print it, etc.)
#                 print(
#                     f"Error processing column update info for {column_id}, {column_name}: {e}"
#                 )

#         dest_heirarchy_update_info = []
#         for column_id, response in template_heirarchy_resp:
#             try:
#                 heir_resp = [
#                     heir_item
#                     for heir_item in response[0]["children"]
#                     if heir_item["id"] is not None
#                 ]
#                 item = (dest_col_rev_dict[template_columns[column_id]], heir_resp)
#                 dest_heirarchy_update_info.append(item)
#             except Exception as e:
#                 # Handle exception (e.g., log it, print it, etc.)
#                 print(f"Error processing hierarchy update info for {column_id}: {e}")

#         update_col_resp = await parallelizer(dest_col_update_info, update_column_data)

#         update_heriarchy_resp = await parallelizer(
#             dest_heirarchy_update_info, update_hierarchy_data
#         )

#     return (
#         template_col_resp,
#         template_heirarchy_resp,
#         update_col_resp,
#         update_heriarchy_resp,
#     )


def hello_pubsub(event, context):
    """Triggered from a message on a Cloud Pub/Sub topic.
    Args:
         event (dict): Event payload.
         context (google.cloud.functions.Context): Metadata for the event.
    """
    # pubsub_message = base64.b64decode(event["data"]).decode("utf-8")
    # json_data = json.loads(pubsub_message)
    json_data = event
    print(
        "Dataset Cloner Started for client_id: ",
        json_data["client_id"],
        " for Dashboard Type: ",
        json_data["dash_type"],
        " Dataset Mapping Dictionary:",
        json_data["payload"],
    )
    # json_data = event
    return asyncio.run(main(json_data["payload"]))


task_payload = {
    "client_id": 1234,
    "dash_type": "Testing",
    "payload": {
        "c5d4f54c-df52-4e09-8a71-13429210824e": "8e99e5fd-af30-42dc-854b-00049e5f8b32",
        "4ac2fe6f-8a0f-44f9-9e56-341947869417": "866ab976-34f4-4389-b99c-57340da04f87",
        "be306e5f-c9c9-4efb-ac08-ff6637d1e08d": "aca9266e-f59d-4d4c-baa8-d6441fbd4b26",
        "79682e20-0990-419e-a249-d3a087e29227": "4e404cd9-acad-4415-8ecc-1a4cabe5ce24",
        "6963c392-6de2-4a5b-9ee1-433bf4fbd039": "77590994-8b7a-4343-ac2d-dda09b09fcac",
        "a5020c43-64ec-46ec-ac46-985e52f4fa27": "81b041df-27a9-49f3-9af7-2afa96c31ebf",
        "caa62a5f-32c5-4073-a46e-743503628b18": "f58c7b81-315f-4a2f-900c-542696fae7ab",
        "2149c6ab-4abf-42f8-82d5-5a7627bfefe1": "e388f7d2-feb8-4756-b379-a038bf08980c",
        "c5137faa-2ba4-4af8-8784-c2eb693d1623": "2e2f13a7-2eea-4adf-b4db-38ae7e4556f4",
        "e8d47a07-05c5-422c-a114-d36f1cec43a9": "710263e3-42fd-42e5-b4a6-3967ceb6c126",
        "4dc33c35-9dba-4252-a793-ce238bb702d6": "8d0e4552-8cdc-482e-8d43-10902329f3af",
        "247c6617-15da-4f5a-8b21-4f93d2467e3c": "2639e392-cbca-43c6-882e-9d2055ac376e",
        "714d7ce0-2f11-494b-89b9-f4b49808c148": "581e4830-2a0c-4185-807b-6488077f42ef",
        "2c4e5bfb-2f46-4d1a-8dbb-1aa288921b93": "74283f13-d703-4a43-a9f6-f26f85674156",
        "6b1fb25a-9e46-4722-be52-803ba37a947e": "b60965fb-c6c7-488e-9abd-0ded77cf8e3e",
        "d9e35bf0-9055-4a28-9d64-3638cf9f7ef0": "c117d336-c69d-4681-875b-a25da9014fdc",
        "1c5971dc-57c9-41f8-bc20-89772cf576b6": "48b1260b-6898-45b5-9460-653cebee1006",
        "e288285a-a31e-4e15-bea3-1a822265f2bf": "c9c7b11f-3f3e-4970-aa83-3e3edee08f5e",
        "3e530a07-e165-44ff-9b23-be030360b8cb": "695da410-f6aa-464a-9bdd-f6a12adbfcab",
        "7c8467af-17fb-400a-a6bf-fb8610a46301": "3d901891-f930-46b1-b5cc-1a436fc7a75d",
        "b39ed2bf-6269-42ba-bd4a-f0b189a9305f": "0296fa72-1202-40f0-9e7b-1284f90bad43",
        "227f18ef-4836-40f5-8b68-eff4cef4e035": "b09e00dd-5114-43eb-8916-8e4fcaf62e95",
        "16082f67-6a67-4731-80f0-56d3c54d6c0b": "d8aaa876-0097-4cb9-bb75-9f8f7bd7917a",
        "02d50340-3234-458f-812b-21c2f41a5242": "65bf9e16-08fe-4129-b76a-4bdc8ca14228",
    },
}

hello_pubsub(task_payload, "context")
