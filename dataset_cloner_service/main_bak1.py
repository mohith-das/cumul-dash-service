import base64
import json
import asyncio
import aiohttp
import requests
import time

# import nest_asyncio
from config import LUZMO_API_KEY, LUZMO_TOKEN, luzmo_endpoints

# nest_asyncio.apply()


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

    try:
        response = requests.post(
            dataset_url,
            json=get_dataset_json_payload,
            headers=headers,
        )
        response.raise_for_status()  # Raise an exception for bad status codes

        # Check if the response status code is in the success range (2xx)
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
            print(
                f"getDatasetJSON API call failed for dataset_id: {dataset_id} with status_code:",
                response.status_code,
            )

    except requests.exceptions.RequestException as e:
        print(f"Dataset Error making request: {e}")
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

    async with session.post(
        dataset_url, json=get_column_json_payload, headers=headers
    ) as response:
        response.raise_for_status()

        if 200 <= response.status < 300:
            json_response = await response.json()
            return column_id, column_name, json_response
        else:
            print(
                f"fetch_column_data API call failed for session_id: {session}, column_id: {column_id}, column_name: {column_name} with status_code: ",
                response.status,
            )


async def update_column_data(session, column_id, column_name, column_data):
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
    async with session.post(
        update_url, json=update_column_json_payload, headers=headers
    ) as response:
        response.raise_for_status()

        if 200 <= response.status < 300:
            return await response.json()
        else:
            print(
                f"update_column_data API failed for session_id: {session}, column_id: {column_id}, column_name: {column_name}, column_data: {column_data} with status_code: ",
                response.status,
            )


async def fetch_hierarchy_data(session, column_id, dataset_id):
    hierarchy_url = luzmo_endpoints["hierarchy_url"]

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

    async with session.post(
        hierarchy_url, json=get_hierarchy_json_payload, headers=headers
    ) as response:
        response.raise_for_status()

        if 200 <= response.status < 300:
            json_response = await response.json()
            return column_id, json_response
        else:
            print(
                f"fetch_hierarchy_data API call failed for session_id: {session}, column_id: {column_id}, dataset_id: {dataset_id} with status_code:",
                response.status,
            )


# async def update_hierarchy_data(session, column_id, hierarchy_data):
#     update_hierarchy_json_payload = {
#         "action": "update",
#         "version": "0.1.0",
#         "key": LUZMO_API_KEY,
#         "token": LUZMO_TOKEN,
#         "id": column_id,
#         "properties": {"updates": hierarchy_data},
#     }

#     update_url = luzmo_endpoints["hierarchy_url"]

#     headers = {
#         "Content-Type": "application/json",
#     }

#     async with session.post(
#         update_url, json=update_hierarchy_json_payload, headers=headers
#     ) as response:
#         # response.raise_for_status()

#         if 200 <= response.status < 300:
#             return await response.json()
#         else:
#             print(
#                 "update_hierarchy_data API call failed for session_id: {session}, column_id: {column_id}, hierarchy_data: {hierarchy_data} with status_code: ",
#                 response.status,
#             )


MAX_RETRIES = 5  # Maximum number of retries
INITIAL_RETRY_DELAY = 2  # Initial delay for retries in seconds
MAX_EXECUTION_TIME = 540  # Cloud function execution limit in seconds


async def update_hierarchy_data(session, column_id, hierarchy_data):
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
    start_time = time.time()

    for attempt in range(MAX_RETRIES):
        try:
            async with session.post(
                update_url, json=update_hierarchy_json_payload, headers=headers
            ) as response:
                if response.status == 429:  # Too Many Requests
                    print(
                        f"Rate limit exceeded, retrying after {retry_delay} seconds..."
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


async def parallelizer(task_info, function_to_call):
    print("Parallelizer Initiailised with Function: ", str(function_to_call))
    async with aiohttp.ClientSession() as session:
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


async def main(dataset_mapping):
    print("Main Fn : Dataset Mapping :", dataset_mapping)
    for key_dataset_id, value_dataset_id in dataset_mapping.items():
        print("Mapping Template from: ", key_dataset_id, "To: ", value_dataset_id)
        template_columns, temp_heirarchy_cols = getDatasetJSON(key_dataset_id)
        dest_columns, dest_heirarchy_cols = getDatasetJSON(value_dataset_id)

        dest_col_rev_dict = {
            value_col_nm: key_col_id
            for key_col_id, value_col_nm in dest_columns.items()
        }
        print("Reverse Column Dict :", dest_col_rev_dict)

        template_col_resp = await parallelizer(
            [(k, v) for k, v in template_columns.items()], fetch_column_data
        )

        template_heirarchy_resp = await parallelizer(
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
                # Handle exception (e.g., log it, print it, etc.)
                print(
                    f"Error processing column update info for {column_id}, {column_name}: {e}"
                )

        dest_heirarchy_update_info = []
        for column_id, response in template_heirarchy_resp:
            try:
                item = (
                    dest_col_rev_dict[template_columns[column_id]],
                    response[0]["children"],
                )
                dest_heirarchy_update_info.append(item)
            except Exception as e:
                # Handle exception (e.g., log it, print it, etc.)
                print(f"Error processing hierarchy update info for {column_id}: {e}")

        update_col_resp = await parallelizer(dest_col_update_info, update_column_data)

        update_heriarchy_resp = await parallelizer(
            dest_heirarchy_update_info, update_hierarchy_data
        )

    return (
        template_col_resp,
        template_heirarchy_resp,
        update_col_resp,
        update_heriarchy_resp,
    )


def hello_pubsub(event, context):
    """Triggered from a message on a Cloud Pub/Sub topic.
    Args:
         event (dict): Event payload.
         context (google.cloud.functions.Context): Metadata for the event.
    """
    # pubsub_message = base64.b64decode(event["data"]).decode("utf-8")
    # json_data = json.loads(pubsub_message)
    # print(
    #     "Dataset Cloner Started for client_id: ",
    #     json_data["client_id"],
    #     " for Dashboard Type: ",
    #     json_data["dash_type"],
    #     " Dataset Mapping Dictionary:",
    #     json_data["payload"],
    # )
    # json_data = event
    # return asyncio.run(main(json_data["payload"]))
    return asyncio.run(main(event))


event = {
    "7c8467af-17fb-400a-a6bf-fb8610a46301": "494b779b-17a9-4739-8aff-60bcbd8567e5",
    "4d159133-7904-4183-b514-1e813c6c85d1": "20b28109-1ad4-4969-a710-cbe3a13260fa",
    "d9e35bf0-9055-4a28-9d64-3638cf9f7ef0": "2c4766ac-4e35-40ee-943f-df5237064828",
    "a5020c43-64ec-46ec-ac46-985e52f4fa27": "46ac34eb-051e-465f-a6d2-26d0ab8189d5",
    "09fe41b3-1f22-4677-88c5-dfd759c5cf2d": "b159fe03-713e-4d26-96e9-e78e27717166",
    "c5d4f54c-df52-4e09-8a71-13429210824e": "2e7135e1-15a2-4dab-877f-d5c8cdef5fb9",
    "714d7ce0-2f11-494b-89b9-f4b49808c148": "be6f26bd-56c8-4948-9ee0-9d6ff504b8d1",
    "b39ed2bf-6269-42ba-bd4a-f0b189a9305f": "f9a2a941-9e91-495c-a008-d0a3650ca55d",
    "1ad2a54a-1ab0-4d3c-baab-e1187a539a2b": "356fb93b-8a56-4621-beda-c1cc54fcea48",
    "1c777a54-1116-43bc-bc63-cf42e29b66d7": "65b29fb2-dd49-4731-bbd5-7415287c4859",
}

hello_pubsub(event, "context")
