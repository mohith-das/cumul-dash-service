import base64
import json
import asyncio
import aiohttp
import requests

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
        json_response = await response.json()
        return column_id, column_name, json_response


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
        return await response.json()


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
        json_response = await response.json()
        return column_id, json_response


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
    # print(update_hierarchy_json_payload)
    async with session.post(
        update_url, json=update_hierarchy_json_payload, headers=headers
    ) as response:
        return await response.json()


async def parallel_column_calls(column_info):
    async with aiohttp.ClientSession() as session:

        tasks = []
        for column_id, column_name in column_info.items():
            try:
                task = fetch_column_data(session, column_id, column_name)
                tasks.append(task)
            except Exception as e:
                # Handle exception (e.g., log it, print it, etc.)
                print(f"Error fetching column data for {column_id}, {column_name}: {e}")

        results = await asyncio.gather(*tasks)
        print("Executed Job Count (Fetch Columns):", len(results))
        return results


async def parallel_column_updates(column_info):
    async with aiohttp.ClientSession() as session:

        tasks = []
        for column_id, column_name, column_data in column_info:
            try:
                task = update_column_data(session, column_id, column_name, column_data)
                tasks.append(task)
            except Exception as e:
                # Handle exception (e.g., log it, print it, etc.)
                print(f"Error updating column data for {column_id}, {column_name}: {e}")

        results = await asyncio.gather(*tasks)
        print("Executed Job Count (Update Columns):", len(results))
        return results


async def parallel_hierarchy_calls(column_ids, dataset_id):
    async with aiohttp.ClientSession() as session:

        tasks = []
        for column_id in column_ids:
            try:
                task = fetch_hierarchy_data(session, column_id, dataset_id)
                tasks.append(task)
            except Exception as e:
                # Handle exception (e.g., log it, print it, etc.)
                print(f"Error fetching hierarchy data for {column_id}: {e}")

        results = await asyncio.gather(*tasks)
        print("Executed Job Count (Fetch Hierarchy):", len(results))
        return results


async def parallel_hierarchy_updates(hierarchy_info):
    async with aiohttp.ClientSession() as session:

        tasks = []
        for column_id, hierarchy_data in hierarchy_info:
            try:
                task = update_hierarchy_data(session, column_id, hierarchy_data)
                tasks.append(task)
            except Exception as e:
                # Handle exception (e.g., log it, print it, etc.)
                print(f"Error updating hierarchy data for {column_id}: {e}")

        results = await asyncio.gather(*tasks)
        print("Executed Job Count (Update Heirarchy):", len(results))
        return results


async def parallel_tasks(task_info, function_to_call):
    async with aiohttp.ClientSession() as session:
        # Create tasks dynamically based on the function and its arguments

        tasks = []
        for args in task_info:
            try:
                task = function_to_call(session, *args)
                tasks.append(task)
            except Exception as e:
                # Handle exception (e.g., log it, print it, etc.)
                print(f"Error calling function with arguments {args}: {e}")

        # Gather the results
        results = await asyncio.gather(*tasks)
        return results


async def main(dataset_mapping):
    print("Main Fn : Dataset Mapping :", dataset_mapping)
    for key, value in dataset_mapping.items():
        print("Mapping Template from: ", key, "To: ", value)
        template_columns, temp_heirarchy_cols = getDatasetJSON(key)
        new_columns, new_heirarchy_cols = getDatasetJSON(value)

        reversed_new_dict = {value: key for key, value in new_columns.items()}
        print("Reverse Column Dict :", reversed_new_dict)
        # Asynchronously fetch data for columns
        column_responses = await parallel_column_calls(template_columns)

        heirarchy_reponses = await parallel_hierarchy_calls(temp_heirarchy_cols, key)

        column_update_info = []
        for column_id, column_name, response in column_responses:
            try:
                item = (
                    reversed_new_dict[column_name],
                    column_name,
                    response["rows"][0],
                )
                column_update_info.append(item)
            except Exception as e:
                # Handle exception (e.g., log it, print it, etc.)
                print(
                    f"Error processing column update info for {column_id}, {column_name}: {e}"
                )

        heirarchy_update_info = []
        for column_id, response in heirarchy_reponses:
            try:
                item = (
                    reversed_new_dict[template_columns[column_id]],
                    response[0]["children"],
                )
                heirarchy_update_info.append(item)
            except Exception as e:
                # Handle exception (e.g., log it, print it, etc.)
                print(f"Error processing hierarchy update info for {column_id}: {e}")

        # Asynchronously make update calls for columns
        update_responses = await parallel_column_updates(column_update_info)

        update_heriarchy_responses = await parallel_hierarchy_updates(
            heirarchy_update_info
        )

    return (
        column_responses,
        column_update_info,
        update_responses,
        update_heriarchy_responses,
    )


def hello_pubsub(event, context):
    """Triggered from a message on a Cloud Pub/Sub topic.
    Args:
         event (dict): Event payload.
         context (google.cloud.functions.Context): Metadata for the event.
    """
    pubsub_message = base64.b64decode(event["data"]).decode("utf-8")
    json_data = json.loads(pubsub_message)
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
