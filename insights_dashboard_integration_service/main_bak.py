import ast
import time
import json
import base64
import requests
import numpy as np
import google.auth
import pandas as pd
import datetime
from datetime import datetime

from google.cloud import bigquery
from google.cloud import pubsub_v1
from creater import createDashboardAPI
from getDashboardJSON import getDashboardJSON
from google.api_core.exceptions import GoogleAPIError


_, PROJECT_ID = google.auth.default()
# PROJECT_ID = None

# if PROJECT_ID == "insightsprod" or PROJECT_ID == None:
if PROJECT_ID == "insightsprod":
    from config import (
        insights_dash_url,
        check_insights_dash_link,
        firebase_insights_login_payload,
        firebase_login_api_key,
        firebase_insights_login_url,
        LUZMO_API_KEY,
        LUZMO_TOKEN,
        LUZMO_BQ_ACCOUNT_ID,
        luzmo_endpoints,
        config_table_id,
        logger_table_id,
        TOPIC
    )
elif PROJECT_ID == "solutionsdw":
    from config_test import (
        insights_dash_url,
        check_insights_dash_link,
        firebase_insights_login_payload,
        firebase_login_api_key,
        firebase_insights_login_url,
        LUZMO_API_KEY,
        LUZMO_TOKEN,
        LUZMO_BQ_ACCOUNT_ID,
        luzmo_endpoints,
        config_table_id,
        logger_table_id,
    )

log_queue = []
logged_asset = None

def create_table_if_not_exists(dataset_id, table_id):
    client = bigquery.Client()
    dataset_ref = client.dataset(dataset_id)

    try:
        table_ref = dataset_ref.table(table_id)
        table = client.get_table(table_ref)
        print(f"Table {table.table_id} exists.")
        return table
    except Exception as e:
        print(f"Table not found, creating new table...")
        schema = [
            bigquery.SchemaField("timestamp", "TIMESTAMP", mode="REQUIRED"),
            bigquery.SchemaField("success", "BOOLEAN", mode="REQUIRED"),
            bigquery.SchemaField("error_message", "STRING", mode="NULLABLE"),
            bigquery.SchemaField("stage", "STRING", mode="NULLABLE"),
        ]
        table = bigquery.Table(dataset_ref.table(table_id), schema=schema)
        table = client.create_table(table)
        print(f"Table created: {table}")
        return table

def bq_logger(success, stage, error_message=None):
    global log_queue
    log_queue.append({
        "timestamp": datetime.now(),
        "success": success,
        "error_message": error_message,
        "stage": stage,
    })

    # Flush logs if the queue size exceeds a certain threshold
    if len(log_queue) >= 100:
        flush_logs()

def flush_logs():
    global log_queue
    if log_queue:
        client = bigquery.Client()
        dataset_id = logger_table_id.split(".")[1]
        table_id = logger_table_id.split(".")[2]
        table_ref = create_table_if_not_exists(dataset_id, table_id)
        errors = client.insert_rows(table_ref, log_queue)
        if errors:
            print(f"Encountered errors while inserting rows: {errors}")
        else:
            print("Success: Data inserted into BigQuery")
        log_queue = []


def warp_dataset(dataset_id):

    current_datetime = datetime.utcnow()  # Get current UTC date and time
    start_of_day = current_datetime.replace(hour=0, minute=0, second=0, microsecond=0)
    formatted_date = start_of_day.strftime("%Y-%m-%dT%H:%M:%S.%f")[:-3] + "Z"
    # formatted_date = current_datetime.strftime("%Y-%m-%dT%H:%M:%S.%f")[:-3] + "Z"

    create_acceleration_payload = {
        "version": "0.1.0",
        "action": "create",
        "properties": {
            "load_type": "full",
            "schedule": {
                "started_at": formatted_date,
                "frequency_quantity": 6,
                "frequency_unit": "hour",
            },
        },
    }

    assocaite_acceleration_payload = {
        "version": "0.1.0",
        "action": "associate",
        "resource": {
            "role": "Securable",
        },
    }

    create_acceleration_payload["key"] = LUZMO_API_KEY
    create_acceleration_payload["token"] = LUZMO_TOKEN
    assocaite_acceleration_payload["key"] = LUZMO_API_KEY
    assocaite_acceleration_payload["token"] = LUZMO_TOKEN

    headers = {
        "Content-Type": "application/json",
    }

    acceleration_url = luzmo_endpoints["acceleration_url"]

    try:
        response = requests.post(
            acceleration_url, json=create_acceleration_payload, headers=headers
        )
        response.raise_for_status()  # Raise an exception for bad status codes

        # Check if the response status code is in the success range (2xx)
        if 200 <= response.status_code < 300:
            print("Create Acc. API call successful")
            response_json = response.json()
            acceleration_id = response_json["id"]
            print("Created Acceleration ID: ", acceleration_id)
            bq_logger(success=True, stage=f"Stage Asset: {logged_asset}, Acceleration ID: {acceleration_id}")

            try:
                assocaite_acceleration_payload["id"] = acceleration_id
                assocaite_acceleration_payload["resource"]["id"] = dataset_id

                response = requests.post(
                    acceleration_url,
                    json=assocaite_acceleration_payload,
                    headers=headers,
                )
                response.raise_for_status()  # Raise an exception for bad status codes

                # Check if the response status code is in the success range (2xx)
                if 200 <= response.status_code < 300:
                    print("Associate Acc., API call successful")
                    response_json = response.json()
                    acc_assoc_id = response_json["id"]
                    print("Assoc. Acc. ID: ", acc_assoc_id)
                    bq_logger(success=True, stage=f"Stage Asset: {logged_asset}, Assoc. Acc. ID: {acc_assoc_id}")

                else:
                    print(f"API call failed with status code {response.status_code}")
                    print("Response:", response.text)
                    bq_logger(success=False, stage=f"Stage Asset: {logged_asset}, Els. Assoc. Acc. Error: ", error_message=str(response.text))

            except requests.exceptions.RequestException as e:
                print(f"Error making request: {e}")
                bq_logger(success=False, stage=f"Stage Asset: {logged_asset},  Assoc. Acc. Error: ", error_message=str(e))

        else:
            print(f"API call failed with status code {response.status_code}")
            print("Response:", response.text)
            bq_logger(success=False, stage=f"Stage Asset: {logged_asset}, Els. Acceleration Error: ", error_message=str(response.text))

    except requests.exceptions.RequestException as e:
        print(f"Error making request: {e}")
        bq_logger(success=False, stage=f"Stage Asset: {logged_asset},  Acceleration Error: ", error_message=str(e))



def create_dataset(table_id):

    dataprovider_payload = {
        "action": "create",
        "version": "0.1.0",
        "properties": {
            "provider": "bigquery",
            "action": "create",
        },
    }

    dataprovider_payload["key"] = LUZMO_API_KEY
    dataprovider_payload["token"] = LUZMO_TOKEN
    dataprovider_payload["properties"]["account_id"] = LUZMO_BQ_ACCOUNT_ID
    dataprovider_payload["properties"]["datasets"] = [table_id]

    headers = {
        "Content-Type": "application/json",
    }

    dataprovider_url = luzmo_endpoints["dataprovider_url"]

    try:
        response = requests.post(
            dataprovider_url, json=dataprovider_payload, headers=headers
        )
        response.raise_for_status()  # Raise an exception for bad status codes

        # Check if the response status code is in the success range (2xx)
        if 200 <= response.status_code < 300:
            print("Create DataProvider, API call successful")
            response_json = response.json()
            dataset_id = response_json["data"][0]["id"]
            print("Created Dataset, Dataset ID:", dataset_id)
            warp_dataset(dataset_id)
            bq_logger(success=True, stage=f"Stage Asset: {logged_asset}, Dataset ID: {dataset_id}")
            return dataset_id

        else:
            print(f"API call failed with status code {response.status_code}")
            print("Response:", response.text)
            bq_logger(success=False, stage=f"Stage Asset: {logged_asset}, Els. Dataset Error: ", error_message=str(response.text))

    except requests.exceptions.RequestException as e:
        print(f"Error making request: {e}")
        bq_logger(success=False, stage=f"Stage Asset: {logged_asset},  Dataset Error: ", error_message=str(e))



def get_matching(input_datasets, dashboard_config):
    matching_dashboards = []

    for dashboard_name, dataset_names in dashboard_config.items():
        if all(dataset_name in input_datasets for dataset_name in dataset_names):
            matching_dashboards.append(dashboard_name)

    bq_logger(success=True, stage=f"Stage Asset: {logged_asset}, Get Matching Fn")

    return matching_dashboards


def get_insights_auth():

    insights_login_params = {"key": firebase_login_api_key}
    insights_login_headers = {"Content-Type": "application/json"}

    try:
        response = requests.post(
            firebase_insights_login_url,
            params=insights_login_params,
            json=firebase_insights_login_payload,
            headers=insights_login_headers,
        )
        response.raise_for_status()

        if 200 <= response.status_code < 300:
            print("Insights Auth, API call successful")
            response_json = response.json()
            BEARER_TOKEN = response_json["idToken"]
            print("Generated, BT:", BEARER_TOKEN)
            bq_logger(success=True, stage=f"Stage Asset: {logged_asset}, Bearer Token: {BEARER_TOKEN}")
            return BEARER_TOKEN
        else:
            print("Bearer token not found in login response")
            bq_logger(success=False, stage=f"Stage Asset: {logged_asset}, Els. Bearer Token Error: ", error_message=str("Bearer token not found in login response"))


    except requests.exceptions.RequestException as e:
        print(f"Error making request: {e}")
        bq_logger(success=False, stage=f"Stage Asset: {logged_asset},  Bearer Token Error: ", error_message=str(e))



def add_to_insights(
    insight_name,
    rank,
    dash_type,
    client_id,
    dash_id,
    int_id,
    ds_man,
    ds_opt,
    dash_filters,
):

    insights_addDash_payload = {
        "name": insight_name,
        "rank": rank,
        "type": dash_type,
        "companyId": client_id,
        "properties": {
            "dashboardId": dash_id,
            "integrationId": int_id,
            "defaultDashboard": False,
            "datasources": {"mandatory": ds_man, "optional": ds_opt},
            "filters": dash_filters,
        },
    }

    BEARER_TOKEN = get_insights_auth()

    if BEARER_TOKEN:
        addDash_insights_headers = {
            "Content-Type": "application/json",
            "Authorization": f"Bearer {BEARER_TOKEN}",
        }
        print("insights_dash_url: ", insights_dash_url)
        print("insights_addDash_payload: ", insights_addDash_payload)
        print("addDash_insights_headers: ", addDash_insights_headers)
        try:
            response = requests.post(
                insights_dash_url,
                json=insights_addDash_payload,
                headers=addDash_insights_headers,
            )
            response.raise_for_status()  # Raise an exception for bad status codes

            # Check if the response status code is in the success range (2xx)
            if 200 <= response.status_code < 300:
                print("Add Dash to Insights, API call successful")
                response_json = response.json()
                log = "Response ID: " + str(response_json["id"]) + " Dash Name: " + str(response_json["name"])
                print(log)
                bq_logger(success=True, stage=f"Stage Asset: {logged_asset}, Add2Insights: {log}")
                return True
            else:
                print(f"API call failed with status code {response.status_code}")
                print("Response:", response.text)
                bq_logger(success=False, stage=f"Stage Asset: {logged_asset}, Els. Add2Insights Error: ", error_message=str(response.text))

                return False
        except requests.exceptions.RequestException as e:
            print(f"Error making request: {e}")
            bq_logger(success=False, stage=f"Stage Asset: {logged_asset},  Add2Insights Error: ", error_message=str(e))

    else:
        print("Bearer token not found in login response")


def create_integration(int_name):

    create_integration_payload = {
        "action": "create",
        "version": "0.1.0",
        "properties": {"name": {}},
    }

    create_integration_payload["key"] = LUZMO_API_KEY
    create_integration_payload["token"] = LUZMO_TOKEN
    create_integration_payload["properties"]["name"]["en"] = int_name

    headers = {
        "Content-Type": "application/json",
    }

    integration_url = luzmo_endpoints["integration_url"]

    try:
        response = requests.post(
            integration_url, json=create_integration_payload, headers=headers
        )
        response.raise_for_status()  # Raise an exception for bad status codes

        # Check if the response status code is in the success range (2xx)
        if 200 <= response.status_code < 300:
            print("Create Luzmo Integration, API call successful")
            response_json = response.json()
            integration_id = response_json["id"]
            print("Integration ID: ", integration_id)
            bq_logger(success=True, stage=f"Stage Asset: {logged_asset}, Integration ID: {integration_id}")
            return integration_id

        else:
            print(f"API call failed with status code {response.status_code}")
            print("Response:", response.text)
            bq_logger(success=False, stage=f"Stage Asset: {logged_asset}, Els. Integration Error: ", error_message=str(response.text))

    except requests.exceptions.RequestException as e:
        print(f"Error making request: {e}")
        bq_logger(success=False, stage=f"Stage Asset: {logged_asset},  Integration Error: ", error_message=str(e))



def associate_integration(integration_id, securable_id):

    associate_integration_payload = {
        "action": "associate",
        "version": "0.1.0",
        "resource": {
            "role": "Securables",
        },
        "properties": {"flagRead": "true"},
    }


    associate_integration_payload["key"] = LUZMO_API_KEY
    associate_integration_payload["token"] = LUZMO_TOKEN
    associate_integration_payload["id"] = integration_id
    associate_integration_payload["resource"]["id"] = securable_id

    headers = {
        "Content-Type": "application/json",
    }

    integration_url = luzmo_endpoints["integration_url"]

    try:
        response = requests.post(
            integration_url, json=associate_integration_payload, headers=headers
        )
        response.raise_for_status()  # Raise an exception for bad status codes

        # Check if the response status code is in the success range (2xx)
        if 200 <= response.status_code < 300:
            print("Associate with Luzmo Integration, API call successful")
            response_json = response.json()
            log = "Response ID: " + str(response_json["id"]) + " Int Name: " + str(response_json["name"])
            print(log)
            bq_logger(success=True, stage=f"Stage Asset: {logged_asset}, Assoc. Integration : {log}")
            return True
        else:
            print(f"API call failed with status code {response.status_code}")
            print("Response:", response.text)
            bq_logger(success=False, stage=f"Stage Asset: {logged_asset}, Els. Assoc. Integration Error: ", error_message=str(response.text))
            return False
    except requests.exceptions.RequestException as e:
        print(f"Error making request: {e}")
        bq_logger(success=False, stage=f"Stage Asset: {logged_asset},  Assoc. Integration Error: ", error_message=str(e))


def check_luzmo_dataset(table_id):

    check_dataset_payload = {
        "action": "get",
        "version": "0.1.0",
        "find": {
            "where": {
                "type": "dataset",
            }
        },
    }
    check_dataset_payload["key"] = LUZMO_API_KEY
    check_dataset_payload["token"] = LUZMO_TOKEN
    check_dataset_payload["find"]["where"]["source_sheet"] = table_id

    headers = {
        "Content-Type": "application/json",
    }

    securable_url = luzmo_endpoints["securable_url"]

    try:
        response = requests.post(
            securable_url, json=check_dataset_payload, headers=headers
        )
        response.raise_for_status()  # Raise an exception for bad status codes

        # Check if the response status code is in the success range (2xx)
        if 200 <= response.status_code < 300:
            print("Check for Dataset in Luzmo, API call successful")
            response_json = response.json()
            if response_json["count"] != 0:
                print("No. of Luzmo Datasets: ", response_json["count"])
                log = str(response_json["rows"][0]["id"])
                bq_logger(success=True, stage=f"Stage Asset: {logged_asset}, Check Dataset Fn. , Dataset ID : {log}")
                return response_json["rows"][0]["id"]
            else:
                bq_logger(success=True, stage=f"Stage Asset: {logged_asset}, Check Dataset Fn. , No Dataset")
                return False

        else:
            print(f"API call failed with status code {response.status_code}")
            print("Response:", response.text)
            bq_logger(success=False, stage=f"Stage Asset: {logged_asset}, Els. Check Dataset Fn. Error: ", error_message=str(response.text))

    except requests.exceptions.RequestException as e:
        print(f"Error making request: {e}")
        bq_logger(success=False, stage=f"Stage Asset: {logged_asset},  Check Dataset Fn. Error: ", error_message=str(e))



def check_insights_dashboards(client_id):

    BEARER_TOKEN = get_insights_auth()

    if BEARER_TOKEN:
        print("Got Token")
        insights_headers = {"Authorization": f"Bearer {BEARER_TOKEN}"}

        try:
            response = requests.get(
                check_insights_dash_link + client_id, headers=insights_headers
            )
            response.raise_for_status()

            if response.ok:
                print("Get Dashboard by Company ID, API call successful")
                response_json = response.json()
                dash_types = []
                for group in response_json["dashboardGroups"]:
                    for dash in group["dashboards"]:
                        if not dash["properties"]["defaultDashboard"]:
                            dash_types.append(dash["type"]["name"])

                print("Dash Type in Company: ", dash_types)
                bq_logger(success=True, stage=f"Stage Asset: {logged_asset}, Check Insights Dashboards Fn. , Dataset ID : {dash_types}")
                return dash_types

            else:
                print("Bearer token not found in login response")
                bq_logger(success=False, stage=f"Stage Asset: {logged_asset}, Els. Check Insights Dashboards Fn. Error: ", error_message=str("Bearer token not found in login response"))


        except requests.exceptions.RequestException as e:
            print(f"Error making request: {e}")
            bq_logger(success=False, stage=f"Stage Asset: {logged_asset},  Check Insights Dashboards Fn. Error: ", error_message=str(e))


def get_dash_filters(dash_id):
    try:
        json = getDashboardJSON(LUZMO_API_KEY, LUZMO_TOKEN, dash_id)
        dash_filters = []
        unique_filter_ids = set()  # Set to store unique filter IDs

        for view in json["rows"][0]["contents"]["views"]:
            for item in view["items"]:
                if item["type"] in ["datefilter", "slider", "selectbox", "slicer"]:
                    options = item.get("options", {})
                    placeholder = options.get("placeholder", {})
                    placeholder_en = placeholder.get("en", "")

                    filter_id = item["id"]
                    if filter_id not in unique_filter_ids:
                        unique_filter_ids.add(filter_id)
                        dash_filters.append({
                            "id": filter_id,
                            "name": placeholder_en,
                        })
                        print("Placeholder Name:", placeholder_en, ":", filter_id)
        bq_logger(success=True, stage=f"Stage Asset: {logged_asset}, Get Dashboard Filters Fn.: {dash_filters}")
        return dash_filters

    except (KeyError, TypeError, requests.RequestException, json.decoder.JSONDecodeError) as e:
        print(f"An error occurred: {e}")
        bq_logger(success=False, stage=f"Stage Asset: {logged_asset}, Get Dashboard Filters Fn. Error: ", error_message=str(e))
        return None  # Or handle the error in an appropriate way


def bq_table_exists(table_id):
    client = bigquery.Client(project=table_id.split(".")[0])
    dataset_ref = client.dataset(table_id.split(".")[1])
    table_ref = dataset_ref.table(table_id.split(".")[2])

    try:
        client.get_table(table_ref)
        bq_logger(success=True, stage=f"Stage Asset: {logged_asset}, BQ Table Check Fn.: {table_id}")
        return True
    except Exception as e:
        # Table does not exist or other error occurred
        bq_logger(success=False, stage=f"Stage Asset: {logged_asset}, BQ Table Check Fn. Error: ", error_message=str(e))
        return False


def config_loader(table_id):
    try:
        client = bigquery.Client(project=table_id.split(".")[0])
        query_string = f"""select * from `{table_id}`"""

        if bq_table_exists(table_id):
            print(f"Dest. Table {table_id} exists.")
            assets_df = client.query(query_string).result().to_dataframe()
            if assets_df.empty:
                log = str("config table is empty! " + table_id)
                print(log)
                bq_logger(success=False, stage=f"Stage Asset: {logged_asset}, Els. Config Loader Fn.: {log}")
            else:
                print("Got Config!", assets_df.head())
                bq_logger(success=True, stage=f"Stage Asset: {logged_asset}, Config Loader Fn.: {table_id}")
                return assets_df
        else:
            print(f"Config Table {table_id} not found.")
            return None
    except GoogleAPIError as e:
        print(f"An error occurred: {e}")
        bq_logger(success=False, stage=f"Stage Asset: {logged_asset}, Config Loader Fn. Error: ", error_message=str(e))
        return None


def hello_pubsub(event, context):
    #     """Triggered from a message on a Cloud Pub/Sub topic.
    #     Args:
    #          event (dict): Event payload.
    #          context (google.cloud.functions.Context): Metadata for the event.
    #     """
    try:
        if PROJECT_ID:
            print("Starting Function in Cloud Mode, with Project ID: ", PROJECT_ID)
            pubsub_message = base64.b64decode(event["data"]).decode("utf-8")
            print(pubsub_message)
            input_json = json.loads(pubsub_message)
            config_df = config_loader(config_table_id)
            publisher = pubsub_v1.PublisherClient()
            topic_path = publisher.topic_path(PROJECT_ID, TOPIC)
        else:
            print("Starting Function in Local Mode!")
            input_json = event
            config_df = pd.read_csv("../misc./CSVs/config.csv")

        if config_df is not None:
            client_id = input_json["client_id"]
            client_name = input_json["client_name"]
            client_dataset = input_json["dataset_name"]
            project_id = input_json["project_id"]
            integration_id = None
            global logged_asset

            dataset_dict = {}

            dashboard_config = {
                key: ast.literal_eval(value)
                for key, value in config_df[["dashboard_id", "dataset_names"]]
                .set_index("dashboard_id")
                .to_dict(orient="dict")["dataset_names"]
                .items()
            }

            configTables_arrays = [eval(item) for item in config_df['dataset_names']]
            combined_configTables_array = np.concatenate(configTables_arrays)
            unique_configTable_names = np.unique(combined_configTables_array)

            eligible_tables_array = [item for item in input_json["table_names"] if item in unique_configTable_names]

            for table_name in eligible_tables_array:
                try:
                    table_id = project_id + ":" + client_dataset + "." + table_name
                    print(table_id)
                    logged_asset = client_id + "_" + table_name
                    dataset_id = check_luzmo_dataset(table_id)
                    if dataset_id:
                        print(
                            f"""Dataset exists in Luzmo, Passing it as Dataset ID 
                            Table Name: {table_name} Table ID: {dataset_id} """
                        )
                    else:
                        print(
                            f"""Dataset does not exist in Luzmo, Creating New Dataset ID
                            Table Name: {table_name} Table ID: {dataset_id}"""
                        )
                        dataset_id = create_dataset(table_id)

                    dataset_dict[table_name] = dataset_id.strip()
                    bq_logger(success=True, stage=f"Stage Asset: {logged_asset}, Main Fn. Table Import Loop Dataset ID: {dataset_id}")
                except Exception as e:
                    print(f"An error occurred while processing table {table_name}: {str(e)}")
                    bq_logger(success=False, stage=f"Stage Asset: {logged_asset}, Main Fn. Table Import Loop Error: ", error_message=str(e))

            print("Dataset Dictionary:", dataset_dict)
            existing_dash_types = check_insights_dashboards(client_id)
            possible_dashboards = get_matching(
                input_datasets=dataset_dict, dashboard_config=dashboard_config
            )
            exclusion_list_dash_id = []
            for t in existing_dash_types:
                if config_df["dashboard_type"].isin([t]).any():
                    exclusion_list_dash_id.append(
                        config_df.loc[config_df["dashboard_type"] == t, "dashboard_id"].iloc[0]
                    )
            # comment the below line after use (excluding Inventory Amazon)
            # exclusion_list_dash_id.append("e5110000-e62a-4a9d-8e0c-43c7cd887a2d")
            dash_ids = [
                item for item in possible_dashboards if item not in exclusion_list_dash_id
            ]
            print("Eligible Dashboards: ", dash_ids)
            if dash_ids:
                integration_id = create_integration(
                    int_name=client_id + "_" + client_name + "_" + str(time.time_ns())
                )
                if integration_id:
                    print("Created Integration with ID: ", integration_id)
                    for tbl_nm, d_id in dataset_dict.items():
                        print(f"Associating {tbl_nm} to Integration")
                        associate_integration(integration_id, securable_id=d_id)

            for dash_id in dash_ids:
                print(dash_id)
                datasetIdMap = {}
                logged_asset = client_id + "_" + dash_id

                for dataset_name in dashboard_config[dash_id]:
                    print(dataset_name)
                    old_datatset_id = ast.literal_eval(
                        config_df.loc[
                            config_df["dashboard_id"] == dash_id, "dataset_dict"
                        ].iloc[0]
                    )[dataset_name]
                    new_dataset_id = dataset_dict[dataset_name]
                    datasetIdMap[old_datatset_id] = new_dataset_id

                print("Dataset Mapping Dictionary: ", datasetIdMap)

                if PROJECT_ID:
                    try:
                        data = json.dumps(datasetIdMap).encode("utf-8")
                        future = publisher.publish(topic_path, data)
                        message_id = future.result()
                        print(f'Message published to Pub/Sub topic {topic_path}, message ID: {message_id}')
                        bq_logger(success=True, stage=f"Stage Asset: {logged_asset}, Main Fn. Table Cloner Invocation Loop Message ID : {message_id}")
                    except Exception as e:
                        print(f"An error occurred while processing table {table_name}: {str(e)}")
                        bq_logger(success=False, stage=f"Stage Asset: {logged_asset}, Main Fn. Table Cloner Invocation Loop Error: ", error_message=str(e))

                new_dash_id = createDashboardAPI(
                    dashboardkey=LUZMO_API_KEY,
                    dashboardtoken=LUZMO_TOKEN,
                    dashboardId=dash_id.strip(),
                    datasetkey=LUZMO_API_KEY,
                    datasettoken=LUZMO_TOKEN,
                    datasetIdMap=datasetIdMap,
                    dashboardName=str(
                        config_df.loc[
                            config_df["dashboard_id"] == dash_id, "dashboard_name"
                        ].iloc[0]
                    )
                    + "_"
                    + client_name
                    + "_"
                    + str(client_id),
                )

                associate_integration(integration_id, securable_id=new_dash_id)
                new_dash_filters = get_dash_filters(new_dash_id)
                dash_meta = config_df.loc[config_df["dashboard_id"] == dash_id]
                add_to_insights(
                    insight_name=dash_meta["dashboard_name"].iloc[0],
                    rank=str(dash_meta["dashboard_rank"].iloc[0]).strip(),
                    dash_type=dash_meta["dashboard_type"].iloc[0].strip(),
                    client_id=str(client_id),
                    dash_id=new_dash_id,
                    int_id=integration_id,
                    ds_man=ast.literal_eval(dash_meta["mandatory_src"].iloc[0]),
                    ds_opt=ast.literal_eval(dash_meta["optional_src"].iloc[0]),
                    dash_filters=new_dash_filters
                )

        flush_logs()
        
    except Exception as e:
        print("An error occurred:", str(e))
