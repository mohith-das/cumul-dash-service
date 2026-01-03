import ast
import time
import json
import base64
import requests
import google.auth
import pandas as pd
from datetime import datetime

from google.cloud import bigquery
from creater import createDashboardAPI
from getDashboardJSON import getDashboardJSON


# _, PROJECT_ID = google.auth.default()
PROJECT_ID = None

if PROJECT_ID == "insightsprod" or PROJECT_ID == None:
# if PROJECT_ID == "insightsprod":
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


def create_table_if_not_exists(dataset_id, table_id):
    client = bigquery.Client()
    dataset_ref = client.dataset(dataset_id)
    table_ref = dataset_ref.table(table_id)

    try:
        client.get_table(table_ref)
    except Exception as e:
        print("Table not found, creating new table...")
        schema = [
            bigquery.SchemaField("timestamp", "TIMESTAMP", mode="REQUIRED"),
            bigquery.SchemaField("success", "BOOLEAN", mode="REQUIRED"),
            bigquery.SchemaField("error_message", "STRING", mode="NULLABLE"),
            bigquery.SchemaField("stage", "STRING", mode="NULLABLE"),
        ]
        table = bigquery.Table(table_ref, schema=schema)
        table = client.create_table(table)
        print("Table created: {}".format(table.table_id))


def bq_logger(success, stage, error_message=None):
    global log_queue
    log_queue.append({
        "timestamp": datetime.datetime.now(),
        "success": success,
        "error_message": error_message,
        "stage": stage,
    })

    # Flush logs if the queue size exceeds a certain threshold
    if len(log_queue) >= 100:  # Adjust threshold as needed
        flush_logs()

def flush_logs():
    global log_queue
    if log_queue:
        client = bigquery.Client()
        dataset_id = logger_table_id.split(".")[1]
        table_id = logger_table_id.split(".")[2]
        create_table_if_not_exists(dataset_id, table_id)
        table_ref = client.dataset(dataset_id).table(table_id)
        errors = client.insert_rows(table_ref, log_queue)
        if errors:
            print("Encountered errors while inserting rows: {}".format(errors))
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
            # bq_logger(success=True, stage=f"Acceleration Created with ID {acceleration_id}")

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

                else:
                    print(f"API call failed with status code {response.status_code}")
                    print("Response:", response.text)
            except requests.exceptions.RequestException as e:
                print(f"Error making request: {e}")

        else:
            print(f"API call failed with status code {response.status_code}")
            print("Response:", response.text)
    except requests.exceptions.RequestException as e:
        print(f"Error making request: {e}")


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
            return dataset_id

        else:
            print(f"API call failed with status code {response.status_code}")
            print("Response:", response.text)
    except requests.exceptions.RequestException as e:
        print(f"Error making request: {e}")


def get_matching(input_datasets, dashboard_config):
    matching_dashboards = []

    for dashboard_name, dataset_names in dashboard_config.items():
        if all(dataset_name in input_datasets for dataset_name in dataset_names):
            matching_dashboards.append(dashboard_name)

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
            return BEARER_TOKEN
        else:
            print("Bearer token not found in login response")

    except requests.exceptions.RequestException as e:
        print(f"Error making request: {e}")


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
                print(
                    "Response ID: ",
                    response_json["id"],
                    " Dash Name: ",
                    response_json["name"],
                )
                return True
            else:
                print(f"API call failed with status code {response.status_code}")
                print("Response:", response.text)
                return False
        except requests.exceptions.RequestException as e:
            print(f"Error making request: {e}")
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
            return integration_id

        else:
            print(f"API call failed with status code {response.status_code}")
            print("Response:", response.text)
    except requests.exceptions.RequestException as e:
        print(f"Error making request: {e}")


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
            print(
                "Response ID: ",
                response_json["id"],
                " Int Name: ",
                response_json["name"],
            )
            return True
        else:
            print(f"API call failed with status code {response.status_code}")
            print("Response:", response.text)
            return False
    except requests.exceptions.RequestException as e:
        print(f"Error making request: {e}")


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
                return response_json["rows"][0]["id"]
            else:
                return False

        else:
            print(f"API call failed with status code {response.status_code}")
            print("Response:", response.text)
    except requests.exceptions.RequestException as e:
        print(f"Error making request: {e}")


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
                return dash_types

            else:
                print("Bearer token not found in login response")

        except requests.exceptions.RequestException as e:
            print(f"Error making request: {e}")


def get_dash_filters(dash_id):


    json = getDashboardJSON(LUZMO_API_KEY, LUZMO_TOKEN, dash_id)
    dash_filters = []
    unique_filter_ids = set()  # Set to store unique filter IDs

    for view in json["rows"][0]["contents"]["views"]:
        for item in view["items"]:
            if item["type"] in ["datefilter", "slider", "selectbox", "slicer"]:
                # if item["options"]["placeholder"]["en"]:
                if "options" in item and isinstance(item["options"], dict):
                    options = item["options"]
                    if "placeholder" in options and isinstance(options["placeholder"], dict):
                        placeholder = options["placeholder"]
                        if "en" in placeholder:
                            placeholder_en = placeholder["en"]

                            filter_id = item["id"]
                            # Check if the filter ID is not already in the set
                            if filter_id not in unique_filter_ids:
                                unique_filter_ids.add(filter_id)
                                dash_filters.append(
                                    {
                                        "id": filter_id,
                                        "name": placeholder_en,
                                    }
                                )
                                print("Placeholder Name:", placeholder_en,":",filter_id,)

    return dash_filters


def bq_table_exists(table_id):
    client = bigquery.Client(project=table_id.split(".")[0])
    dataset_ref = client.dataset(table_id.split(".")[1])
    table_ref = dataset_ref.table(table_id.split(".")[2])

    try:
        client.get_table(table_ref)
        return True
    except Exception as e:
        # Table does not exist or other error occurred
        return False


def config_loader(table_id):
    client = bigquery.Client(project=table_id.split(".")[0])
    query_string = f"""select * from `{table_id}`"""

    if bq_table_exists(table_id):
        print(f"Dest. Table {table_id} exists.")
        assets_df = client.query(query_string).result().to_dataframe()
        if assets_df.empty:
            print("Config Read Empty")
        else:
            print("Got Config!", assets_df.head())
            return assets_df
    else:
        print(f"Config Table {table_id} not found.")
        flag = 0


def hello_pubsub(event, context):
    #     """Triggered from a message on a Cloud Pub/Sub topic.
    #     Args:
    #          event (dict): Event payload.
    #          context (google.cloud.functions.Context): Metadata for the event.
    #     """
    if PROJECT_ID:
        print("Starting Function in Cloud Mode, with Project ID: ", PROJECT_ID)
        pubsub_message = base64.b64decode(event["data"]).decode("utf-8")
        print(pubsub_message)
        input_json = json.loads(pubsub_message)
        config_df = config_loader(config_table_id)
    else:
        print("Starting Function in Local Mode!")
        input_json = event
        config_df = pd.read_csv("../misc./CSVs/config.csv")

    client_id = input_json["client_id"]
    client_name = input_json["client_name"]
    client_dataset = input_json["dataset_name"]
    project_id = input_json["project_id"]
    integration_id = None

    dataset_dict = {}

    dashboard_config = {
        key: ast.literal_eval(value)
        for key, value in config_df[["dashboard_id", "dataset_names"]]
        .set_index("dashboard_id")
        .to_dict(orient="dict")["dataset_names"]
        .items()
    }

    for table_name in input_json["table_names"]:
        table_id = project_id + ":" + client_dataset + "." + table_name
        print(table_id)
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
            dash_filters=new_dash_filters,
            #  dash_filters=[]
        )


input_json = {
   "client_id": "3777",

  "client_name": "sitaradirectinc",

  "project_id": "insightsprod",

  "dataset_name": "sitaradirectinc_3777_prod_presentation",
    "table_names": [
#       "FacebookAds_Advertising_cumul",
# "FacebookAds_Advertising_pbi",
 "Executive_Dashboard_pbi",
"LostSpend_Advertising_pbi",
 
# "Inventory_Amazon_cumul",
# "Inventory_Amazon_DistinctAsins",
 
# "PnL_Amazon_Cumul",
 
# "Amazon_account_overview_customer_section_cumul",
# "Amazon_account_overview_sales",
# "Amazon_account_overview_feedback",
# "amazon_account_overview_sales_marketing_profit",
# "amazon_account_overview_product_cumul",
# "Amazon_account_overview_listings_cumul",
# "amazon_account_overview_customer_rfm_cumul",
 
# "customer_cohortwise_pivot_cumul",
# "customer_rfm_segments_cumul",
# "customer_glance_cltv_cumul",
# "customer_glance_pbi",
# "customer_glance_customer_metrics_pbi"
    ]}

# hello_pubsub(input_json, "")
