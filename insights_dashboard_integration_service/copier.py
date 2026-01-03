import ast
import pandas as pd
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
from creater import createDashboardAPI
from getDashboardJSON import getDashboardJSON
from insights_dashboard_integration_service.main_old import (
    associate_integration,
    create_integration,
    add_to_insights,
    get_dash_filters,
)

# failed_dashboards = []
SRC_API_KEY = LUZMO_API_KEY
SRC_TOKEN = LUZMO_TOKEN
DEST_API_KEY = SRC_API_KEY
DEST_TOKEN = SRC_TOKEN
int_name_var = "godirect_prod_3670"
client_id = 3670


# def get_datasetFromDash(dash_df):
#     result_df = pd.DataFrame(columns=["dashboard_id", "dataset_name", "dataset_id"])
#     for idx, dash_id in enumerate(dash_df["dashboard_id"]):
#         if not pd.isna(dash_id):
#             print("Index:", idx, " Dash ID:", dash_id)
#             json_data = getDashboardJSON(LUZMO_API_KEY, LUZMO_TOKEN, dash_id)
#             for dataset in json_data["rows"][0]["datasets"]:
#                 dataset_name = dataset["name"]["en"]
#                 dataset_id = dataset["id"]
#                 dashboard_id = dash_id
#                 result_df = result_df.append(
#                     {
#                         "dashboard_id": dashboard_id,
#                         "dataset_name": dataset_name,
#                         "dataset_id": dataset_id,
#                     },
#                     ignore_index=True,
#                 )

#     result_df["dataset_name"] = result_df["dataset_name"].str.lower()
#     return result_df


dashboard_df = pd.read_csv("kbrands_fin.csv")
# dataset_df = get_datasetFromDash(dashboard_df)
# dataset_df.to_csv("sai_extract_bak_dataset.csv")

dash_ids = dashboard_df["dashboard_id"].unique()

# dataset_df.set_index("dashboard_id", inplace=True)
dashboard_df.set_index("dashboard_id", inplace=True)


integration_id = create_integration(int_name=int_name_var)

for dash_id in dash_ids:
    print(dash_id)
    datasetIdMap = {}

    for row in dashboard_df.loc[[dash_id]].iterrows():
        print(row[1]["dataset_id"])
        old_datatset_id = row[1]["dataset_id"].strip()
        # new_dataset_id = dashboard_df.loc[[dash_id], "dest_dataset"].iloc[0]
        new_dataset_id = row[1]["dst_dataset_id"].strip()
        associate_integration(integration_id, securable_id=new_dataset_id.strip())
        datasetIdMap[old_datatset_id] = new_dataset_id

    new_dash_id = createDashboardAPI(
        dashboardkey=SRC_API_KEY,
        dashboardtoken=SRC_TOKEN,
        dashboardId=dash_id.strip(),
        datasetkey=DEST_API_KEY,
        datasettoken=DEST_TOKEN,
        datasetIdMap=datasetIdMap,
        dashboardName=str(dashboard_df.loc[dash_id, "dashboard_name"]) + str(client_id),
    )
    associate_integration(integration_id, securable_id=new_dash_id)
    # new_dash_filters = get_dash_filters(new_dash_id)
    # dashboard_type = dashboard_df.loc[dash_id][1]
    # dash_meta = dashboard_df.loc[dash_id]
    add_to_insights(
        insight_name=dashboard_df.loc[dash_id, "dashboard_name"],
        rank=str(dashboard_df.loc[dash_id, "dashboard_rank"]).strip(),
        dash_type=dashboard_df.loc[dash_id, "dashboard_type"].strip(),
        client_id=str(client_id),
        dash_id=new_dash_id,
        int_id=integration_id,
        ds_man=["AMAZONSELLER", "SHOPIFYV2"],
        ds_opt=[
            "AMAZONSBADS",
            "AMAZONSDADS",
            "AMAZONSPADS",
            "GOOGLEADS",
            "FACEBOOK",
            "BINGADS",
            "GA4",
            "KLAVIYO3",
            "PINTEREST_V2",
            "GSS",
        ],
        dash_filters="",
        # dash_filters=new_dash_filters,
    )
