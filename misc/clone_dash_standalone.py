import asyncio
import aiohttp
import sys
import tracemalloc

tracemalloc.start()
sys.path.insert(0, "../insights_dashboard_integration_service")
from creater import createDashboardAPI
import pandas as pd

# dashboard_df = dashboard_df_og.copy()
# dataset_df = dataset_df_og.copy()
failed_dashboards = []
PROD_LUZMO_API_KEY = "REDACTED"
PROD_LUZMO_TOKEN = "REDACTED"
SRC_API_KEY = PROD_LUZMO_API_KEY
SRC_TOKEN = PROD_LUZMO_TOKEN
DEST_API_KEY = SRC_API_KEY
DEST_TOKEN = SRC_TOKEN
# int_name_var = "tejaswini_dashboards_int"
# client_id = 3058
# dash_ids = dataset_df["dashboard_id"].unique()

# dataset_df.set_index("dashboard_id", inplace=True)
# dashboard_df.set_index("dashboard_id", inplace=True)


# integration_id = create_integration(int_name=int_name_var)
async def main(data):
    df = pd.read_csv("input_cloner_stdaln.csv", index_col=False)
    result = {}
    dash_ids = df["dash_id"].unique()

    for _, row in df.iterrows():
        print(row)
        dash_id = row["dash_id"]
        src_dataset_id = row["src_dataset_id"]
        dest_dataset_id = row["dest_dataset_id"]

        if dash_id not in result:
            result[dash_id] = {}

        result[dash_id][src_dataset_id] = dest_dataset_id

    # Print the resulting dictionary
    print(result)

    for dash_id in dash_ids:
        print(dash_id)
        # datasetIdMap = {}

        # for row in dataset_df.loc[[dash_id]].iterrows():
        #     # row[1]["dataset_id"]
        #     print(row[1]["dataset_id"])
        #     old_datatset_id = row[1]["dataset_id"].strip()
        #     # new_dataset_id = dashboard_df.loc[[dash_id], "dest_dataset"].iloc[0]
        #     new_dataset_id = row[1]["dataset_id"].strip()
        #     # associate_integration(integration_id, securable_id=new_dataset_id)
        #     datasetIdMap[old_datatset_id] = new_dataset_id

        print("Dataset Map:", result[dash_id])
        print(
            "Dash ID: ",
            dash_id,
            " Dash Name: ",
            df.loc[df["dash_id"] == dash_id, "dashboard_name"].iloc[0],
        )
        async with aiohttp.ClientSession() as session:
            new_dash_id = await createDashboardAPI(
                session,
                dashboardkey=SRC_API_KEY,
                dashboardtoken=SRC_TOKEN,
                dashboardId=dash_id.strip(),
                datasetkey=DEST_API_KEY,
                datasettoken=DEST_TOKEN,
                datasetIdMap=result[dash_id],
                dashboardName=df.loc[df["dash_id"] == dash_id, "dashboard_name"].iloc[
                    0
                ],
            )
            print("New DashID:", new_dash_id)
        # associate_integration(integration_id, securable_id=new_dash_id)
        # new_dash_filters = get_dash_filters(new_dash_id)

        # dash_meta = dashboard_df.loc[dash_id]
        # print(dash_meta)
        # add_to_insights(
        #     insight_name=dash_meta["dashboard_name"],
        #     rank=str(dash_meta["dashboard_rank"]).strip(),
        #     dash_type=dash_meta["dashboard_type"].strip(),
        #     client_id=client_id,
        #     dash_id=new_dash_id,
        #     int_id=integration_id,
        #     ds_man=ast.literal_eval(dash_meta["mandatory_src"]),
        #     ds_opt=ast.literal_eval(dash_meta["optional_src"]),
        #     dash_filters=new_dash_filters,
        # )


asyncio.run(main("event"))
