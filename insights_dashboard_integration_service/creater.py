import json
import aiohttp
import asyncio
from getDashboardJSON import getDashboardJSON
from getDatasetJSON import getDatasetJSON
from createDashboard import createDashboard


async def createDashboardAPI(session, dashboardkey, dashboardtoken, dashboardId, datasetkey, datasettoken, datasetIdMap, dashboardName):
    json1 = await getDashboardJSON(session, dashboardkey, dashboardtoken, dashboardId)
    json_string = json.dumps(json1)

    datasetMapper = {}
    idx = 0
    for dataset in json1["rows"][0]["datasets"]:
        datasetMapper[dataset["id"]] = idx
        idx += 1

    for oId, nId in datasetIdMap.items():
        json2 = await getDatasetJSON(session, datasetkey, datasettoken, nId)
        whichi = datasetMapper[oId]

        oldDatasetIds = {}
        for i in range(len(json1["rows"][0]["datasets"][whichi]["columns"])):
            oldDatasetIds[str(json1["rows"][0]["datasets"][whichi]["columns"][i]["name"]["en"]).lower()] = json1["rows"][0]["datasets"][whichi]["columns"][i]["id"]

        if "rows" not in json2 or not json2["rows"]:
            raise ValueError("No rows found in json2")
        if "columns" not in json2["rows"][0] or not json2["rows"][0]["columns"]:
            raise ValueError("No columns found in the first row of json2")

        newDatasetIds = {}
        for i in range(len(json2["rows"][0]["columns"])):
            newDatasetIds[str(json2["rows"][0]["columns"][i]["name"]["en"]).lower()] = json2["rows"][0]["columns"][i]["id"]

        idMapper = {json1["rows"][0]["datasets"][whichi]["id"]: nId}
        for k1, v1 in oldDatasetIds.items():
            idMapper[v1] = newDatasetIds.get(k1)

        for key, value in idMapper.items():
            if value is None:
                continue
            if key in json_string:
                json_string = json_string.replace(key, value)

    json_data = json.loads(json_string)
    contents = json_data["rows"][0]["contents"]
    css = json_data["rows"][0]["css"]
    dash_id = await createDashboard(session, datasetkey, datasettoken, dashboardName, contents, css)
    return dash_id

