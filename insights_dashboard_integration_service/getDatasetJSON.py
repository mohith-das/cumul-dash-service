import aiohttp
import asyncio
import requests
import json

# Define API endpoint
api_url = "https://api.us.cumul.io/0.1.0/securable"


async def getDatasetJSON(session, key, token, datasetId):
    payload = {
        "action": "get",
        "key": key,
        "token": token,
        "version": "0.1.0",
        "find": {
            "where": {
                "id": datasetId,
            },
            "attributes": ["id", "name", "source_sheet"],
            "include": [
                {"model": "Column", "attributes": ["name", "id", "source_name"]}
            ],
        },
    }
    async with session.post(api_url, json=payload) as response:
        response.raise_for_status()
        if response.status == 200:
            data = await response.json()
            print("Got Dataset JSON for:", datasetId)
            return data
        else:
            print("API request failed with status code:", response.status)
