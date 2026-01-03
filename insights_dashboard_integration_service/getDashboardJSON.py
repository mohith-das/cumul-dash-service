import aiohttp
import asyncio
import requests
import json

# Define API endpoint
api_url = "https://api.us.cumul.io/0.1.0/securable"

async def getDashboardJSON(session, key, token, dashboardId):
    payload = {
        "action": "get",
        "key": key,
        "token": token,
        "version": "0.1.0",
        "find": {
            "where": {"id": dashboardId, "type": "dashboard"},
            "include": [
                {
                    "model": "Securable",
                    "as": "Datasets",
                    "include": [
                        {
                            "attributes": ["id", "name"],
                            "model": "Column",
                            "jointype": "inner",
                        }
                    ],
                }
            ],
        },
    }
    async with session.post(api_url, json=payload) as response:
        response.raise_for_status()
        if response.status == 200:
            data = await response.json()
            print("Got Dashboard JSON for:", dashboardId)
            return data
        else:
            print("API request failed with status code:", response.status)

