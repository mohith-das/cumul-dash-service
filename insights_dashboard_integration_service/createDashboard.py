import requests
import json

# Define API endpoint
api_url = "https://api.us.cumul.io/0.1.0/securable"



async def createDashboard(session, key, token, dashboardName, contents, css):
    # Define request payload
    payload = {
        "action": "create",
        "key": key,
        "token": token,
        "version": "0.1.0",
        "properties": {
            "type": "dashboard",
            "name": {"en": dashboardName},
            "contents": contents,
            "css": css,
        },
    }
    
    async with session.post(api_url, json=payload) as response:
        # Check response status code
        if response.status == 200:
            print("Successfully Created the dashboard!")
            response_json = await response.json()
            dash_id = response_json["id"]
            return dash_id
        else:
            # Handle API error
            print("API request failed with status code:", response.status)

