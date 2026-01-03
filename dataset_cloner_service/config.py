# Luzmo Credentials & URLs

import os

# "LUZMO_API_KEY", "LUZMO_TOKEN"
LUZMO_KnT_CREDENTIALS = [
    (
        os.getenv("LUZMO_API_KEY"),
        os.getenv("LUZMO_TOKEN"),
    ),
]

luzmo_base_url = "https://api.us.cumul.io/0.1.0/"
luzmo_endpoints = {
    "column_url": luzmo_base_url + "column",
    "securable_url": luzmo_base_url + "securable",
    "hierarchy_url": luzmo_base_url + "hierarchy",
}
