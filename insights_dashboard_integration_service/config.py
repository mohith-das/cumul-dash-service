# Insights URLs and Credentials
import os
insights_dash_url = "https://insights-service.sarasanalytics.com/dashboard"
check_insights_dash_link = (
    "https://insights-service.sarasanalytics.com/dashboard/by-company/"
)
firebase_insights_login_payload = {
    "email": os.getenv("INSIGHTS_EMAIL"),
    "password": os.getenv("INSIGHTS_PASSWORD"),
    "returnSecureToken": True,
}
firebase_login_api_key = os.getenv("FIREBASE_LOGIN_API_KEY")
firebase_insights_login_url = (
    "https://identitytoolkit.googleapis.com/v1/accounts:signInWithPassword"
)

# Luzmo Credentials & URLs
LUZMO_API_KEY = os.getenv("LUZMO_API_KEY")
LUZMO_TOKEN = os.getenv("LUZMO_TOKEN")
LUZMO_BQ_ACCOUNT_ID = os.getenv("LUZMO_BQ_ACCOUNT_ID")

luzmo_base_url = "https://api.us.cumul.io/0.1.0/"
luzmo_endpoints = {
    "acceleration_url": luzmo_base_url + "acceleration",
    "dataprovider_url": luzmo_base_url + "dataprovider",
    "integration_url": luzmo_base_url + "integration",
    "securable_url": luzmo_base_url + "securable",
    "collection_url": luzmo_base_url + "collection",
}

# Dashboard Service Resources
config_table_id = "insightsprod.insights_config.insights_dashboardService_config_prod"
logger_table_id = "insightsprod.insights_config.dash_service_logger"

# Insights Dataset Cloner Assets
TOPIC = "insights_dataset_cloner"

# Slack Variables
slack_workflow_link = os.getenv("SLACK_WORKFLOW_LINK")
