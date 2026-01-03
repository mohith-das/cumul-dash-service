# Insights URLs and Credentials
import os
insights_dash_url = "https://test-insights-service.sarasanalytics.com/dashboard"
check_insights_dash_link = (
    "https://test-insights-service.sarasanalytics.com/dashboard/by-company/"
)
firebase_insights_login_payload = {
    "email": os.getenv("INSIGHTS_EMAIL_TEST"),
    "password": os.getenv("INSIGHTS_PASSWORD_TEST"),
    "returnSecureToken": True,
}
firebase_login_api_key = os.getenv("FIREBASE_LOGIN_API_KEY_TEST")
firebase_insights_login_url = (
    "https://identitytoolkit.googleapis.com/v1/accounts:signInWithPassword"
)

# Luzmo Credentials & URLs
LUZMO_API_KEY = os.getenv("LUZMO_API_KEY_TEST")
LUZMO_TOKEN = os.getenv("LUZMO_TOKEN_TEST")
LUZMO_BQ_ACCOUNT_ID = os.getenv("LUZMO_BQ_ACCOUNT_ID_TEST")

luzmo_base_url = "https://api.us.cumul.io/0.1.0/"
luzmo_endpoints = {
    "acceleration_url": luzmo_base_url + "acceleration",
    "dataprovider_url": luzmo_base_url + "dataprovider",
    "integration_url": luzmo_base_url + "integration",
    "securable_url": luzmo_base_url + "securable",
}

# Dashboard Service Resources
config_table_id = "solutionsdw.insights_config.insights_dashboardService_config"
logger_table_id = "solutionsdw.insights_config.dash_service_logger"

#Slack Variables
slack_workflow_link = os.getenv("SLACK_WORKFLOW_LINK")
