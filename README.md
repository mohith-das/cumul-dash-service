# Cumul Dash Service

Sanitized snapshot of dashboard integration utilities and dataset cloning workflows for a Luzmo/Insights stack.

## Highlights
- Automates dashboard integration and dataset cloning tasks.
- Organizes multiple scripts for lineage, integration, and utilities.
- Secrets are provided via environment variables only.

## Tech
Python, Luzmo API, GCP services

## Structure
- `dataset_cloner_service/` - Clone datasets and related assets.
- `insights_dashboard_integration_service/` - Integrate dashboards with Insights services.
- `insights_service/` - Supporting services.
- `dash_lineage_update/` - Lineage helpers.
- `misc/` - Prototypes and scratchpad artifacts.

## Configuration
Provide secrets via environment variables (no secrets are stored in this repo):
- `LUZMO_API_KEY`
- `LUZMO_TOKEN`
- `LUZMO_BQ_ACCOUNT_ID`
- `INSIGHTS_EMAIL`
- `INSIGHTS_PASSWORD`
- `FIREBASE_LOGIN_API_KEY`
- `SLACK_WORKFLOW_LINK`

Optional test variants:
- `INSIGHTS_EMAIL_TEST`
- `INSIGHTS_PASSWORD_TEST`
- `FIREBASE_LOGIN_API_KEY_TEST`
- `LUZMO_API_KEY_TEST`
- `LUZMO_TOKEN_TEST`
- `LUZMO_BQ_ACCOUNT_ID_TEST`

## Usage
```bash
python insights_dashboard_integration_service/main.py
```

## Notes
- This repo is a sanitized snapshot with secrets removed.
