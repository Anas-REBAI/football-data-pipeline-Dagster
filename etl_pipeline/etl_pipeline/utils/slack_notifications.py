import requests
from etl_pipeline.config.settings import SLACK_WEBHOOK_URL

def send_slack_message(message: str):
    """Sends a message to a Slack channel using a webhook."""

    # Check if the webhook URL is configured
    if not SLACK_WEBHOOK_URL:
        print("❌ Slack Webhook URL is missing!")
        return

    # Prepare the payload
    payload = {"text": message}

    # Send the POST request to Slack
    response = requests.post(SLACK_WEBHOOK_URL, json=payload)

    # Handle response status
    if response.status_code != 200:
        print(f"❌ Failed to send Slack message: {response.text}")
    else:
        print("✅ Slack notification sent successfully!")
