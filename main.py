import time
from facebook_business.api import FacebookAdsApi
from facebook_business.adobjects.adaccount import AdAccount
from datetime import datetime, timedelta
import json
from facebook_business.adobjects.adsinsights import AdsInsights
from facebook_business.adobjects.adreportrun import AdReportRun
import requests
import csv
import pandas as pd

# Step 1: Initialize API
ACCESS_TOKEN = ''
AD_ACCOUNT_IDS = ['act_10154725495867831', 'act_337235638130357', 'act_952301619019241']
BASE_URL = f'https://graph.facebook.com/v22.0'
since = '2024-10-01'
until = '2024-10-31'

for AD_ACCOUNT_ID in AD_ACCOUNT_IDS:
    OUTPUT_FILE = f'facebook_campaign_report_{AD_ACCOUNT_ID}_{since}_to_{until}.csv'
    print(f"\n📊 Processing account: {AD_ACCOUNT_ID}")
    FacebookAdsApi.init(access_token=ACCESS_TOKEN)
    account = AdAccount(AD_ACCOUNT_ID)

    # Step 2: Define insights parameters
    submit_url = f"{BASE_URL}/{AD_ACCOUNT_ID}/insights"
    params = {
        'access_token': ACCESS_TOKEN,
        'level': 'campaign',
        'fields': ['campaign_id,campaign_name,account_id,account_name,actions,unique_actions,clicks,impressions,reach,inline_link_clicks,spend,frequency,date_start,date_stop'],
        'time_range[since]': since,
        'time_range[until]': until,
        'limit': 10,
        'async': 'true'
    }

    print("Submitting async report job...")
    job = account.get_insights(params=params, is_async=True)

    # Try extracting the report_run_id
    report_run_id = job.get(AdReportRun.Field.id)  #This is a recommended parameter for the async jobs

    if not report_run_id:
        raise Exception(f"❌ No report_run_id in response for account {AD_ACCOUNT_ID}: {job}")

    print(f"✅ Report Run ID: {report_run_id}")

    # --- Poll for status ---
    status_url = f"https://graph.facebook.com/v22.0/{report_run_id}"
    timeout_minutes = 5
    start_time = time.time()

    # Step 4: Poll job status every 30 seconds
    while True:
        status_resp = requests.get(status_url, params={'access_token': ACCESS_TOKEN})
        status_data = status_resp.json()

        status = status_data.get('async_status')
        percent = status_data.get('async_percent_completion', 0)

        print(f"Status: {status}, Completion: {percent}%")

        if status == 'Job Completed':
            print("✅ Job completed.")
            break
        elif status == 'Job Failed':
            raise Exception("❌ Async job failed.")
        time.sleep(10)

    # ✅ Step 6: Download report (OUTSIDE the while loop)
    print("📥 Downloading report...")
    results = job.get_result()

    if results:
        flattened_rows = []

        for row in results:
            row_dict = dict(row)

            # Flatten action-type lists like 'actions' and 'unique_actions'
            for action_field in ['actions', 'unique_actions']:
                if action_field in row_dict and isinstance(row_dict[action_field], list):
                    try:
                        for item in row_dict[action_field]:
                            if isinstance(item, dict) and 'action_type' in item and 'value' in item:
                                column_name = item['action_type']
                                row_dict[column_name] = item['value']
                    except Exception as e:
                        print(f"Warning: Failed to flatten field '{action_field}' - {e}")
                    row_dict.pop(action_field, None)

            flat = pd.json_normalize(row_dict, sep='_').to_dict(orient='records')[0]
            flattened_rows.append(flat)

        df = pd.DataFrame(flattened_rows)
        df.to_csv(OUTPUT_FILE, index=False, quoting=csv.QUOTE_NONE, escapechar=' ')
        print(f"✅ Report downloaded as {OUTPUT_FILE}")
    else:
        print("⚠️ No data returned in the report.")

