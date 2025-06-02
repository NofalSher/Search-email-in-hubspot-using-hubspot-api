import requests
import pandas as pd
import time
from config import HUBSPOT_API_KEY, INPUT_FILE, OUTPUT_FILE, EMAIL_COLUMN

# === HUBSPOT API Setup ===
HEADERS = {
    "Authorization": f"Bearer {HUBSPOT_API_KEY}",
    "Content-Type": "application/json"
}

def find_contact_by_email(email):
    """Search contact by email (primary or additional) and return contact ID."""
    url = "https://api.hubapi.com/crm/v3/objects/contacts/search"
    payload = {
        "filterGroups": [
            {
                "filters": [
                    {
                        "propertyName": "email",
                        "operator": "EQ",
                        "value": email
                    }
                ]
            },
            {
                "filters": [
                    {
                        "propertyName": "hs_additional_emails",
                        "operator": "CONTAINS_TOKEN",
                        "value": email
                    }
                ]
            }
        ],
        "properties": ["email", "hs_additional_emails"]
    }

    while True:
        response = requests.post(url, headers=HEADERS, json=payload)

        if response.status_code == 200:
            results = response.json().get('results', [])
            if results:
                return results[0]['id']
            return None

        elif response.status_code == 429:
            print(f"Rate limit hit. Waiting 10 seconds before retrying for email: {email}")
            time.sleep(10)
            continue

        else:
            print(f"Error searching for email {email}: Status {response.status_code} - {response.text}")
            return None

# === READ INPUT FILE ===
df = pd.read_excel(INPUT_FILE)

# === PROCESS EMAILS ===
results = []

for idx, row in df.iterrows():
    input_email = row.get(EMAIL_COLUMN)
    print(f"Processing row {idx + 1}/{len(df)}: {input_email}")

    if pd.isna(input_email) or not isinstance(input_email, str) or not input_email.strip():
        print(f"  Skipping invalid or empty email at row {idx + 1}")
        contact_id = None
    else:
        contact_id = find_contact_by_email(input_email.strip())

    results.append({
        "input_email": input_email,
        "contact_id": contact_id if contact_id else ""
    })

# === WRITE OUTPUT FILE ===
output_df = pd.DataFrame(results)
output_df.to_excel(OUTPUT_FILE, index=False)

print(f"\nDone. Results saved to {OUTPUT_FILE}")
