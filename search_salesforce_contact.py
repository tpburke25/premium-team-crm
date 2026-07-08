"""
search_salesforce_contact.py
Premium Team CRM

One-time / on-demand pull: find Salesforce Contacts matching a name (or other
field), along with their associated Accounts, and write results into tbl_search
in Supabase. Uses the same OAuth/SOAP auth and Supabase REST pattern as
sync_salesforce.py. Designed to be re-run for different searches without
overwriting previous results (each run is tagged with a search_label and
run_at timestamp in tbl_search).

Usage:
    python search_salesforce_contact.py "<search_label>" "<search_field>" "<search_value>"

Example:
    python search_salesforce_contact.py "Karen Quiroz - Accountant" "Name" "Karen Quiroz"
"""

import os
import sys
import requests
from datetime import datetime, timezone

# ── CONFIG ────────────────────────────────────────────
SF_USERNAME     = os.environ['SF_USERNAME']
SF_PASSWORD     = os.environ['SF_PASSWORD']
SF_INSTANCE_URL = os.environ['SF_INSTANCE_URL']
SUPABASE_URL    = os.environ['SUPABASE_URL']
SUPABASE_KEY    = os.environ['SUPABASE_KEY']

SF_LOGIN_URL    = 'https://login.salesforce.com'
BATCH_SIZE      = 500

SEARCH_LABEL = sys.argv[1]   # e.g. 'Karen Quiroz - Accountant'
SEARCH_FIELD = sys.argv[2]   # e.g. 'Name'
SEARCH_VALUE = sys.argv[3]   # e.g. 'Karen Quiroz'


# ── SALESFORCE AUTH (identical to sync_salesforce.py) ─
def sf_login():
    print("Authenticating with Salesforce...")
    res = requests.post(f"{SF_LOGIN_URL}/services/oauth2/token", data={
        'grant_type':    'password',
        'client_id':     'PlatformCLI',
        'client_secret': '',
        'username':      SF_USERNAME,
        'password':      SF_PASSWORD,
    })
    if res.ok:
        data = res.json()
        print(f"Logged in via OAuth to {data['instance_url']}")
        return data['access_token'], data['instance_url']

    # SOAP fallback
    import xml.etree.ElementTree as ET
    res = requests.post(f"{SF_LOGIN_URL}/services/Soap/u/57.0",
        headers={'Content-Type': 'text/xml', 'SOAPAction': 'login'},
        data=f"""<?xml version="1.0" encoding="utf-8"?>
<soapenv:Envelope xmlns:soapenv="http://schemas.xmlsoap.org/soap/envelope/"
    xmlns:urn="urn:partner.soap.sforce.com">
  <soapenv:Body>
    <urn:login>
      <urn:username>{SF_USERNAME}</urn:username>
      <urn:password>{SF_PASSWORD}</urn:password>
    </urn:login>
  </soapenv:Body>
</soapenv:Envelope>""")
    if not res.ok:
        print(f"Login failed: {res.text}")
        sys.exit(1)
    root = ET.fromstring(res.text)
    ns = {'sf': 'urn:partner.soap.sforce.com'}
    token    = root.find('.//sf:sessionId', ns).text
    instance = root.find('.//sf:serverUrl', ns).text.split('/services')[0]
    print(f"Logged in via SOAP to {instance}")
    return token, instance


def sf_query(token, instance, soql):
    headers = {'Authorization': f'Bearer {token}', 'Content-Type': 'application/json'}
    url     = f"{instance}/services/data/v57.0/query"
    rows    = []
    params  = {'q': soql}
    while True:
        res = requests.get(url, headers=headers, params=params)
        if not res.ok:
            print(f"Query failed: {res.text}")
            return rows
        data = res.json()
        rows.extend(data.get('records', []))
        if data.get('done', True):
            break
        url    = instance + data['nextRecordsUrl']
        params = {}
    return rows


# ── SUPABASE INSERT (append-only — no upsert, no dedup) ─
def supabase_insert(table, rows):
    if not rows:
        print(f"  No rows to insert for {table}")
        return 0
    headers = {
        'apikey':        SUPABASE_KEY,
        'Authorization': f'Bearer {SUPABASE_KEY}',
        'Content-Type':  'application/json',
    }
    inserted = 0
    for i in range(0, len(rows), BATCH_SIZE):
        batch = rows[i:i + BATCH_SIZE]
        res   = requests.post(
            f"{SUPABASE_URL}/rest/v1/{table}",
            headers=headers,
            json=batch
        )
        if res.ok:
            inserted += len(batch)
            print(f"  Batch {i//BATCH_SIZE + 1}: {len(batch)} rows inserted ✓")
        else:
            print(f"  Batch {i//BATCH_SIZE + 1} error: {res.text[:300]}")
    return inserted


def now_iso():
    return datetime.now(timezone.utc).isoformat()


# ── MAIN ──────────────────────────────────────────────
def main():
    print("=" * 50)
    print("Premium Team CRM — Salesforce Contact/Account Search")
    print(f"Search: {SEARCH_LABEL} ({SEARCH_FIELD} LIKE '%{SEARCH_VALUE}%')")
    print(f"Started: {now_iso()}")
    print("=" * 50)

    token, instance = sf_login()

    soql = f"""
        SELECT Id, Name, Email, Phone, AccountId, Account.Name, Account.FTS_ID__c
        FROM Contact
        WHERE {SEARCH_FIELD} LIKE '%{SEARCH_VALUE}%'
    """
    records = sf_query(token, instance, soql)
    print(f"Found {len(records)} matching Contact record(s).")

    rows = []
    for r in records:
        account = r.get('Account') or {}
        rows.append({
            'search_label':  SEARCH_LABEL,
            'search_field':  f"Contact.{SEARCH_FIELD}",
            'search_value':  SEARCH_VALUE,
            'contact_id':    r.get('Id'),
            'contact_name':  r.get('Name'),
            'contact_email': r.get('Email'),
            'contact_phone': r.get('Phone'),
            'account_id':    r.get('AccountId'),
            'account_name':  account.get('Name'),
            'fts_id':        account.get('FTS_ID__c'),
            'run_at':        now_iso(),
        })

    inserted = supabase_insert('tbl_search', rows)
    print(f"\n✓ {inserted} row(s) inserted into tbl_search")
    print(f"Finished: {now_iso()}")
    print("=" * 50)


if __name__ == '__main__':
    main()
