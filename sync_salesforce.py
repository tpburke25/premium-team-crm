"""
Salesforce → Supabase Sync Script
Premium Team CRM
Runs via GitHub Actions on a schedule
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

# ── SALESFORCE AUTH ───────────────────────────────────
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


# ── SUPABASE UPSERT ───────────────────────────────────
def supabase_upsert(table, rows):
    if not rows:
        print(f"  No rows to upsert for {table}")
        return 0
    headers = {
        'apikey':        SUPABASE_KEY,
        'Authorization': f'Bearer {SUPABASE_KEY}',
        'Content-Type':  'application/json',
        'Prefer':        'resolution=merge-duplicates'
    }
    upserted = 0
    for i in range(0, len(rows), BATCH_SIZE):
        batch = rows[i:i + BATCH_SIZE]
        res   = requests.post(
            f"{SUPABASE_URL}/rest/v1/{table}",
            headers=headers,
            json=batch
        )
        if res.ok:
            upserted += len(batch)
            print(f"  Batch {i//BATCH_SIZE + 1}: {len(batch)} rows upserted ✓")
        else:
            print(f"  Batch {i//BATCH_SIZE + 1} error: {res.text[:300]}")
    return upserted


# ── HELPERS ───────────────────────────────────────────
def clean_date(val):
    if not val: return None
    return str(val)[:10]

def clean_num(val):
    if val is None: return None
    try: return float(val)
    except: return None

def clean_int(val):
    if val is None: return None
    try: return int(float(val))
    except: return None

def now_iso():
    return datetime.now(timezone.utc).isoformat()


# ── OPPORTUNITIES SYNC ────────────────────────────────
def sync_opportunities(token, instance):
    print("\nSyncing Opportunities...")
    soql = """
        SELECT
            Id, AccountId, Name, StageName, CloseDate, CreatedDate,
            LeadSource, Description, Closed_Reason__c,
            Owner.Name, Additional_Rep__c,
            Account.Name, Account.ParentId, Account.Parent.Name,
            Account.Industry, Account.BillingCity, Account.BillingState,
            Account.BillingPostalCode,
            Account.FTS_ID__c,
            Account.Days_Since_Activity__c,
            Account.Accounting_Package__c,
            ASM__c,
            Total_Flat_Rate__c,
            Setup_Amount__c,
            Total_Setup_Amount__c,
            Total_of_Setup_and_ASM__c,
            Loc__c,
            Product_Interests__c
        FROM Opportunity
        WHERE IsDeleted = false
        AND Owner.LastName IN ('Burke', 'Adcock', 'Pottle', 'Cuellar', 'Behymer')
        ORDER BY CloseDate ASC
    """
    records = sf_query(token, instance, soql)
    print(f"  Pulled {len(records)} records from Salesforce")

    seen = set()
    rows = []
    for r in records:
        if r['Id'] in seen:
            continue
        seen.add(r['Id'])
        acc    = r.get('Account') or {}
        parent = acc.get('Parent') or {}
        rows.append({
            'opportunity_id':       r.get('Id'),
            'account_id':           r.get('AccountId'),
            'fts_id':               acc.get('FTS_ID__c'),
            'parent_account_id':    acc.get('ParentId'),
            'account_name':         acc.get('Name'),
            'parent_account':       parent.get('Name'),
            'opportunity_name':     r.get('Name'),
            'opportunity_owner':    (r.get('Owner') or {}).get('Name'),
            'additional_rep':       r.get('Additional_Rep__c'),
            'stage':                r.get('StageName'),
            'created_date':         clean_date(r.get('CreatedDate')),
            'close_date':           clean_date(r.get('CloseDate')),
            'days_since_activity':  clean_int(acc.get('Days_Since_Activity__c')),
            'closed_reason':        r.get('Closed_Reason__c'),
            'description':          r.get('Description'),
            'lead_source':          r.get('LeadSource'),
            'num_locations':        clean_int(r.get('Loc__c')),
            'product_name':         r.get('Product_Interests__c'),
            'industry':             acc.get('Industry'),
            'accounting_package':   acc.get('Accounting_Package__c'),
            'city':                 acc.get('BillingCity'),
            'state':                acc.get('BillingState'),
            'zip':                  acc.get('BillingPostalCode'),
            'asm_flat_rate':        clean_num(r.get('ASM__c')),
            'total_flat_rate':      clean_num(r.get('Total_Flat_Rate__c')),
            'setup_amount':         clean_num(r.get('Setup_Amount__c')),
            'total_setup_amount':   clean_num(r.get('Total_Setup_Amount__c')),
            'total_setup_and_flat': clean_num(r.get('Total_of_Setup_and_ASM__c')),
            'synced_at':            now_iso(),
        })

    upserted = supabase_upsert('tbl_opportunities', rows)
    print(f"  ✓ {upserted} opportunities upserted to Supabase")
    return len(rows), upserted


# ── TASKS SYNC ────────────────────────────────────────
def sync_tasks(token, instance):
    print("\nSyncing Tasks...")
    soql = """
        SELECT
            Id, AccountId, WhoId, Who.Name,
            Subject, Type, Status, Priority,
            ActivityDate, CreatedDate,
            Owner.Name, Description,
            Account.Name, Account.Industry,
            Account.FTS_ID__c,
            Account.Accounting_Package__c,
            Account.Days_Since_Activity__c
        FROM Task
        WHERE IsDeleted = false
        AND Status != 'Completed'
        AND Owner.LastName IN ('Burke', 'Adcock', 'Pottle', 'Cuellar', 'Behymer')
        ORDER BY ActivityDate DESC
    """
    records = sf_query(token, instance, soql)
    print(f"  Pulled {len(records)} tasks from Salesforce")

    rows = []
    for r in records:
        acc = r.get('Account') or {}
        who = r.get('Who') or {}
        rows.append({
            'activity_id':        r.get('Id'),
            'account_id':         r.get('AccountId'),
            'fts_id':             acc.get('FTS_ID__c'),
            'account_name':       acc.get('Name'),
            'subject':            r.get('Subject'),
            'type':               r.get('Type'),
            'status':             r.get('Status'),
            'priority':           r.get('Priority'),
            'due_date':           clean_date(r.get('ActivityDate')),
            'created_date':       clean_date(r.get('CreatedDate')),
            'assigned_to':        (r.get('Owner') or {}).get('Name'),
            'comments':           r.get('Description'),
            'industry':           acc.get('Industry'),
            'accounting_package': acc.get('Accounting_Package__c'),
            'contact_name':       who.get('Name') if (r.get('WhoId') or '').startswith('003') else None,
            'days_since_activity': clean_int(acc.get('Days_Since_Activity__c')),
            'synced_at':          now_iso(),
        })

    upserted = supabase_upsert('tbl_tasks', rows)
    print(f"  ✓ {upserted} tasks upserted to Supabase")
    return len(rows), upserted



# ── ACTIVITIES SYNC ──────────────────────────────────
def sync_activities(token, instance):
    print("\nSyncing Activities...")
    soql = """
        SELECT
            Id, AccountId, WhoId, Who.Name,
            Subject, Type, Status,
            ActivityDate, CreatedDate,
            Owner.Name, Description,
            Account.Name, Account.Industry,
            Account.FTS_ID__c,
            Account.Accounting_Package__c
        FROM Task
        WHERE IsDeleted = false
        AND Status = 'Completed'
        AND ActivityDate >= LAST_N_DAYS:180
        AND Owner.LastName IN ('Burke', 'Adcock', 'Pottle', 'Cuellar', 'Behymer')
        ORDER BY ActivityDate DESC
    """
    records = sf_query(token, instance, soql)
    print(f"  Pulled {len(records)} activity records from Salesforce")

    rows = []
    for r in records:
        acc = r.get('Account') or {}
        who = r.get('Who') or {}
        rows.append({
            'activity_id':        r.get('Id'),
            'account_id':         r.get('AccountId'),
            'fts_id':             acc.get('FTS_ID__c'),
            'account_name':       acc.get('Name'),
            'subject':            r.get('Subject'),
            'type':               r.get('Type'),
            'status':             r.get('Status'),
            'activity_date':      clean_date(r.get('ActivityDate')),
            'created_date':       clean_date(r.get('CreatedDate')),
            'assigned_to':        (r.get('Owner') or {}).get('Name'),
            'comments':           r.get('Description'),
            'industry':           acc.get('Industry'),
            'accounting_package': acc.get('Accounting_Package__c'),
            'contact_name':       who.get('Name') if (r.get('WhoId') or '').startswith('003') else None,
            'synced_at':          now_iso(),
        })

    upserted = supabase_upsert('tbl_activities', rows)
    print(f"  ✓ {upserted} activities upserted to Supabase")
    return len(rows), upserted


# ── MAIN ──────────────────────────────────────────────
def main():
    print("=" * 50)
    print("Premium Team CRM — Salesforce Sync")
    print(f"Started: {now_iso()}")
    print("=" * 50)

    token, instance = sf_login()
    results = {}

    pulled, upserted = sync_opportunities(token, instance)
    results['opportunities'] = {'pulled': pulled, 'upserted': upserted}

    pulled, upserted = sync_tasks(token, instance)
    results['tasks'] = {'pulled': pulled, 'upserted': upserted}

    pulled, upserted = sync_activities(token, instance)
    results['activities'] = {'pulled': pulled, 'upserted': upserted}

    print("\n" + "=" * 50)
    print("Sync Summary:")
    for obj, counts in results.items():
        print(f"  {obj}: {counts['pulled']} pulled, {counts['upserted']} upserted")
    print(f"Finished: {now_iso()}")
    print("=" * 50)

if __name__ == '__main__':
    main()
