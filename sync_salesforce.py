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


# ── SUPABASE DELETE STALE ─────────────────────────────
def supabase_delete_stale(table, id_field, current_ids):
    """Delete rows from table whose id_field value is not in current_ids."""
    headers = {
        'apikey':        SUPABASE_KEY,
        'Authorization': f'Bearer {SUPABASE_KEY}',
        'Content-Type':  'application/json',
    }

    existing_ids = []
    page, size = 0, 1000
    while True:
        res = requests.get(
            f"{SUPABASE_URL}/rest/v1/{table}?select={id_field}&limit={size}&offset={page * size}",
            headers=headers
        )
        if not res.ok:
            print(f"  Could not fetch existing IDs from {table}: {res.text[:200]}")
            return 0
        batch = res.json()
        if not isinstance(batch, list):
            break
        existing_ids.extend(r[id_field] for r in batch if r.get(id_field))
        if len(batch) < size:
            break
        page += 1

    current_set = set(current_ids)
    stale_ids   = [i for i in existing_ids if i not in current_set]

    if not stale_ids:
        print(f"  No stale rows to delete from {table}")
        return 0

    deleted = 0
    for i in range(0, len(stale_ids), BATCH_SIZE):
        batch    = stale_ids[i:i + BATCH_SIZE]
        id_list  = ','.join(f'"{id}"' for id in batch)
        res = requests.delete(
            f"{SUPABASE_URL}/rest/v1/{table}?{id_field}=in.({id_list})",
            headers=headers
        )
        if res.ok:
            deleted += len(batch)
        else:
            print(f"  Delete batch error: {res.text[:200]}")

    print(f"  Deleted {deleted} stale rows from {table}")
    return deleted


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

def derive_close_month(close_date_str):
    """Derive YYYY-MM from a YYYY-MM-DD date string."""
    if not close_date_str: return None
    return str(close_date_str)[:7]


# ── CONTACT ROLES FETCH ───────────────────────────────
def fetch_primary_contacts(token, instance, opp_ids):
    """
    Fetch primary contacts from OpportunityContactRole for a list of opp IDs.
    Returns a dict: { opportunity_id: { name, title, phone, email } }
    """
    if not opp_ids:
        return {}

    print("  Fetching primary contacts from OpportunityContactRole...")
    contacts = {}

    # Process in batches to avoid SOQL length limits
    batch_size = 200
    for i in range(0, len(opp_ids), batch_size):
        batch   = opp_ids[i:i + batch_size]
        id_list = "', '".join(batch)
        soql    = f"""
            SELECT
                OpportunityId,
                Contact.Name,
                Contact.Title,
                Contact.Phone,
                Contact.Email,
                IsPrimary
            FROM OpportunityContactRole
            WHERE OpportunityId IN ('{id_list}')
            AND IsPrimary = true
        """
        records = sf_query(token, instance, soql)
        for r in records:
            opp_id  = r.get('OpportunityId')
            contact = r.get('Contact') or {}
            if opp_id and opp_id not in contacts:
                contacts[opp_id] = {
                    'contact_name':  contact.get('Name'),
                    'contact_title': contact.get('Title'),
                    'contact_phone': contact.get('Phone'),
                    'contact_email': contact.get('Email'),
                }

    print(f"  Found primary contacts for {len(contacts)} opportunities")
    return contacts


# ── OPPORTUNITIES SYNC ────────────────────────────────
def sync_opportunities(token, instance):
    print("\nSyncing Opportunities...")
    soql = """
        SELECT
            Id, AccountId, Name, StageName, CloseDate, CreatedDate,
            LastModifiedDate, LastActivityDate,
            LeadSource, Description, Closed_Reason__c,
            Type,
            Owner.Name, Owner.UserRole.Name, Owner.Department, Owner.Title,
            Additional_Rep__c,
            Account.Name, Account.ParentId, Account.Parent.Name,
            Account.Industry, Account.Type,
            Account.BillingStreet, Account.BillingCity, Account.BillingState,
            Account.BillingPostalCode,
            Account.FTS_ID__c,
            Account.Days_Since_Activity__c,
            Account.LastActivityDate,
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

    # Collect opp IDs for contact roles fetch
    opp_ids = [r['Id'] for r in records]
    primary_contacts = fetch_primary_contacts(token, instance, opp_ids)

    seen = set()
    rows = []
    for r in records:
        if r['Id'] in seen:
            continue
        seen.add(r['Id'])
        acc    = r.get('Account') or {}
        parent = acc.get('Parent') or {}
        owner  = r.get('Owner') or {}
        role   = owner.get('UserRole') or {}
        opp_id = r.get('Id')
        pc     = primary_contacts.get(opp_id, {})

        close_date = clean_date(r.get('CloseDate'))
        rows.append({
            'opportunity_id':         opp_id,
            'account_id':             r.get('AccountId'),
            'fts_id':                 acc.get('FTS_ID__c'),
            'parent_account_id':      acc.get('ParentId'),
            'account_name':           acc.get('Name'),
            'parent_account':         parent.get('Name'),
            'opportunity_name':       r.get('Name'),
            'opportunity_owner':      owner.get('Name'),
            'owner_role':             role.get('Name'),
            'owner_department':       owner.get('Department'),
            'owner_title':            owner.get('Title'),
            'additional_rep':         r.get('Additional_Rep__c'),
            'type':                   r.get('Type'),
            'stage':                  r.get('StageName'),
            'created_date':           clean_date(r.get('CreatedDate')),
            'close_date':             close_date,
            'close_month':            derive_close_month(close_date),
            'last_modified_date':     r.get('LastModifiedDate'),
            'last_activity':          clean_date(r.get('LastActivityDate')),
            'account_last_activity':  clean_date(acc.get('LastActivityDate')),
            'days_since_activity':    clean_int(acc.get('Days_Since_Activity__c')),
            'closed_reason':          r.get('Closed_Reason__c'),
            'description':            r.get('Description'),
            'lead_source':            r.get('LeadSource'),
            'num_locations':          clean_int(r.get('Loc__c')),
            'product_name':           r.get('Product_Interests__c'),
            'industry':               acc.get('Industry'),
            'accounting_package':     acc.get('Accounting_Package__c'),
            'billing_street':         acc.get('BillingStreet'),
            'city':                   acc.get('BillingCity'),
            'state':                  acc.get('BillingState'),
            'zip':                    acc.get('BillingPostalCode'),
            'contact_name':           pc.get('contact_name'),
            'contact_title':          pc.get('contact_title'),
            'contact_phone':          pc.get('contact_phone'),
            'contact_email':          pc.get('contact_email'),
            'asm_flat_rate':          clean_num(r.get('ASM__c')),
            'total_flat_rate':        clean_num(r.get('Total_Flat_Rate__c')),
            'setup_amount':           clean_num(r.get('Setup_Amount__c')),
            'total_setup_amount':     clean_num(r.get('Total_Setup_Amount__c')),
            'total_setup_and_flat':   clean_num(r.get('Total_of_Setup_and_ASM__c')),
            'synced_at':              now_iso(),
        })

    upserted = supabase_upsert('tbl_opportunities', rows)
    print(f"  ✓ {upserted} opportunities upserted to Supabase")
    return len(rows), upserted, rows


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
            'activity_id':         r.get('Id'),
            'account_id':          r.get('AccountId'),
            'fts_id':              acc.get('FTS_ID__c'),
            'account_name':        acc.get('Name'),
            'subject':             r.get('Subject'),
            'type':                r.get('Type'),
            'status':              r.get('Status'),
            'priority':            r.get('Priority'),
            'due_date':            clean_date(r.get('ActivityDate')),
            'created_date':        clean_date(r.get('CreatedDate')),
            'assigned_to':         (r.get('Owner') or {}).get('Name'),
            'comments':            r.get('Description'),
            'industry':            acc.get('Industry'),
            'accounting_package':  acc.get('Accounting_Package__c'),
            'contact_name':        who.get('Name') if (r.get('WhoId') or '').startswith('003') else None,
            'days_since_activity': clean_int(acc.get('Days_Since_Activity__c')),
            'synced_at':           now_iso(),
        })

    upserted = supabase_upsert('tbl_tasks', rows)

    current_ids = [r['activity_id'] for r in rows]
    deleted     = supabase_delete_stale('tbl_tasks', 'activity_id', current_ids)

    print(f"  ✓ {upserted} tasks upserted, {deleted} stale tasks removed")
    return len(rows), upserted, rows


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
    return len(rows), upserted, rows


# ── ACCOUNTS SYNC ─────────────────────────────────────
def sync_accounts(opp_rows, task_rows, activity_rows):
    """
    Build tbl_accounts from the already-fetched opp, task, and activity rows.
    One row per unique account_id. Owners array = all unique reps across all three.
    Rows are never deleted — accounts accumulate permanently once touched.
    Account-level fields sourced from opps (richest source); nulls for task/activity-only accounts.
    """
    print("\nSyncing Accounts...")

    accounts = {}
    today = datetime.now(timezone.utc).date().isoformat()

    # ── Pass 1: seed from opportunities (richest field data) ──
    for r in opp_rows:
        acct_id = r.get('account_id')
        if not acct_id:
            continue
        owner = r.get('opportunity_owner')
        if acct_id not in accounts:
            accounts[acct_id] = {
                'account_id':        acct_id,
                'account_name':      r.get('account_name'),
                'fts_id':            r.get('fts_id'),
                'parent_account_id': r.get('parent_account_id'),
                'parent_account':    r.get('parent_account'),
                'industry':          r.get('industry'),
                'type':              r.get('type'),
                'billing_street':    r.get('billing_street'),
                'billing_city':      r.get('city'),
                'billing_state':     r.get('state'),
                'billing_zip':       r.get('zip'),
                'accounting_package': r.get('accounting_package'),
                'owners':            set(),
                'first_seen_date':   today,
                'synced_at':         now_iso(),
            }
        if owner:
            accounts[acct_id]['owners'].add(owner)
        # Backfill nulls with data from later opp rows for the same account
        for field in ('account_name', 'fts_id', 'industry', 'billing_street',
                      'billing_city', 'billing_state', 'billing_zip', 'accounting_package'):
            if not accounts[acct_id].get(field) and r.get(field):
                accounts[acct_id][field] = r.get(field)

    # ── Pass 2: add accounts from tasks (may not have an opp) ──
    for r in task_rows:
        acct_id = r.get('account_id')
        if not acct_id:
            continue
        owner = r.get('assigned_to')
        if acct_id not in accounts:
            accounts[acct_id] = {
                'account_id':        acct_id,
                'account_name':      r.get('account_name'),
                'fts_id':            r.get('fts_id'),
                'parent_account_id': None,
                'parent_account':    None,
                'industry':          r.get('industry'),
                'type':              None,
                'billing_street':    None,
                'billing_city':      None,
                'billing_state':     None,
                'billing_zip':       None,
                'accounting_package': r.get('accounting_package'),
                'owners':            set(),
                'first_seen_date':   today,
                'synced_at':         now_iso(),
            }
        if owner:
            accounts[acct_id]['owners'].add(owner)

    # ── Pass 3: add accounts from activities ──
    for r in activity_rows:
        acct_id = r.get('account_id')
        if not acct_id:
            continue
        owner = r.get('assigned_to')
        if acct_id not in accounts:
            accounts[acct_id] = {
                'account_id':        acct_id,
                'account_name':      r.get('account_name'),
                'fts_id':            r.get('fts_id'),
                'parent_account_id': None,
                'parent_account':    None,
                'industry':          r.get('industry'),
                'type':              None,
                'billing_street':    None,
                'billing_city':      None,
                'billing_state':     None,
                'billing_zip':       None,
                'accounting_package': r.get('accounting_package'),
                'owners':            set(),
                'first_seen_date':   today,
                'synced_at':         now_iso(),
            }
        if owner:
            accounts[acct_id]['owners'].add(owner)

    # ── Serialize owners set → sorted list for Postgres array ──
    rows = []
    for acct in accounts.values():
        acct['owners'] = sorted(list(acct['owners']))
        rows.append(acct)

    print(f"  Built {len(rows)} unique accounts from opps/tasks/activities")

    # Upsert — never delete stale accounts
    upserted = supabase_upsert('tbl_accounts', rows)
    print(f"  ✓ {upserted} accounts upserted to Supabase (no deletions — accounts accumulate)")
    return len(rows), upserted


# ── MAIN ──────────────────────────────────────────────
def main():
    print("=" * 50)
    print("Premium Team CRM — Salesforce Sync")
    print(f"Started: {now_iso()}")
    print("=" * 50)

    token, instance = sf_login()
    results = {}

    pulled, upserted, opp_rows = sync_opportunities(token, instance)
    results['opportunities'] = {'pulled': pulled, 'upserted': upserted}

    pulled, upserted, task_rows = sync_tasks(token, instance)
    results['tasks'] = {'pulled': pulled, 'upserted': upserted}

    pulled, upserted, activity_rows = sync_activities(token, instance)
    results['activities'] = {'pulled': pulled, 'upserted': upserted}

    pulled, upserted = sync_accounts(opp_rows, task_rows, activity_rows)
    results['accounts'] = {'pulled': pulled, 'upserted': upserted}

    print("\n" + "=" * 50)
    print("Sync Summary:")
    for obj, counts in results.items():
        print(f"  {obj}: {counts['pulled']} pulled, {counts['upserted']} upserted")
    print(f"Finished: {now_iso()}")
    print("=" * 50)

if __name__ == '__main__':
    main()
