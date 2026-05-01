"""
Test script — validates SOQL filter and counts rows before full sync
Run via GitHub Actions manually to check row counts
"""

import os
import sys
import requests

SF_USERNAME  = os.environ['SF_USERNAME']
SF_PASSWORD  = os.environ['SF_PASSWORD']
SF_LOGIN_URL = 'https://login.salesforce.com'

def sf_login():
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
    root = ET.fromstring(res.text)
    ns = {'sf': 'urn:partner.soap.sforce.com'}
    token    = root.find('.//sf:sessionId', ns).text
    instance = root.find('.//sf:serverUrl', ns).text.split('/services')[0]
    print(f"Logged in via SOAP to {instance}")
    return token, instance


def sf_count(token, instance, soql):
    """Run a COUNT() query to get row count without pulling all data."""
    headers = {'Authorization': f'Bearer {token}', 'Content-Type': 'application/json'}
    res = requests.get(
        f"{instance}/services/data/v57.0/query",
        headers=headers,
        params={'q': soql}
    )
    if not res.ok:
        print(f"Query failed ({res.status_code}): {res.text[:500]}")
        return None
    data = res.json()
    return data.get('totalSize', 0)


def sf_query_sample(token, instance, soql, limit=5):
    """Pull a small sample of rows to verify field values."""
    headers = {'Authorization': f'Bearer {token}', 'Content-Type': 'application/json'}
    res = requests.get(
        f"{instance}/services/data/v57.0/query",
        headers=headers,
        params={'q': soql + f' LIMIT {limit}'}
    )
    if not res.ok:
        print(f"Sample query failed: {res.text[:300]}")
        return []
    return res.json().get('records', [])


def main():
    print("=" * 60)
    print("SOQL Filter Test — Premium Team CRM")
    print("=" * 60)

    token, instance = sf_login()

    # ── FILTER LOGIC ──────────────────────────────────────────
    # Matches Salesforce report filters:
    # - Show Me: All opportunities
    # - Close Date: All Time
    # - Opportunity Status: Any
    # - Probability: All
    # - Product Name: does not contain 'Setup' or 'Transfers'
    # - Opportunity Owner: Burke, Adcock, Pottle, Cuellar, Behymer

    owners = "('Burke', 'Adcock', 'Pottle', 'Cuellar', 'Behymer')"

    # Count total raw rows (with duplicates from multiple products)
    count_soql = f"""
        SELECT COUNT()
        FROM OpportunityLineItem
        WHERE Opportunity.IsDeleted = false
        AND Opportunity.Owner.LastName IN {owners}
        AND Name NOT LIKE '%Setup%'
        AND Name NOT LIKE '%Transfer%'
    """
    print("\n1. Counting OpportunityLineItem rows (raw, may have dupes)...")
    raw_count = sf_count(token, instance, count_soql.strip())
    print(f"   Raw row count: {raw_count}")

    # Count unique opportunities matching the filter
    opp_count_soql = f"""
        SELECT COUNT_DISTINCT(Id)
        FROM OpportunityLineItem
        WHERE Opportunity.IsDeleted = false
        AND Opportunity.Owner.LastName IN {owners}
        AND Name NOT LIKE '%Setup%'
        AND Name NOT LIKE '%Transfer%'
    """
    # Note: COUNT_DISTINCT not available in all orgs, use alternative
    opp_alt_soql = f"""
        SELECT COUNT()
        FROM Opportunity
        WHERE IsDeleted = false
        AND Owner.LastName IN {owners}
        AND Id IN (
            SELECT OpportunityId FROM OpportunityLineItem
            WHERE Name NOT LIKE '%Setup%'
            AND Name NOT LIKE '%Transfer%'
        )
    """
    print("\n2. Counting unique Opportunities after product filter...")
    unique_count = sf_count(token, instance, opp_alt_soql.strip())
    print(f"   Unique opportunity count: {unique_count}")

    # Count opportunities without any product filter (baseline)
    baseline_soql = f"""
        SELECT COUNT()
        FROM Opportunity
        WHERE IsDeleted = false
        AND Owner.LastName IN {owners}
    """
    print("\n3. Baseline — all opportunities for these owners (no product filter)...")
    baseline_count = sf_count(token, instance, baseline_soql.strip())
    print(f"   Baseline count: {baseline_count}")

    # Pull a sample to verify field values
    sample_soql = f"""
        SELECT Id, Name, StageName, CloseDate, Owner.Name,
               Account.Name, Account.FTS_ID__c,
               ASM__c, Loc__c
        FROM Opportunity
        WHERE IsDeleted = false
        AND Owner.LastName IN {owners}
    """
    print("\n4. Sample of 5 records (field value check)...")
    samples = sf_query_sample(token, instance, sample_soql.strip())
    for s in samples:
        acc = s.get('Account') or {}
        owner = (s.get('Owner') or {}).get('Name', '?')
        print(f"   - {acc.get('Name','?')} | {s.get('StageName')} | Owner: {owner} | FTS: {acc.get('FTS_ID__c')} | ASM: {s.get('ASM__c')} | Locs: {s.get('Loc__c')}")

    print("\n" + "=" * 60)
    print("Summary:")
    print(f"  Raw rows (with product dupes): {raw_count}")
    print(f"  Unique opportunities:          {unique_count}")
    print(f"  Baseline (no product filter):  {baseline_count}")
    print("=" * 60)


if __name__ == '__main__':
    main()
