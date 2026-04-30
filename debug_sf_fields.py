"""
Debug script — prints Salesforce Account custom fields related to FTS/Activity/Accounting
"""

import os
import sys
import requests

SF_USERNAME     = os.environ['SF_USERNAME']
SF_PASSWORD     = os.environ['SF_PASSWORD']
SF_LOGIN_URL    = 'https://login.salesforce.com'

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
    return token, instance

def describe_object(token, instance, obj_name, keywords):
    headers = {'Authorization': f'Bearer {token}', 'Content-Type': 'application/json'}
    res = requests.get(f"{instance}/services/data/v57.0/sobjects/{obj_name}/describe", headers=headers)
    if not res.ok:
        print(f"Failed to describe {obj_name}: {res.text}")
        return
    fields = res.json().get('fields', [])
    print(f"\n=== {obj_name} CUSTOM FIELDS (filtered) ===")
    custom = [f for f in fields if f['name'].endswith('__c') and
              any(k in f['name'].lower() or k in f['label'].lower() for k in keywords)]
    for f in sorted(custom, key=lambda x: x['name']):
        print(f"  API Name: {f['name']:<50} Label: {f['label']}")
    if not custom:
        print("  No matching fields found — printing ALL custom fields:")
        for f in sorted([f for f in fields if f['name'].endswith('__c')], key=lambda x: x['name']):
            print(f"  API Name: {f['name']:<50} Label: {f['label']}")

def main():
    print("Logging in...")
    token, instance = sf_login()
    print(f"Connected to {instance}")

    # Account fields — looking for FTS ID, Days Since Activity, Accounting Package
    describe_object(token, instance, 'Account', 
        ['fts', 'days', 'activity', 'accounting', 'package', 'custom'])

if __name__ == '__main__':
    main()
