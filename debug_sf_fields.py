"""
Debug script — prints raw Salesforce Opportunity field names
Run this once to confirm correct API field names
"""

import os
import sys
import json
import requests

SF_USERNAME     = os.environ['SF_USERNAME']
SF_PASSWORD     = os.environ['SF_PASSWORD']
SF_INSTANCE_URL = os.environ['SF_INSTANCE_URL']
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
    root = ET.fromstring(res.text)
    ns = {'sf': 'urn:partner.soap.sforce.com'}
    token    = root.find('.//sf:sessionId', ns).text
    instance = root.find('.//sf:serverUrl', ns).text.split('/services')[0]
    return token, instance

def main():
    print("Logging in to Salesforce...")
    token, instance = sf_login()
    print(f"Connected to {instance}")

    # Get field metadata for Opportunity object
    headers = {'Authorization': f'Bearer {token}', 'Content-Type': 'application/json'}
    res = requests.get(f"{instance}/services/data/v57.0/sobjects/Opportunity/describe", headers=headers)
    
    if not res.ok:
        print(f"Failed to get metadata: {res.text}")
        sys.exit(1)

    fields = res.json().get('fields', [])
    
    # Print all custom fields (ending in __c) and key standard fields
    print("\n=== CUSTOM FIELDS ON OPPORTUNITY ===")
    custom = [f for f in fields if f['name'].endswith('__c')]
    for f in sorted(custom, key=lambda x: x['name']):
        print(f"  API Name: {f['name']:<40} Label: {f['label']}")

    print("\n=== KEY STANDARD FIELDS ===")
    key_standard = ['Id', 'AccountId', 'Name', 'StageName', 'CloseDate', 
                    'CreatedDate', 'OwnerId', 'LeadSource', 'Description']
    for f in fields:
        if f['name'] in key_standard:
            print(f"  API Name: {f['name']:<40} Label: {f['label']}")

if __name__ == '__main__':
    main()
