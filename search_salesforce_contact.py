"""
search_salesforce_contact.py

One-time pull: find Salesforce Contacts matching a name (or other field),
along with their associated Accounts, and write results into tbl_search
in Supabase. Designed to be re-run for different searches without
overwriting previous results (each run is tagged with a search_label
and run_at timestamp).

Usage (local):
    python search_salesforce_contact.py "Karen Quiroz - Accountant" "Name" "Karen Quiroz"

Usage (GitHub Actions):
    Triggered via workflow_dispatch inputs — see search_salesforce.yml
"""

from simple_salesforce import Salesforce
from supabase import create_client
import os
import sys

# ---- CONFIGURE EACH RUN (via command-line args) ----
SEARCH_LABEL = sys.argv[1]   # free-text label to identify this run later
SEARCH_FIELD = sys.argv[2]   # Salesforce Contact field to search on
SEARCH_VALUE = sys.argv[3]   # value to match (used in a LIKE '%value%')
# -----------------------------

# ---- Salesforce auth (same pattern as sync_salesforce.py) ----
SF_USERNAME = os.environ["SF_USERNAME"]      # tburke@fintech.com
SF_PASSWORD = os.environ["SF_PASSWORD"]      # password + security token concatenated

sf = Salesforce(username=SF_USERNAME, password=SF_PASSWORD)

# ---- Supabase auth ----
SUPABASE_URL = os.environ["SUPABASE_URL"]
SUPABASE_KEY = os.environ["SUPABASE_KEY"]
supabase = create_client(SUPABASE_URL, SUPABASE_KEY)

# ---- Build and run SOQL query ----
soql = f"""
    SELECT Id, Name, Email, Phone, AccountId, Account.Name
    FROM Contact
    WHERE {SEARCH_FIELD} LIKE '%{SEARCH_VALUE}%'
"""

print(f"Running search: {SEARCH_LABEL}")
results = sf.query_all(soql)
records = results.get("records", [])
print(f"Found {len(records)} matching Contact record(s).")

# ---- Transform + insert into tbl_search ----
rows = []
for r in records:
    account = r.get("Account") or {}
    rows.append({
        "search_label": SEARCH_LABEL,
        "search_field": f"Contact.{SEARCH_FIELD}",
        "search_value": SEARCH_VALUE,
        "contact_id": r.get("Id"),
        "contact_name": r.get("Name"),
        "contact_email": r.get("Email"),
        "contact_phone": r.get("Phone"),
        "account_id": r.get("AccountId"),
        "account_name": account.get("Name"),
    })

if rows:
    response = supabase.table("tbl_search").insert(rows).execute()
    print(f"Inserted {len(rows)} row(s) into tbl_search.")
else:
    print("No rows to insert — no matches found.")
