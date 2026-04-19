import json
import os

import gspread
import modal
import requests
from fastapi import HTTPException, Request
from google.oauth2.service_account import Credentials

# ── Modal App ─────────────────────────────────────────────────────────────────
app = modal.App("lead-pipeline")

image = modal.Image.debian_slim(python_version="3.11").pip_install(
    "requests", "gspread", "google-auth", "fastapi[standard]"
)


# ── Helpers ───────────────────────────────────────────────────────────────────
def get_sheets_client(creds_json_str: str):
    creds_info = json.loads(creds_json_str)
    creds = Credentials.from_service_account_info(
        creds_info,
        scopes=[
            "https://spreadsheets.google.com/feeds",
            "https://www.googleapis.com/auth/spreadsheets",
            "https://www.googleapis.com/auth/drive",
        ],
    )
    return gspread.authorize(creds)


def verify_email_reoon(email: str, api_key: str) -> bool:
    """Return True only if Reoon reports safe/valid. On API/network errors, fail closed (False)."""
    try:
        r = requests.get(
            "https://emailverifier.reoon.com/api/v1/verify",
            params={"email": email, "key": api_key, "mode": "quick"},
            timeout=10,
        )
        status = r.json().get("status", "")
        return status in ("safe", "valid")
    except Exception as e:
        print(f"[reoon] error verifying {email}: {e}")
        return False


def _verify_bearer(request: Request) -> None:
    """Require Authorization: Bearer <PIPELINE_AUTH_TOKEN>."""
    token = os.environ.get("PIPELINE_AUTH_TOKEN", "").strip()
    if not token:
        raise HTTPException(
            status_code=500,
            detail="PIPELINE_AUTH_TOKEN is not set in Modal secret lead-pipeline-secrets",
        )
    auth = request.headers.get("Authorization") or ""
    if not auth.startswith("Bearer "):
        raise HTTPException(
            status_code=401,
            detail="Missing Authorization header. Use: Authorization: Bearer <token>",
        )
    provided = auth.removeprefix("Bearer ").strip()
    if not provided or provided != token:
        raise HTTPException(status_code=401, detail="Invalid bearer token")


# ── Core pipeline (callable via .remote() for testing; not exposed as HTTP) ───
@app.function(
    image=image,
    secrets=[modal.Secret.from_name("lead-pipeline-secrets")],
    timeout=600,
)
def execute_pipeline() -> dict:
    reoon_key = os.environ["REOON_API_KEY"]
    sheet_id = os.environ["GOOGLE_SHEET_ID"]

    gc = get_sheets_client(os.environ["GOOGLE_SERVICE_ACCOUNT_JSON"])
    spreadsheet = gc.open_by_key(sheet_id)
    raw_ws = spreadsheet.worksheet("raw")
    leads_ws = spreadsheet.worksheet("Main")

    all_values = raw_ws.get_all_values()
    if not all_values or len(all_values) < 2:
        return {"status": "done", "message": "raw tab is empty — nothing to process"}

    raw_headers = [h.lstrip("\ufeff").strip().strip('"') for h in all_values[0]]
    print(f"[raw] headers: {raw_headers}")

    scraped = []
    for row in all_values[1:]:
        if not any(row):
            continue
        lead = {}
        for i, key in enumerate(raw_headers):
            value = row[i] if i < len(row) else ""
            if key not in lead:
                lead[key] = value
            elif not lead[key] and value:
                lead[key] = value
        scraped.append(lead)

    print(f"[raw] found {len(scraped)} leads")

    existing_rows = leads_ws.get_all_records()
    existing_urls = {
        str(r.get("linkedin", "")).strip().lower()
        for r in existing_rows
        if r.get("linkedin")
    }
    existing_emails = {
        str(r.get("email", "")).strip().lower() for r in existing_rows if r.get("email")
    }

    fresh = []
    for lead in scraped:
        url = lead.get("linkedin", "").strip().lower()
        email = lead.get("email", "").strip().lower()
        if url and url in existing_urls:
            continue
        if email and email in existing_emails:
            continue
        fresh.append(lead)

    print(
        f"[dedup] {len(fresh)} fresh (removed {len(scraped) - len(fresh)} duplicates)"
    )

    pre_verified = all(
        lead.get("status", "").strip().lower() == "safe"
        for lead in fresh
        if lead.get("email")
    )

    verified = []
    if pre_verified and fresh:
        print("[reoon] data already verified — skipping re-check")
        verified = [lead for lead in fresh if lead.get("email", "").strip()]
    else:
        for lead in fresh:
            email = lead.get("email", "").strip()
            if not email:
                print(
                    f"[skip] no email for {lead.get('full_name', 'unknown')} — discarded"
                )
                continue
            if verify_email_reoon(email, reoon_key):
                verified.append(lead)
            else:
                print(f"[reoon] discarded {email}")

    print(f"[reoon] {len(verified)} leads passed verification")

    rows_to_add = []
    for lead in verified:
        rows_to_add.append(
            [
                lead.get("first_name", ""),
                lead.get("full_name", ""),
                lead.get("headline", ""),
                lead.get("company_name", ""),
                lead.get("company_description", ""),
                lead.get("company_size", ""),
                lead.get("industry", ""),
                lead.get("linkedin", ""),
                lead.get("email", ""),
                "safe",
                "",
                "",
                "",
                "",
                "0",
                "",
            ]
        )

    if rows_to_add:
        leads_ws.append_rows(rows_to_add, value_input_option="USER_ENTERED")

    print(f"[sheet] appended {len(rows_to_add)} rows")

    raw_ws.clear()
    print("[raw] cleared")

    return {
        "status": "done",
        "raw_leads": len(scraped),
        "fresh_after_dedup": len(fresh),
        "verified_emails": len(verified),
        "added_to_main": len(rows_to_add),
    }


# ── HTTP entry (Bearer required) ─────────────────────────────────────────────
@app.function(
    image=image,
    secrets=[modal.Secret.from_name("lead-pipeline-secrets")],
    timeout=600,
)
@modal.fastapi_endpoint(method="POST")
def run_pipeline(request: Request):
    _verify_bearer(request)
    return execute_pipeline.local()


if __name__ == "__main__":
    with app.run():
        print(execute_pipeline.remote())
