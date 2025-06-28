import json
import os

"""
 Holded API documentation: https://developers.holded.com/reference/documents
 Colorian API documentation: https://thirdparty.clorian.com/
    Username: clorian_doc
    Password: 8d4AyJAc
"""

CREDENTIALS_FILE = os.path.join(os.path.dirname(__file__), "credentials.json")

def load_credentials() -> dict:
    with open(CREDENTIALS_FILE, "r") as f:
        return json.load(f)

def save_credentials(data: dict) -> None:
    with open(CREDENTIALS_FILE, "w") as f:
        json.dump(data, f, indent=4)

credentials = load_credentials()
CLORIAN_ACCOUNTS = credentials["clorian_accounts"]
HOLDED_API_KEY   = credentials["holded"]["api_key"]

"""CLORIAN ACCOUNTS HELPERS"""
# TOKEN HELPERS 
def update_auth_token( clorian_account: str, new_token: str) -> None:
    for acc in CLORIAN_ACCOUNTS:
        if acc.get("name", "").lower() == clorian_account.lower():
            acc["auth_token"] = new_token                    
            save_credentials(credentials)
            return
    raise ValueError(f"Clorian account '{clorian_account}' not found")

def get_auth_token(clorian_account: str) -> str:
    for acc in CLORIAN_ACCOUNTS:
        if acc.get("name", "").lower() == clorian_account.lower():
            return acc.get("auth_token") or acc.get("refresh_token", "")
    return ""


def update_refresh_token(clorian_account: str, new_token: str) -> None:
    """Persist a new long-lived refresh token."""
    for acc in CLORIAN_ACCOUNTS:
        if acc.get("name") == clorian_account:
            acc["refresh_token"] = new_token
            save_credentials(credentials)
            return
    raise ValueError(f"Clorian account '{clorian_account}' not found")


def get_refresh_token(clorian_account: str) -> str:
    """Retrieve the current long-lived refresh token."""
    for acc in CLORIAN_ACCOUNTS:
        if acc.get("name") == clorian_account:
            return acc.get("refresh_token", "")
    return ""

# OFFSET HELPERS
def set_offset(clorian_account: str, offset: int, account_type: str = "general") -> None:
    for acc in CLORIAN_ACCOUNTS:
        if acc["name"] == clorian_account and account_type in acc["cuentas_a_migrar"]:
            idx = acc["cuentas_a_migrar"].index(account_type)
            acc["offset_cuentas_a_migrar"][idx] = offset
            return

def increment_offset(clorian_account: str, account_type: str = "general", persist: bool = False) -> int:
    """
    Increments the offset and returns the new value.
    If persist=True, writes the updated credentials back to disk.
    """
    for acc in CLORIAN_ACCOUNTS:
        if acc["name"] == clorian_account and account_type in acc["cuentas_a_migrar"]:
            idx = acc["cuentas_a_migrar"].index(account_type)
            acc["offset_cuentas_a_migrar"][idx] += 1
            new_val = acc["offset_cuentas_a_migrar"][idx]
            if persist:
                save_credentials(credentials)
            return new_val
    return 0  # account or type not found

def get_offset(clorian_account: str, account_type: str = "general") -> int:
    for acc in CLORIAN_ACCOUNTS:
        if acc["name"] == clorian_account and account_type in acc["cuentas_a_migrar"]:
            idx = acc["cuentas_a_migrar"].index(account_type)
            return acc["offset_cuentas_a_migrar"][idx]
    return 0



if __name__ == "__main__":
    w = get_auth_token("Clorian Flamenco Granada"); print(w)
    # a = update_auth_token("Clorian Flamenco Granada", "puercoa")