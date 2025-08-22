from datetime import datetime, timedelta, timezone
import subprocess
import asyncio
import json
from typing import Optional, List, Tuple

from src.services.clorian_service import ClorianService


def _format_dt(dt: datetime) -> str:
    return dt.strftime("%Y%m%d%H%M%S")


def _daily_ranges(start_dt: datetime, end_dt: datetime) -> List[Tuple[datetime, datetime]]:
    """Split [start_dt, end_dt] into <=24h daily windows (00:00:00 → 23:59:59)."""
    ranges: List[Tuple[datetime, datetime]] = []
    cur = start_dt.replace(hour=0, minute=0, second=0, microsecond=0)
    last = end_dt.replace(hour=23, minute=59, second=59, microsecond=0)
    while cur <= last:
        chunk_start = max(start_dt, cur)
        day_end = cur.replace(hour=23, minute=59, second=59, microsecond=0)
        chunk_end = min(end_dt, day_end)
        ranges.append((chunk_start, chunk_end))
        cur = (cur + timedelta(days=1)).replace(hour=0, minute=0, second=0, microsecond=0)
    return ranges


def build_curl_get_bills(account_name: str,
                         start_date: str = "2025-07-01",
                         end_date: Optional[str] = None,
                         simplified: bool = False) -> str:
    """Return a curl command for Clorian bills (normal or simplified) without token.

    - Reads client_id and pos from credentials via ClorianService
    - Formats dates to YYYYMMDDHHMMSS (start at 00:00:00, end at 23:59:59)
    """
    service = ClorianService(account_name)

    # Parse dates
    if start_date:
        start_dt = datetime.strptime(start_date, "%Y-%m-%d").replace(hour=0, minute=0, second=0, microsecond=0)
    else:
        start_dt = datetime.utcnow().replace(hour=0, minute=0, second=0, microsecond=0)

    if end_date:
        end_dt = datetime.strptime(end_date, "%Y-%m-%d").replace(hour=23, minute=59, second=59, microsecond=0)
    else:
        end_dt = datetime.utcnow()

    endpoint = "simplified" if simplified else "normal"

    def _make(s: datetime, e: datetime) -> str:
        url = (
            f"https://services.clorian.com/ws/bills/{endpoint}?clientId={service.clorian_client_id}"
            f"&startDatetime={_format_dt(s)}&endDatetime={_format_dt(e)}&showAnnulationLines=true"
        )
        return (
            f"curl -sS '{url}' "
            f"-H 'Accept: application/json' "
            f"-H 'Authorization: Bearer <TOKEN>' "
            f"-H 'pos: {service.pos}'"
        )

    # If the range exceeds 24h, return one curl per day (newline-separated)
    parts = [
        _make(s, e) for (s, e) in _daily_ranges(start_dt, end_dt)
    ]
    return "\n".join(parts)


def build_curl_get_bills_with_token(account_name: str,
                                    start_date: str = "2025-07-01",
                                    end_date: Optional[str] = None,
                                    simplified: bool = False) -> str:
    """Return a curl command including a fresh Bearer token for the account."""
    import asyncio
    service = ClorianService(account_name)
    asyncio.run(service.refresh_token())

    if start_date:
        start_dt = datetime.strptime(start_date, "%Y-%m-%d").replace(hour=0, minute=0, second=0, microsecond=0)
    else:
        start_dt = datetime.utcnow().replace(hour=0, minute=0, second=0, microsecond=0)

    if end_date:
        end_dt = datetime.strptime(end_date, "%Y-%m-%d").replace(hour=23, minute=59, second=59, microsecond=0)
    else:
        end_dt = datetime.utcnow()

    endpoint = "simplified" if simplified else "normal"

    def _make(s: datetime, e: datetime) -> str:
        url = (
            f"https://services.clorian.com/ws/bills/{endpoint}?clientId={service.clorian_client_id}"
            f"&startDatetime={_format_dt(s)}&endDatetime={_format_dt(e)}&showAnnulationLines=true"
        )
        return (
            f"curl -sS '{url}' "
            f"-H 'Accept: application/json' "
            f"-H 'Authorization: Bearer {service.access_token}' "
            f"-H 'pos: {service.pos}'"
        )

    parts = [
        _make(s, e) for (s, e) in _daily_ranges(start_dt, end_dt)
    ]
    return "\n".join(parts)


def build_curl_get_bills_last_hours(account_name: str,
                                    hours: int = 23,
                                    simplified: bool = False) -> str:
    """Return a curl command for Clorian bills covering the last N hours (default 23h).

    Uses current UTC time as end and end - hours as start.
    """
    service = ClorianService(account_name)

    end_dt = datetime.now(timezone.utc)
    start_dt = end_dt - timedelta(hours=hours)

    endpoint = "simplified" if simplified else "normal"
    url = (
        f"https://services.clorian.com/ws/bills/{endpoint}?clientId={service.clorian_client_id}"
        f"&startDatetime={_format_dt(start_dt)}&endDatetime={_format_dt(end_dt)}&showAnnulationLines=true"
    )

    curl = (
        f"curl -sS '{url}' "
        f"-H 'Accept: application/json' "
        f"-H 'Authorization: Bearer <TOKEN>' "
        f"-H 'pos: {service.pos}'"
    )
    return curl


def build_curl_get_bills_with_token_last_hours(account_name: str,
                                               hours: int = 23,
                                               simplified: bool = False) -> str:
    """Return a curl including a fresh Bearer token for the last N hours (default 23h)."""
    import asyncio
    service = ClorianService(account_name)
    asyncio.run(service.refresh_token())

    end_dt = datetime.now(timezone.utc)
    start_dt = end_dt - timedelta(hours=hours)

    endpoint = "simplified" if simplified else "normal"
    url = (
        f"https://services.clorian.com/ws/bills/{endpoint}?clientId={service.clorian_client_id}"
        f"&startDatetime={_format_dt(start_dt)}&endDatetime={_format_dt(end_dt)}&showAnnulationLines=true"
    )

    curl = (
        f"curl -sS '{url}' "
        f"-H 'Accept: application/json' "
        f"-H 'Authorization: Bearer {service.access_token}' "
        f"-H 'pos: {service.pos}'"
    )
    return curl


def fetch_bills_last_hours(account_name: str,
                           hours: int = 23,
                           simplified: bool = False) -> list:
    """Fetch bills from Clorian for the last N hours (default 23h) and return them as a list."""

    async def _run() -> list:
        service = ClorianService(account_name)
        end_dt = datetime.utcnow()
        start_dt = end_dt - timedelta(hours=hours)

        if simplified:
            # simplified bills endpoint
            return await service.get_bills(days_back=0, start_date=start_dt, end_date=end_dt, concurrency=5)
        else:
            # normal bills endpoint
            return await service.get_bills_v2(days_back=0, start_date=start_dt, end_date=end_dt, concurrency=5)

    return asyncio.run(_run())


def print_bills_last_hours(account_name: str,
                           hours: int = 23,
                           simplified: bool = False) -> None:
    """Fetch and print bills for the last N hours in pretty JSON."""
    bills = fetch_bills_last_hours(account_name, hours=hours, simplified=simplified)
    print(json.dumps(bills, indent=2, ensure_ascii=False))


def run_curl(curl_cmd: str, pretty: bool = True) -> int:
    """Run a curl command and print the result to the terminal.

    If pretty=True, pretty-print JSON using Python's json.tool.
    Returns the process return code.
    """
    cmd = curl_cmd + (" | python -m json.tool" if pretty else "")
    return subprocess.run(cmd, shell=True, check=False).returncode


def print_curl_last_hours_via_curl(account_name: str,
                                   hours: int = 23,
                                   simplified: bool = False,
                                   with_token: bool = True,
                                   pretty: bool = True) -> int:
    """Build the curl for the last N hours and execute it, printing the output.

    - with_token=True: uses a fresh Bearer token
    - pretty=True: pretty-prints JSON
    Returns the process return code.
    """
    if with_token:
        curl_cmd = build_curl_get_bills_with_token_last_hours(account_name, hours=hours, simplified=simplified)
    else:
        curl_cmd = build_curl_get_bills_last_hours(account_name, hours=hours, simplified=simplified)
    return run_curl(curl_cmd, pretty=pretty)


def print_curl_command_last_hours(account_name: str,
                                  hours: int = 23,
                                  simplified: bool = False,
                                  with_token: bool = True) -> None:
    """Print the curl command for the last N hours without executing it."""
    if with_token:
        cmd = build_curl_get_bills_with_token_last_hours(account_name, hours=hours, simplified=simplified)
    else:
        cmd = build_curl_get_bills_last_hours(account_name, hours=hours, simplified=simplified)
    print(cmd)


def fetch_all_bills_since_date(account_name: str,
                               start_date: str = "2025-07-01",
                               simplified: bool = False,
                               concurrency: int = 10) -> list:
    """Fetch all bills since `start_date` up to now using the Python client (auto-slices by day)."""
    async def _run() -> list:
        service = ClorianService(account_name)
        end_dt = datetime.utcnow()
        if simplified:
            return await service.get_bills(days_back=0, start_date=start_date, end_date=end_dt, concurrency=concurrency)
        return await service.get_bills_v2(days_back=0, start_date=start_date, end_date=end_dt, concurrency=concurrency)

    return asyncio.run(_run())


def fetch_and_save_bills_since_date(account_name: str,
                                    start_date: str = "2025-07-01",
                                    simplified: bool = False,
                                    out_file: Optional[str] = None,
                                    concurrency: int = 10) -> str:
    """Fetch all bills since `start_date` and save them to JSON. Returns the output file path."""
    bills = fetch_all_bills_since_date(account_name, start_date=start_date, simplified=simplified, concurrency=concurrency)
    if out_file is None:
        out_file = "simplified_bills.json" if simplified else "normal_bills.json"
    with open(out_file, "w") as f:
        json.dump(bills, f, indent=2, ensure_ascii=False)
    print(f"Saved {len(bills)} bills to {out_file}")
    return out_file


if __name__ == "__main__":
    # Obtener y guardar TODAS las facturas normales desde 2025-07-01
    a = fetch_and_save_bills_since_date("Cloarian Account II", start_date="2025-07-01", simplified=True, concurrency=20)
    # print(a)
    
    # Si necesitas simplified, descomenta:
    # fetch_and_save_bills_since_date("Cloarian Account II", start_date="2025-07-01", simplified=True, concurrency=20)


