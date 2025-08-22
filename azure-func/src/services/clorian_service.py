import asyncio
import aiohttp
import logging
from datetime import datetime, timedelta
from typing import Optional, Union, List
import time
import json
import base64
import time
import re
import unicodedata
from urllib.parse import quote_plus
from aiohttp.client_exceptions import ClientConnectorError, ClientConnectorDNSError

from src.config.settings import update_auth_token, get_auth_token, update_refresh_token, get_refresh_token, get_clorian_account

AUTH_HEADER = "Basic " + base64.b64encode(
    b"third-party:dGhpcmRQYXJ0eVBhc3M="
).decode()

# Configure logging
logger = logging.getLogger(__name__)

class ClorianService:
    def __init__(self, clorian_account: str):
        config = get_clorian_account(clorian_account)

        self.name = config.get("name", "Clorian Service")
        self.username = config.get("username")
        self.password = config.get("password")
        self._refresh_token = config.get("refresh_token")
        self.base_url = "https://api.clorian.com"

        # OAuth client id used for token exchange (fixed per third-party integration)
        self.client_id = "third-party"

        # Business parameters coming from credentials.json
        self.clorian_client_id: int | None = config.get("client_id")
        self.pos: int | None = config.get("pos") or config.get("pos_id")

        if not self.clorian_client_id or not self.pos:
            raise ValueError(
                f"Missing 'client_id' or 'pos' for Clorian account '{self.name}' in credentials.json"
            )

    # RENEW TOKENS
    async def refresh_token(self) -> str:
        logger.debug(f"🔑 Refreshing token for Clorian account: {self.name}")
        url = "https://services.clorian.com/user/oauth/token"
        headers = {
            "Authorization": AUTH_HEADER,
            "Content-Type": "application/x-www-form-urlencoded",
        }

        # try refresh-token first
        data = {"grant_type": "refresh_token", "refresh_token": self._refresh_token} if self._refresh_token else None
        auth_method = "refresh_token" if data else "password"
        logger.debug(f"🔐 Using authentication method: {auth_method} for {self.name}")
        async with aiohttp.ClientSession() as s:
            if data:
                r = await s.post(url, headers=headers, data=data)
                if r.status in (400, 401):
                    data = None
                else:
                    r.raise_for_status()
                    j = await r.json()
            if not data:
                data = {"grant_type": "password", "username": self.username, "password": self.password}
                r = await s.post(url, headers=headers, data=data)
                r.raise_for_status()
                j = await r.json()

        # Set variables of token saving
        self.access_token   = j["access_token"]
        self._refresh_token = j.get("refresh_token", "")
        self.expires_at     = time.time() + j.get("expires_in", 3600) - 30

        # Update POS from token (posAllowed) if provided
        try:
            pos_allowed = j.get("posAllowed") or []
            if pos_allowed:
                # prefer configured POS if it is allowed; otherwise take the first allowed
                configured_pos = str(self.pos) if getattr(self, "pos", None) is not None else None
                if configured_pos and configured_pos in pos_allowed:
                    selected_pos = configured_pos
                else:
                    selected_pos = str(pos_allowed[0])

                # store as int when possible
                try:
                    self.pos = int(selected_pos)
                except (TypeError, ValueError):
                    self.pos = selected_pos

                logger.debug(f"✅ Using POS from token posAllowed: {self.pos}")
        except Exception as e:
            logger.warning(f"⚠️  Could not set POS from token posAllowed: {e}")

        logger.debug(f"✅ Successfully obtained new tokens for {self.name}")
        logger.debug(f"🔑 Access token expires in {j.get('expires_in', 3600)} seconds")

        # Save tokens (may fail in Azure Functions due to read-only filesystem)
        try:
            logger.debug(f"💾 Attempting to persist tokens for {self.name}")
            update_auth_token(self.name, self.access_token)
            if self._refresh_token:
                update_refresh_token(self.name, self._refresh_token)
            logger.debug(f"✅ Tokens persisted successfully for {self.name}")
        except Exception as e:
            logger.warning(f"⚠️  Could not persist tokens for {self.name}: {e}")
            logger.info(f"🔄 {self.name} tokens will be refreshed on next execution")
   

    # BILLS OPERATIONS
    async def get_bills(self, days_back: int = 365, *, start_date: Optional[Union[datetime, str]] = None, end_date:   Optional[Union[datetime, str]] = None, concurrency: int = 10):
        """
        Fetch simplified bills (/ws/bills/simplified).
        """
        # RETRIVE ACCES TOKEN
        if not getattr(self, "access_token", None) or time.time() >= getattr(self, "expires_at", 0):
            await self.refresh_token()

        # Use account-specific clientId and POS from credentials

        # compute date range
        if end_date is None:
            utc_end = datetime.utcnow()
        elif isinstance(end_date, str):
            utc_end = datetime.strptime(end_date, "%Y-%m-%d")
        else:
            utc_end = end_date

        if start_date is None:
            utc_from = utc_end - timedelta(days=days_back)
        elif isinstance(start_date, str):
            utc_from = datetime.strptime(start_date, "%Y-%m-%d")
        else:
            utc_from = start_date

        utc_end  = utc_end.replace(hour=23, minute=59, second=59, microsecond=0)
        utc_from = utc_from.replace(hour=0,  minute=0, second=0, microsecond=0)

        # 24-hour slices
        one_day = timedelta(days=1)
        windows = []
        cursor  = utc_from.replace(hour=0, minute=0, second=0, microsecond=0)
        idx     = 0
        while cursor <= utc_end:
            start = cursor.strftime("%Y%m%d%H%M%S")
            end   = min(cursor + one_day - timedelta(seconds=1), utc_end)
            end   = end.strftime("%Y%m%d%H%M%S")
            windows.append((idx, start, end))
            cursor += one_day
            idx += 1

        sem   = asyncio.Semaphore(concurrency)
        bills = []

        async def fetch_slice(session, index, start_s, end_s):
            url = (
                f"https://services.clorian.com/ws/bills/simplified"
                f"?clientId={self.clorian_client_id}&startDatetime={start_s}&endDatetime={end_s}"
                f"&showAnnulationLines=true"
            )
            headers = {
                "Accept": "application/json",
                "Authorization": f"Bearer {self.access_token}",
                "pos": str(self.pos),
            }

            for attempt in (1, 2):
                try:
                    async with sem, session.get(url, headers=headers) as r:
                        if r.status == 401:
                            await self.refresh_token()
                            headers["Authorization"] = f"Bearer {self.access_token}"
                            continue
                        if r.status == 200:
                            return index, (await r.json() or [])
                        return index, []
                except (ClientConnectorError, ClientConnectorDNSError):
                    if attempt == 1:
                        await asyncio.sleep(2)
                    else:
                        print(f"[WARN] network error on {start_s}-{end_s}")
            return index, []

        # parallel fetch
        connector = aiohttp.TCPConnector(limit_per_host=concurrency)
        async with aiohttp.ClientSession(connector=connector) as sess:
            results = await asyncio.gather(*(fetch_slice(sess, *w) for w in windows))

        # keep chronological order
        results.sort(key=lambda t: t[0])
        ordered = [bill for _, chunk in results for bill in chunk]
        return ordered

    async def get_bills_v2(self, days_back: int = 365, *, start_date: Optional[Union[datetime, str]] = None, end_date:   Optional[Union[datetime, str]] = None, concurrency: int = 10):
        """
        Fetch normal bills (/ws/bills/normal).
        """
        # Ensure valid access token
        if not getattr(self, "access_token", None) or time.time() >= getattr(self, "expires_at", 0):
            await self.refresh_token()

        # Use account-specific clientId and POS from credentials

        # Compute date range
        if end_date is None:
            utc_end = datetime.utcnow()
        elif isinstance(end_date, str):
            utc_end = datetime.strptime(end_date, "%Y-%m-%d")
        else:
            utc_end = end_date

        if start_date is None:
            utc_from = utc_end - timedelta(days=days_back)
        elif isinstance(start_date, str):
            utc_from = datetime.strptime(start_date, "%Y-%m-%d")
        else:
            utc_from = start_date

        utc_end  = utc_end.replace(hour=23, minute=59, second=59, microsecond=0)
        utc_from = utc_from.replace(hour=0,  minute=0, second=0, microsecond=0)

        # Build 24-hour windows
        one_day = timedelta(days=1)
        windows = []
        cursor  = utc_from.replace(hour=0, minute=0, second=0, microsecond=0)
        idx     = 0
        while cursor <= utc_end:
            start = cursor.strftime("%Y%m%d%H%M%S")
            end   = min(cursor + one_day - timedelta(seconds=1), utc_end)
            end   = end.strftime("%Y%m%d%H%M%S")
            windows.append((idx, start, end))
            cursor += one_day
            idx += 1

        sem   = asyncio.Semaphore(concurrency)
        bills = []

        async def fetch_slice(session, index, start_s, end_s):
            url = (
                f"https://services.clorian.com/ws/bills/normal"
                f"?clientId={self.clorian_client_id}&startDatetime={start_s}&endDatetime={end_s}"
                f"&showAnnulationLines=true"
            )
            headers = {
                "Accept": "application/json",
                "Authorization": f"Bearer {self.access_token}",
                "pos": str(self.pos),
            }

            for attempt in (1, 2):
                try:
                    async with sem, session.get(url, headers=headers) as r:
                        if r.status == 401:
                            await self.refresh_token()
                            headers["Authorization"] = f"Bearer {self.access_token}"
                            continue
                        if r.status == 200:
                            return index, (await r.json() or [])
                        return index, []
                except (ClientConnectorError, ClientConnectorDNSError):
                    if attempt == 1:
                        await asyncio.sleep(2)
                    else:
                        print(f"[WARN] network error on {start_s}-{end_s}")
            return index, []

        # Parallel fetch
        connector = aiohttp.TCPConnector(limit_per_host=concurrency)
        async with aiohttp.ClientSession(connector=connector) as sess:
            results = await asyncio.gather(*(fetch_slice(sess, *w) for w in windows))

        # Keep chronological order
        results.sort(key=lambda t: t[0])
        ordered = [bill for _, chunk in results for bill in chunk]
        return ordered


    async def get_bill_by_id(self, bill_id: int, show_annulations: bool = True) -> List[dict]:
        """Retrieve one ordinary bill by its identifier"""

        # ACCES TOKEN
        if not getattr(self, "access_token", None) or time.time() >= getattr(self, "expires_at", 0):
            await self.refresh_token()

        # Use account-specific clientId and POS from credentials
        base_url = "https://services.clorian.com/ws/bills/normal"
        url = (
            f"{base_url}?clientId={self.clorian_client_id}"
            f"&billId={bill_id}"
            f"{'&showAnnulationLines=true' if show_annulations else ''}"
        )

        headers = {
            "Accept": "application/json",
            "Authorization": f"Bearer {self.access_token}",
            "pos": str(self.pos),
        }

        # single request, retry once on token expiry
        for attempt in (1, 2):
            try:
                async with aiohttp.ClientSession() as session:
                    async with session.get(url, headers=headers) as resp:
                        if resp.status == 401 and attempt == 1:
                            await self.refresh_token()
                            headers["Authorization"] = f"Bearer {self.access_token}"
                            continue                    # retry once
                        if resp.status == 200:
                            return await resp.json()
                        if resp.status == 404:
                            return None                 # bill not found
                        raise RuntimeError(f"get_bill_by_id failed {resp.status}: {await resp.text()}")
            except (ClientConnectorError, ClientConnectorDNSError) as e:
                raise RuntimeError(f"Network error while fetching bill {bill_id}: {e}") from e

    # PRODUCTS OPERATIONS
    async def get_products(self) -> list:
        """Get product master data"""
        if not getattr(self, "access_token", None) or time.time() >= getattr(self, "expires_at", 0):
            await self.refresh_token()

        url = f"https://services.clorian.com/ws/masters/products?clientId={self.clorian_client_id}"

        headers = {
            "Accept": "application/json",
            "Authorization": f"Bearer {self.access_token}",
            "Accept-Language": "es",
            "pos": str(self.pos),
        }

        async with aiohttp.ClientSession() as s:
            async with s.get(url, headers=headers) as r:
                if r.status == 200:
                    return await r.json()
                if r.status == 401:
                    await self.refresh_token()
                    headers["Authorization"] = f"Bearer {self.access_token}"
                    async with s.get(url, headers=headers) as r2:
                        return await r2.json() if r2.status == 200 else []
                return []

    # OTHER OPERATIONS
    async def get_payment(self, payment_id: int) -> dict:
        pass

    async def get_payments():
        pass

    # TESTING OPERATIONS
    async def get_purchases(self, days_back: int = 5, concurrency: int = 10) -> list:
        """Fetch every Clorian purchase from `days_back` days ago up to now."""
        if not getattr(self, "access_token", None) or time.time() >= getattr(self, "expires_at", 0):
            await self.refresh_token()

        lang = "es"
        end_dt   = datetime.utcnow()
        start_dt = end_dt - timedelta(days=days_back)

        # build 24-h windows
        one_day = timedelta(days=1)
        ranges  = []
        cur = start_dt.replace(hour=0, minute=0, second=0, microsecond=0)
        while cur <= end_dt:
            ranges.append((
                cur.strftime("%Y%m%d%H%M%S"),
                (cur + one_day - timedelta(seconds=1)
                if cur + one_day <= end_dt else end_dt).strftime("%Y%m%d%H%M%S")
            ))
            cur += one_day

        sem = asyncio.Semaphore(concurrency)
        purchases: list = []

        async def fetch_range(session: aiohttp.ClientSession, start_str: str, end_str: str):
            url = (f"https://services.clorian.com/ws/purchases"
                f"?clientId={self.clorian_client_id}&startDatetime={start_str}&endDatetime={end_str}")
            headers = {
                "Accept": "application/json",
                "Authorization": f"Bearer {self.access_token}",
                "pos": str(self.pos),
                "Accept-Language": lang,
            }

            for attempt in (1, 2):  # one retry on DNS/network error
                try:
                    async with sem, session.get(url, headers=headers) as r:
                        if r.status == 401:
                            await self.refresh_token()
                            headers["Authorization"] = f"Bearer {self.access_token}"
                            continue  # retry same URL with fresh token
                        if r.status == 200:
                            purchases.extend(await r.json() or [])
                        return
                except (ClientConnectorError, ClientConnectorDNSError):
                    if attempt == 1:
                        await asyncio.sleep(2)
                    else:
                        print(f"[WARN] skipped {start_str}-{end_str}: DNS/connect error")

        # ---- single shared session ----------------------------------------------
        connector = aiohttp.TCPConnector(limit_per_host=concurrency)
        async with aiohttp.ClientSession(connector=connector) as session:
            await asyncio.gather(*(fetch_range(session, s, e) for s, e in ranges))

        return purchases


async def main_testing():

    clorian = ClorianService("Clorian Flamenco Granada")
    await clorian.refresh_token()

    print(clorian.access_token)
  

    bills = await clorian.get_bills(days_back=365, start_date="2025-07-01")
   
    print("Bills:", len(bills))

    with open("simplified_bills.json", "w") as f:
        json.dump(bills, f, indent=2, ensure_ascii=False)

    
if __name__ == "__main__":
    asyncio.run(main_testing())