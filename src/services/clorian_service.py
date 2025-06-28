import asyncio
import aiohttp
from datetime import datetime
import json
import base64
import time
import re
import unicodedata
from urllib.parse import quote_plus

from src.config.settings import update_auth_token, get_auth_token, update_refresh_token, get_refresh_token

AUTH_HEADER = "Basic " + base64.b64encode(
    b"third-party:dGhpcmRQYXJ0eVBhc3M="
).decode()

class ClorianService:
    def __init__(self, config: dict):
        self.name = config.get("name", "Clorian Service")
        self.username = config.get("username")
        self.password = config.get("password")
        self._refresh_token = config.get("refresh_token")
        self.base_url = "https://api.clorian.com"


        self.client_id = "third-party"

    # RENEW TOKENS
    async def refresh_token(self) -> str:
        url = "https://services.clorian.com/user/oauth/token"
        headers = {
            "Authorization": AUTH_HEADER,
            "Content-Type": "application/x-www-form-urlencoded",
        }

        # try refresh-token first
        data = {"grant_type": "refresh_token", "refresh_token": self._refresh_token} if self._refresh_token else None
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

        # Save tokens
        update_auth_token(self.name, self.access_token)
        if self._refresh_token:
            update_refresh_token(self.name, self._refresh_token)
   

    # INVOICE OPERATIONS (BILLS)
    async def get_invoices(self, ):
        """In the context of Invoice in Clorian is 'Bills', they both include Invoices and Client data"""
        pass


    # PRODUCTS OPERATIONS
    async def get_products(self) -> list:
        """Get product master data"""
        if not getattr(self, "access_token", None) or time.time() >= getattr(self, "expires_at", 0):
            await self.refresh_token()

        client_id = 107
        pos= 2372

        url = f"https://services.clorian.com/ws/masters/products?clientId={client_id}"

        headers = {
            "Accept": "application/json",
            "Authorization": f"Bearer {self.access_token}",
            "Accept-Language": "es",
            "pos": str(pos),
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


    # TESTING OPERATIONS
    async def get_purchases(self) -> list:
        """Get all purchase data"""
        if not getattr(self, "access_token", None) or time.time() >= getattr(self, "expires_at", 0):
            await self.refresh_token()

        cid = 107
        pos = 2372

        from datetime import timedelta

        start_dt = datetime(2000, 1, 1)
        now_dt   = datetime.utcnow()
        step     = timedelta(days=90)

        headers_base = {
            "Accept": "application/json",
            "Accept-Language": "es",
            "pos": str(pos),
        }

        out = []
        async with aiohttp.ClientSession() as s:
            cur = start_dt
            while cur <= now_dt:
                end_dt = min(cur + step - timedelta(seconds=1), now_dt)
                url = (
                    "https://services.clorian.com/ws/purchases"
                    f"?clientId={cid}"
                    f"&startDatetime={cur.strftime('%Y%m%d%H%M%S')}"
                    f"&endDatetime={end_dt.strftime('%Y%m%d%H%M%S')}"
                )
                headers = headers_base | {"Authorization": f"Bearer {self.access_token}"}

                async with s.get(url, headers=headers) as r:
                    if r.status == 401:
                        await self.refresh_token()
                        headers["Authorization"] = f"Bearer {self.access_token}"
                        async with s.get(url, headers=headers) as r2:
                            if r2.status == 200:
                                out.extend(await r2.json())
                    elif r.status == 200:
                        out.extend(await r.json())

                cur = end_dt + timedelta(seconds=1)

        return out


async def main_testing():

    config = {"name": "Clorian Flamenco Granada", "username": "integration-holded@flamencogranada.com", "password": "3BdH649KT4d1T5JETlaj?", "refresh_token": "sddsdsd", "cuentas_a_migrar": [ "general" ], "offset_cuentas_a_migrar": [ 0 ] } 
    clorian = ClorianService(config)
    await clorian.refresh_token()
    # products = await clorian.get_purchases()

    """
    for product in products:
        print(product)
        break

    print(len(products))
    """

if __name__ == "__main__":
    asyncio.run(main_testing())