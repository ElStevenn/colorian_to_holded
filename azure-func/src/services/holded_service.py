import aiohttp
import asyncio
import json
import base64
import calendar
import os
from urllib.parse import quote_plus
from datetime import datetime, timedelta
import random
from aiohttp import ClientConnectorError, ClientTimeout, ClientError, ServerTimeoutError

from src.config.settings import HOLDED_API_KEY

TRANSIENT = {502, 503, 504}

class HoldedService:
    def __init__(self):
        self.api_key = HOLDED_API_KEY
        self.base_url = "https://api.holded.com/api"
        self.headers = {
            "Accept": "application/json",
            "Key": self.api_key,
        }
        # Reusable HTTP session with sane defaults
        self._session: aiohttp.ClientSession | None = None
        self._client_timeout = ClientTimeout(total=30, connect=10, sock_connect=10, sock_read=20)

    def _ts(self, dt: datetime) -> int:
        """UTC → unix-timestamp (int)."""
        return calendar.timegm(dt.timetuple())

    def _get_session(self) -> aiohttp.ClientSession:
        if self._session is None or self._session.closed:
            connector = aiohttp.TCPConnector(limit=12, ttl_dns_cache=300)
            self._session = aiohttp.ClientSession(
                headers=self.headers,
                timeout=self._client_timeout,
                connector=connector,
            )
        return self._session

    async def _get(self, url: str, *, max_tries: int = 4) -> aiohttp.ClientResponse:
        backoff = 1.5
        for attempt in range(1, max_tries + 1):
            try:
                sess = self._get_session()
                resp = await sess.get(url)
                if resp.status in TRANSIENT:
                    raise RuntimeError(f"Holded {resp.status}")
                await resp.read()          # ⬅️ descarga y deja el body cacheado
                return resp
            except (RuntimeError, ClientConnectorError, asyncio.TimeoutError, ServerTimeoutError, ClientError):
                if attempt == max_tries:
                    raise
                await asyncio.sleep(backoff + random.random())
                backoff *= 2
        
                
    async def _post(self, url: str, payload: dict, *, max_tries: int = 4):
        backoff = 1.5
        for attempt in range(1, max_tries + 1):
            try:
                sess = self._get_session()
                resp = await sess.post(url, json=payload)
                if resp.status in TRANSIENT:
                    raise RuntimeError(f"Holded {resp.status}")
                return resp
            except (RuntimeError, ClientConnectorError, asyncio.TimeoutError, ServerTimeoutError, ClientError):
                if attempt == max_tries:
                    raise
                await asyncio.sleep(backoff + random.random())
                backoff *= 2

    # IVOICE OPERATIONS
    async def list_contacts(self, *, page_size: int = 200) -> list[dict]:
        """
        Return **every** contact stored in Holded – no silent cut-offs.
        """
        base_url = f"{self.base_url}/invoicing/v1/contacts"
        all_contacts: list[dict] = []

        async with aiohttp.ClientSession(headers=self.headers) as sess:
            page = 1
            while True:
                url = f"{base_url}?page={page}&pageSize={page_size}"
                async with sess.get(url) as r:
                    if r.status != 200:
                        raise RuntimeError(
                            f"Holded error {r.status}: {await r.text()}"
                        )

                    contacts: list = await self._json(r)

                all_contacts.extend(contacts)

                # last page reached when we receive fewer rows than page_size
                if len(contacts) < page_size:
                    break

                page += 1

        return all_contacts
                        
    async def invoice_details(self, document_id, doc_type: str = "invoice"):
        """Get invoice details by document ID"""
        url = self.base_url + "/invoicing/v1/documents/{doc_type}/{document_id}"

        async with aiohttp.ClientSession() as session:
            async with session.get(url, headers=self.headers) as res:
                if res.status != 200:
                    print(f"Error getting invoice details for {document_id}")
                    return None
                try:
                    return await res.json()
                except:
                    return None
                
    async def create_invoice(self, invoice_data: dict, doc_type: str = "invoice"):
        """Create a new invoice in Holded"""
        url = self.base_url + f"/invoicing/v1/documents/{doc_type}"
        body = invoice_data

        async with aiohttp.ClientSession() as session:
            async with session.post(url, json=body, headers=self.headers) as res:

                if res.status != 200:
                    try:
                        error = await res.json()
                        print(f"An error ocurred: {res.status} : {error}")
                    except Exception as e:
                        try:
                            error = await res.text()
                            print("Die error ->", error)
                        except Exception as e:
                            raise IndexError("An error ocurred while creating the invoice -> ", e)

                if res.status == 500:
                    print("Interval Server Error ocurred", res.text)
                    return 

                data = await res.json()
                print(data)


    async def _json(self, resp: aiohttp.ClientResponse) -> list | dict:
        """
        Return *resp* decoded as JSON – even when Holded lies and sends
        `text/html`.  If the body is not JSON, raise RuntimeError.
        """
        text  = await resp.text()
        try:
            return json.loads(text or "null")
        except json.JSONDecodeError:
            raise RuntimeError(
                f"Holded replied with HTML (status {resp.status}): "
                f"{text[:120]}…"
            )
        
    def _unix_ts(self, dt: datetime) -> int:
        """Return *dt* as a UTC-timestamp (int)."""
        return calendar.timegm(dt.timetuple())
    
    async def invoice_by_docnumber(self, doc_number: str, *, doc_type: str = "invoice", page_size: int = 200, lookback_years: int = 5):
        """
        Devuelve la factura cuyo `docNumber` (o `invoiceNum`) coincide con
        *doc_number*, o `None` si no existe.
        """
        base = f"{self.base_url}/invoicing/v1/documents/{doc_type}"
        qdoc = quote_plus(doc_number)

        # ── 1) filtro rápido ────────────────────────────────────────────────
        resp = await self._get(f"{base}?docNumber={qdoc}")
        data = await self._json(resp)        # _json ya comprueba status==200

        # - Si Holded respeta el filtro puede devolver dict, lista con 0..n docs o []
        if isinstance(data, dict):           # ← hit
            return data
        if isinstance(data, list):
            for doc in data:
                if doc.get("docNumber") == doc_number or doc.get("invoiceNum") == doc_number:
                    return doc
            # si lista vacía o sin match, probar invoiceNum y luego escaneo

        # Intento adicional con invoiceNum por si el API usa ese nombre
        resp2 = await self._get(f"{base}?invoiceNum={qdoc}")
        data2 = await self._json(resp2)
        if isinstance(data2, dict):
            return data2
        if isinstance(data2, list):
            for doc in data2:
                if doc.get("docNumber") == doc_number or doc.get("invoiceNum") == doc_number:
                    return doc

        # ── 2) escaneo paginado por ventanas de 1 año ──────────────────────
        today = datetime.utcnow().replace(hour=0, minute=0, second=0, microsecond=0)
        stop  = today - timedelta(days=lookback_years * 365)

        w_end = today
        while w_end > stop:
            w_start = max(stop, w_end - timedelta(days=365))
            params  = (
                f"starttmp={int(w_start.timestamp())}"
                f"&endtmp={int(w_end.timestamp())}"
            )

            page = 1
            while True:
                url  = f"{base}?{params}&page={page}&pageSize={page_size}"
                resp = await self._get(url)
                docs: list = await self._json(resp)

                for doc in docs:
                    if doc.get("docNumber") == doc_number or doc.get("invoiceNum") == doc_number:
                        return doc

                if len(docs) < page_size:            # última página de la ventana
                    break
                page += 1

            w_end = w_start                           # retrocede un año

        return None

    async def close(self):
        if self._session and not self._session.closed:
            await self._session.close()

    async def list_documents(self,
                             start_ts: int,
                             end_ts: int,
                             *,
                             doc_type: str = "invoice",
                             page_size: int = 200) -> list[dict]:
        """
        List all documents in Holded for a given time window [start_ts, end_ts].
        Returns a list of document dicts.
        """
        base = f"{self.base_url}/invoicing/v1/documents/{doc_type}"
        params = f"starttmp={int(start_ts)}&endtmp={int(end_ts)}"

        page = 1
        all_docs: list[dict] = []
        while True:
            url = f"{base}?{params}&page={page}&pageSize={page_size}"
            resp = await self._get(url)
            docs: list = await self._json(resp)

            if not isinstance(docs, list):
                break
            all_docs.extend(docs)

            if len(docs) < page_size:
                break
            page += 1

        return all_docs
    async def check_invoice_exists(self, bill_number: str):
        
        factura_holded = {

        }

    # CONTACTS OPERATIOS
            
    async def contact_details(self, contact_id: str):
        url = self.base_url + f"/invoicing/v1/contacts/{contact_id}"

        async with aiohttp.ClientSession() as session:
            async with session.get(url, headers=self.headers) as res:
                if res.status != 200:
                    error_text = await res.text()
                    print(f"Error fetching contact {contact_id}: {res.status} — {error_text}")
                    return {}

                return await res.json()

    async def create_contact(self, contact_data: dict):
        url = self.base_url + "/invoicing/v1/contacts"
        body = contact_data

        async with aiohttp.ClientSession() as session:
            async with session.post(url, json=body, headers=self.headers) as res:
                if res.status not in (200, 201):
                    error_text = await res.text()
                    print(f"Error creating contact: {res.status} — {error_text}")
                    return None

                return await res.json()

    async def list_contacts(self) -> list[dict]:
        """ List all contacts in Holded."""
        url = f"{self.base_url}/invoicing/v1/contacts"

        async with aiohttp.ClientSession() as sess:
            async with sess.get(url, headers=self.headers) as res:
                if res.status != 200:
                    print("Error fetching contacts:", res.status, await res.text())
                    return []
                return await res.json()


    async def contact_by_code(
        self,
        code: str,
        *,
        page_size: int = 500          # Holded max
    ) -> dict | None:
        """
        Return the contact whose `code` (CIF / NIF) equals *code*, or None
        if it does not exist.

        1) Try the undocumented filter  /contacts?code=…
        - If the API honours the filter it returns [ {contact} ].
        - If not, it may return many contacts or even an HTML page.
        2) If step 1 fails, scan every page until we find a match.
        """
        base = f"{self.base_url}/invoicing/v1/contacts"
        code = code.strip().upper()

        async with aiohttp.ClientSession(headers=self.headers) as sess:

            # ── 1) quick filter ─────────────────────────────────────────────
            url = f"{base}?code={quote_plus(code)}"
            async with sess.get(url) as r:
                try:
                    data = await self._json(r)              # tolerant JSON helper
                    if isinstance(data, list):
                        # Scan the list – only return if the code really matches
                        for c in data:
                            if c.get("code", "").strip().upper() == code:
                                return c
                        # Filter ignored → fall through to full scan
                except RuntimeError:
                    # Received HTML → fall through to full scan
                    pass

            # ── 2) full paginated scan ─────────────────────────────────────
            page = 1
            while True:
                url = f"{base}?page={page}&pageSize={page_size}"
                async with sess.get(url) as r:
                    contacts: list = await self._json(r)

                for c in contacts:
                    if c.get("code", "").strip().upper() == code:
                        return c

                if len(contacts) < page_size:     # reached last page
                    return None
                page += 1
    # PRODUCTS OPERATIONS
    

async def test_2():
    holded = HoldedService()

    res = await holded.invoice_by_docnumber("ZOR24-0sd0139"); print(res)

async def main_tests():
    holded = HoldedService()
    new_contact = {
        "name":     "Test User",
        "code":     "12345678A",  # DNI or NIF
        "type":     "client",
        "isperson": True,
        "billAddress": {
            "address":    "Calle Falsa 123",
            "city":       "Madrid",
            "postalCode": "28013",
            "province":   "Madrid",
            "country":    "ES"
        }
    }

    # Create a new contact
    # created_contact = await holded.create_contact(new_contact); print("Created Contact:", created_contact)
    contact = await holded.contact_by_code("B18609719"); print(contact)
    # a = await holded.list_contacts(); print(a)


    # cotacts = await holded.list_invoices(); print(cotacts)

if __name__ == "__main__":
    asyncio.run(main_tests()) 