import asyncio
import json
import pytz
import re
import base64
import logging
from datetime import datetime, timedelta
from collections import defaultdict
from typing import Dict, Any
from decimal import Decimal, ROUND_HALF_UP
import random
import traceback
import time

from src.services.clorian_service import ClorianService
from src.services.holded_service import HoldedService
from src.config.settings import CLORIAN_ACCOUNTS, get_offset, increment_offset, _clean

# Configure logging
logger = logging.getLogger(__name__)


class AsyncService:
    """Main class to handle asynchronous operations for syncing data between Clorian and Holded."""
    def __init__(self):
        self.tz_mad = pytz.timezone("Europe/Madrid")
        self.holded_api = HoldedService()
        self._contact_cache = {}

    async def fetch_clorian_invoices(self):
        """Main function to fetch invoices from Clorian."""
        logger.info(f"📋 Starting invoice sync for {len(CLORIAN_ACCOUNTS)} Clorian accounts")
        tasks = []

        for i, acc in enumerate(CLORIAN_ACCOUNTS, 1):
            account_name = acc.get("name", "Unknown Account")
            logger.info(f"🏢 Processing account {i}/{len(CLORIAN_ACCOUNTS)}: {account_name}")
            
            try:
                clorian_account = ClorianService(account_name)
                logger.info(f"🔑 Refreshing authentication token for {account_name}")
                await clorian_account.refresh_token()
                logger.info(f"✅ Token refreshed successfully for {account_name}")
                
                # Sync from fixed start date
                now = datetime.utcnow()
                start_date = datetime(2024, 7, 1)
                sync_period = f"{start_date.strftime('%Y-%m-%d %H:%M:%S')} -> {now.strftime('%Y-%m-%d %H:%M:%S')}"
                logger.info(f"📅 Fixed sync period for {account_name}: {sync_period}")

                tasks.append(
                    self.process_account_invoices(
                        clorian_account,
                        start_date=start_date,
                        end_date=now,
                        simplified=False,
                    )
                )
                
            except Exception as e:
                logger.error(f"❌ Failed to setup account {account_name}: {e}")
                logger.error(f"Traceback: {traceback.format_exc()}")
                continue

        if not tasks:
            logger.warning("⚠️  No accounts were successfully initialized")
            return
            
        logger.info(f"⚡ Running parallel sync for {len(tasks)} accounts")
        try:
            await asyncio.gather(*tasks)
            logger.info("✅ All account syncs completed")
        except Exception as e:
            logger.error(f"❌ Error during parallel account sync: {e}")
            raise

    def _holded_id(self, obj: dict | None) -> str | None:
        """Returns Holded document / contact ID"""
        if not obj:
            return None
        return obj.get("_id") or obj.get("id") or obj.get("contactId")

    async def process_account_invoices(self, clorian_account: "ClorianService",  start_date: str | datetime | None = None, end_date: str | datetime | None = None, days_back: int = 365 * 10,  doc_limit: int = None, simplified: bool = False):
        account_name = clorian_account.name
        logger.info(f"📊 Starting invoice processing for account: {account_name}")
        process_start = time.time()
        
        try:
            logger.info(f"🔍 Fetching invoices from Clorian API (account: {account_name}) [simplified={simplified}]")
            if simplified:
                all_invoices = await clorian_account.get_bills(
                    start_date=start_date,
                    end_date=end_date,
                    days_back=days_back,
                    concurrency=10,
                )
            else:
                all_invoices = await clorian_account.get_bills_v2(
                    start_date=start_date,
                    end_date=end_date,
                    days_back=days_back,
                    concurrency=10,
                )
            logger.info(f"📄 Retrieved {len(all_invoices)} invoices from {account_name}")
        except Exception as e:
            logger.error(f"❌ Failed to fetch invoices from {account_name}: {e}")
            raise

        GENERIC_CODE = ""
        GENERIC_CONTACT_ID = "6870e8c71d1ac03be40e7f16"
        self._contact_cache = {}
        
        # Processing counters
        processed_count = 0
        skipped_duplicates = 0
        created_contacts = 0
        created_invoices = 0
        errors_count = 0

        def has_nif(n: str) -> bool:
            return bool(n and n.strip())

        # Process as many as the platform allows, but keep a hard time budget
        max_invoices_per_run = 10**9  # effectively unlimited; controlled by time
        max_execution_time = 8 * 60  # 8 minutes to leave buffer for cleanup
        
        if len(all_invoices) > max_invoices_per_run:
            logger.info(f"📅 Processing will be limited by execution time, not by a fixed count")
            # Sort by date (newest first)
            all_invoices = sorted(all_invoices, key=lambda x: x.get('billDate', ''), reverse=True)
            
        # Pre-fetch existing Holded documents in the same time window to avoid per-invoice duplicate calls
        try:
            holded_docs_cache: dict[str, dict] = {}
            if all_invoices:
                min_date = min(x.get("billDate", "2099-12-31 23:59:59") for x in all_invoices)
                max_date = max(x.get("billDate", "1970-01-01 00:00:00") for x in all_invoices)
                w_start = int(datetime.strptime(min_date, "%Y-%m-%d %H:%M:%S").timestamp())
                w_end   = int(datetime.strptime(max_date, "%Y-%m-%d %H:%M:%S").timestamp())
                # small padding
                w_start -= 86400
                w_end   += 86400
                docs = await self.holded_api.list_documents(w_start, w_end, doc_type="invoice", page_size=200)
                for d in docs:
                    key = d.get("docNumber") or d.get("invoiceNum")
                    if key:
                        holded_docs_cache[str(key)] = d
            logger.info(f"📚 Prefetched {len(holded_docs_cache)} Holded docs for duplicate detection")
        except Exception:
            holded_docs_cache = {}
            logger.warning("⚠️  Could not prefetch Holded documents; falling back to per-invoice lookup")

        logger.info(f"🔄 Processing {len(all_invoices)} invoices for {account_name}")
        for i, bill in enumerate(all_invoices, 1):
            # Check execution time to avoid timeout
            elapsed_time = time.time() - process_start
            if elapsed_time > max_execution_time:
                logger.warning(f"⏰ Stopping processing after {i-1} invoices due to time limit ({elapsed_time:.1f}s)")
                break
                
            # Progress logging every 5 invoices
            if i % 5 == 0 or i == 1:
                elapsed = elapsed_time
                remaining_time = max_execution_time - elapsed
                logger.info(f"📈 Progress: {i}/{len(all_invoices)} invoices ({(i/len(all_invoices)*100):.1f}%) - Elapsed: {elapsed:.1f}s, Remaining: {remaining_time:.1f}s")
            try:
                bill_number = bill.get("billNumber", "Unknown")
                logger.info(f"📋 Processing invoice {i}/{len(all_invoices)}: {bill_number} (Account: {account_name})")
                
                # --- 1) duplicados -------------------------------------------------
                logger.info(f"🔍 Checking for duplicate invoice: {bill_number}")
                duplicate_check_start = time.time()
                duplicate_exists = holded_docs_cache.get(bill["billNumber"]) if holded_docs_cache else await self.holded_api.invoice_by_docnumber(bill["billNumber"])
                duplicate_check_time = time.time() - duplicate_check_start
                logger.info(f"⏱️  Duplicate check completed in {duplicate_check_time:.2f}s")
                
                if duplicate_exists:
                    logger.info(f"⏭️  Invoice {bill_number} already exists in Holded, skipping")
                    skipped_duplicates += 1
                    continue

                nif = (bill.get("vatNumber") or "").strip().upper()
                holded_contact_id = None                       # ← siempre parte a None

                # --- 2) contacto SOLO si hay NIF -----------------------------------
                if has_nif(nif):
                    logger.debug(f"👤 Processing contact with NIF: {nif} for invoice {bill_number}")
                    holded_contact_id = self._contact_cache.get(nif)

                    if not holded_contact_id:                  # no estaba cacheado
                        logger.debug(f"🔍 Searching for existing contact with NIF: {nif}")
                        existing = await self.holded_api.contact_by_code(code=nif)
                        holded_contact_id = self._holded_id(existing)

                        if not holded_contact_id:              # no existía en Holded
                            logger.debug(f"🆕 Creating new contact for NIF: {nif}")
                            contact_create_start = time.time()
                            created = await self.holded_api.create_contact(
                                self.transform_clorian_bill_to_holded_contact(bill)
                            )
                            contact_create_time = time.time() - contact_create_start
                            holded_contact_id = self._holded_id(created)
                            created_contacts += 1
                            logger.debug(f"✅ Contact created successfully for NIF: {nif} in {contact_create_time:.2f}s")
                        else:
                            logger.debug(f"♻️  Using existing contact for NIF: {nif}")

                        self._contact_cache[nif] = holded_contact_id  # cachear
                    else:
                        logger.debug(f"💾 Using cached contact for NIF: {nif}")
                else:
                    logger.debug(f"🔓 No NIF found for invoice {bill_number}, using generic contact")

                # --- 3) construir factura ------------------------------------------
                logger.debug(f"🏗️  Transforming Clorian invoice {bill_number} to Holded format")
                inv = await self.transform_invoice_clorian_to_holded(
                    bill,
                    contact=bool(holded_contact_id)            # True si hay contacto real
                )

                inv.update(
                    contactId   = holded_contact_id or GENERIC_CONTACT_ID,
                    contactCode = nif or GENERIC_CODE,
                )

                logger.info(f"📤 Creating invoice {bill_number} in Holded")
                invoice_create_start = time.time()
                await self.holded_api.create_invoice(inv)
                invoice_create_time = time.time() - invoice_create_start
                created_invoices += 1
                processed_count += 1
                logger.info(f"✅ Invoice {bill_number} created successfully in Holded in {invoice_create_time:.2f}s")

            except Exception as exc:
                errors_count += 1
                logger.error(f'❌ Error processing invoice {bill.get("billNumber", "Unknown")} (billId: {bill.get("billId", "Unknown")}): {exc}')
                logger.error(f"Traceback: {traceback.format_exc()}")
            finally:
                # Reduced sleep to speed up processing (was 0.5s)
                time.sleep(0.1)
                
        # Log processing summary
        duration = time.time() - process_start
        logger.info(f"📊 Account {account_name} processing summary:")
        logger.info(f"  📄 Total invoices processed: {processed_count}")
        logger.info(f"  ⏭️  Skipped duplicates: {skipped_duplicates}")
        logger.info(f"  👤 New contacts created: {created_contacts}")
        logger.info(f"  📋 New invoices created: {created_invoices}")
        logger.info(f"  ❌ Errors encountered: {errors_count}")
        logger.info(f"  ⏱️  Processing time: {duration:.2f} seconds")
        
        if errors_count > 0:
            logger.warning(f"⚠️  Account {account_name} completed with {errors_count} errors")
        else:
            logger.info(f"✅ Account {account_name} processed successfully")

    async def transform_invoice_clorian_to_holded(self, clorian_invoice: dict, contact: bool):
        """Build the JSON body for POST /documents/invoice (Holded)"""
        bill_number = clorian_invoice.get("billNumber", "Unknown")
        bill_id = clorian_invoice.get("billId", "Unknown")
        logger.debug(f"🔄 Starting transformation for invoice {bill_number} (ID: {bill_id})")

        # --- utilidades -------------------------------------------------
        def _round2(x: float) -> float:
            return float(Decimal(str(x)).quantize(Decimal("0.01"), ROUND_HALF_UP))

        has_nif = bool((clorian_invoice.get("vatNumber") or "").strip())
        country_code = (clorian_invoice.get("country") or "")[:2].upper()
        
        logger.debug(f"📋 Invoice details: has_nif={has_nif}, country={country_code}, contact={contact}")

        # ---------- CONTACT NAME (la parte importante) ------------------
        if has_nif:
            contact_name = (
                clorian_invoice.get("legalEntityName")                   # empresa
                or " ".join(                                             # persona
                    filter(None, (
                        clorian_invoice.get("firstName"),
                        clorian_invoice.get("lastName1"),
                        clorian_invoice.get("lastName2"),
                    ))
                ).strip()
            )
        else:
            # - No nif -
            contact_name = "cliente general"

        # ---------- cabecera Holded -------------------------------------
        holded: Dict[str, Any] = {
            "docType":          "invoice",
            "invoiceNum":       clorian_invoice["billNumber"],
            "date":             int(datetime.strptime(
                                    clorian_invoice["billDate"],
                                    "%Y-%m-%d %H:%M:%S"
                                  ).timestamp()),
            "contactName":      contact_name,
            "contactCode":      clorian_invoice.get("vatNumber", "") if has_nif else "",
            "contactAddress":   clorian_invoice.get("address", ""),
            "contactCity":      clorian_invoice.get("city", ""),
            "contactCountryCode": country_code,
            "contactCp":        clorian_invoice.get("postalCode", ""),
            "items":            [],
        }

        if contact:                           # ya teníamos el contacto creado
            holded["contactId"] = clorian_invoice["clientId"]
        # else:
        #     holded["contactId"] = "64ff7d0f4f8cb00012345678"

        # ---------- líneas / impuestos ----------------------------------
        taxes        = clorian_invoice.get("billTaxes", [])
        default_rate = taxes[0]["taxRate"] if taxes else 0

        for line in clorian_invoice.get("billLines", []):
            rate = next((t["taxRate"] for t in taxes
                         if t["billId"] == clorian_invoice["billId"]), default_rate)
            rate_pct = round(rate * 100, 2) if rate <= 1 else round(rate, 2)

            holded["items"].append({
                "serviceId": str(line.get("reservationId", "")),
                "name":      f"Reserva {line.get('reservationId', '')}",
                "subtotal":  _round2(line.get("billLineBaseAmount", 0)),
                "tax":       rate_pct,
            })

        # ---------- forma de pago ---------------------------------------
        origin = (clorian_invoice.get("billLines", [{}])[0]
                  .get("paymentOrigin", "").lower())
        holded["paymentMethodId"] = {
        "cash": "68a83139c4854186960aac9f",
        "deferred": "68a820fcb61533185f0c0f8d",
        "transfer": "688356b04be192a8cd05faea",
        "voucher": "68a8210f30da643cd9023447",
        "prepayment": "68a821360555d6c96f0b14de",
        "paypal-e": "68a827829a619d9b360f6632",
        "paypal": "68a8217b8562a06be406fa40",
        "adyen-pos-v": "68a827b3fe7f2e3d7408388f",
        "pos2": "68a827c5a73dd0823409fd1e",
        "alipay": "68a827d7543c5c1f860f0659",
        "wechat": "68a827e2219129001a0540cc",
        "bizum": "68a827f0471f09253f07c069"
        }.get(origin, "")

        return holded

    def transform_clorian_bill_to_holded_contact(self, bill: Dict[str, Any], *, contact_type: str = "client"):
        """Build the JSON body for POST /contacts in Holded from a Clorian bill."""
        is_person = bill.get("personType", "").upper() == "INDIVIDUAL"

        if is_person:
            name = _clean(
                " ".join(
                    p
                    for p in (
                        bill.get("firstName"),
                        bill.get("lastName1"),
                        bill.get("lastName2"),
                    )
                    if p
                )
            ) or "Unnamed person"
        else:
            name = _clean(bill.get("legalEntityName")) or "Unnamed company"

        contact: Dict[str, Any] = {
            "name": name,
            "code": _clean(bill.get("vatNumber")),
            "type": contact_type,  # client / supplier / …
            "isperson": is_person,
            "email": _clean(bill.get("email")),
            "phone": _clean(bill.get("mobile") or bill.get("telephone")),
            "billAddress": {
                "address": _clean(bill.get("address"), 120),
                "city": _clean(bill.get("city"), 60),
                "postalCode": _clean(bill.get("postalCode"), 15),
                "province": _clean(bill.get("state"), 60),
                "country": (bill.get("country") or "")[:2].upper(),
            },
        }

        client_id = bill.get("clientId")
        if client_id:
            contact["CustomId"] = str(client_id)

        contact["billAddress"] = {k: v for k, v in contact["billAddress"].items() if v}
        if not contact["billAddress"]:
            del contact["billAddress"]

        contact = {k: v for k, v in contact.items() if v not in ("", None, {})}
        return contact


async def invoice_converter():
    """Used to convert a single Clorian Invoice"""
    
    pass

async def migration_proceed():
    """Main entry point for the Clorian to Holded sync process."""
    logger.info("🚀 Starting Clorian to Holded sync process")
    start_time = time.time()
    
    try:
        async_service = AsyncService()
        await async_service.fetch_clorian_invoices()
        
        duration = time.time() - start_time
        logger.info(f"✅ Sync process completed successfully in {duration:.2f} seconds")
        
    except Exception as e:
        duration = time.time() - start_time
        logger.error(f"❌ Sync process failed after {duration:.2f} seconds: {e}")
        logger.error(f"Traceback: {traceback.format_exc()}")
        raise

async def main_test():
    clorian_account = ClorianService("Clorian Flamenco Granada")
    await clorian_account.refresh_token()
    async_service = AsyncService()
    clorian_bill = {
        "billId": 447732,
        "simplified": False,
        "processDate": "2024-07-02 00:00:00",
        "clientId": 107,
        "billSender": "client",
        "type": "individual",
        "status": "valid",
        "annulation": False,
        "billDate": "2024-07-02 14:53:40",
        "billNumber": "ZOR24-00129",
        "operationStartDatetime": "2024-07-02 14:53:40",
        "operationEndDatetime": "2024-07-02 14:53:40",
        "baseAmount": 184.090905,
        "taxAmount": 18.409095,
        "legalEntityName": "Visitours Excursiones S.L.",
        "vatNumberType": "CIF",
        "vatNumber": "B41739202",
        "address": "Calle Francisco Pacheco Nº1",
        "city": "Tomares",
        "state": "Sevilla",
        "postalCode": "41940",
        "country": "ES",
        "languageId": 1,
        "personType": "CORPORATION",
        "billLines": [
        {
            "billLineId": 3860115,
            "billLineBaseAmount": 184.090905,
            "billLineTaxAmount": 18.409095,
            "paymentDate": "2024-07-02 14:51:49",
            "paymentReference": "1567544050152720155",
            "reservationId": 63100830,
            "paymentId": 50676537,
            "firstPayment": True,
            "secondPayment": True,
            "paymentOrigin": "transfer",
            "paymentCreationDate": "2024-06-18 15:16:45",
            "paymentModificationDate": "2024-07-02 14:51:49",
            "paymentSalesGroupId": 844,
            "paymentReservationId": 52630733
        }
        ],
        "billTaxes": [
        {
            "billId": 447732,
            "taxRate": 0.1,
            "taxAmount": 18.409095,
            "taxBasis": 184.09095
        }
        ]
    }

    '''
    bills = await clorian_account.get_bills(days_back=370, concurrency=10)
    with open("bills.json", 'w') as f:
        json.dump(bills, f, indent=2, ensure_ascii=False)
    '''

    # new_contact = async_service.transform_clorian_bill_to_holded_contact(clorian_bill)

    holded_bill = await async_service.transform_invoice_clorian_to_holded(clorian_bill)
    print("Holded bill:", holded_bill)

    with open("holded_invoice.json", "w") as f:
        json.dump(holded_bill, f, indent=2, ensure_ascii=False)


if __name__ == "__main__":
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(levelname)s %(name)s: %(message)s"
    )
    asyncio.run(migration_proceed())
