import asyncio
import json
import pytz
import re
import base64
from datetime import datetime
from collections import defaultdict
import random
import traceback
import time

from src.services.clorian_service import ClorianService
from src.services.holded_service import HoldedService
from src.config.settings import CLORIAN_ACCOUNTS, HOLDED_API_KEY


class AsyncService:
    """Main class to handle asynchronous operations for syncing data between Clorian and Holded."""
    def __init__(self):
        self.tz_mad = pytz.timezone("Europe/Madrid")
        self.holded_api = HoldedService()

    async def fetch_clorian_invoices(self):
        """Main function to fetch invoices from Clorian."""
        tasks = []

        for acc in CLORIAN_ACCOUNTS:
            clorian_account = ClorianService(acc)
            await clorian_account.refresh_token()


            break

    async def process_account_invoices(self, clorian_account: "ClorianService"):
        """"""



    async def fetch_clorian_products(self):
        pass
    
    async def transform_invoice_clorian_to_holded(self, clorian_invoice: dict, ):
        invoice = {}

    def transform_invoice_clorian_to_holded_client(self, clorian_invoice: dict):
        """Transform Clorian invoice client data to Holded format."""

        # Detect person vs company
        is_person = clorian_invoice.get("personType", "").upper() == "INDIVIDUAL"

        # Name
        if is_person:
            parts = [
                clorian_invoice.get("firstName", "").strip(),
                clorian_invoice.get("lastName1", "").strip(),
                clorian_invoice.get("lastName2", "").strip()
            ]
            name = " ".join(p for p in parts if p)
        else:
            name = clorian_invoice.get("legalEntityName", "").strip()

        # Tax ID
        code = clorian_invoice.get("vatNumber", "").strip()

        bill_address = {
            "address":    clorian_invoice.get("address", "").strip(),
            "city":       clorian_invoice.get("city", "").strip(),
            "postalCode": clorian_invoice.get("postalCode", "").strip(),
            "province":   clorian_invoice.get("state", "").strip(),
            "country":    clorian_invoice.get("country", "").strip()
        }

        new_contact = {
            "name":       name or "Sin Nombre",
            "code":       code or None,
            "type":       "client",
            "isperson":   is_person,
            "billAddress": bill_address
        }

        if clorian_invoice.get("clientId"):
            new_contact["CustomId"] = clorian_invoice.get("clientId")

   

        return new_contact

async def invoice_converter():
    """Used to convert a single Clorian Invoice"""
    pass


async def main_tes():
    config = {"name": "Clorian Flamenco Granada", "username": "integration-holded@flamencogranada.com", "password": "3BdH649KT4d1T5JETlaj?", "refresh_token": "sddsdsd", "cuentas_a_migrar": [ "general" ], "offset_cuentas_a_migrar": [ 0 ] } 
    clorian_account = ClorianService(config)
    await clorian_account.refresh_token()
    async_service = AsyncService()
    clorian_bill = {
        "address": "string",
        "annulation": True,
        "baseAmount": 0,
        "billDate": "2020-04-01T15:50:09.224Z",
        "billId": 0,
        "billLines": [
        {
            "billLineBaseAmount": 0,
            "billLineId": 0,
            "billLineTaxAmount": 0,
            "firstPayment": True,
            "paymentCreationDate": "2020-04-01T15:50:09.224Z",
            "paymentDate": "2020-04-01T15:50:09.224Z",
            "paymentId": 0,
            "paymentModificationDate": "2020-04-01T15:50:09.224Z",
            "paymentOrigin": "string",
            "paymentReference": "string",
            "paymentSalesGroupId": 0,
            "paymentVoucherPartnerId": 0,
            "reservationId": 0,
            "secondPayment": True,
            "paymentReservationId": 0
        }
        ],
        "billNumber": "string",
        "billSender": "string",
        "billTaxes": [
        {
            "billId": 0,
            "taxAmount": 0,
            "taxBasis": 0,
            "taxId": 0,
            "taxRate": 0
        }
        ],
        "city": "string",
        "clientId": 0,
        "country": "string",
        "firstName": "string",
        "languageId": 0,
        "lastName1": "string",
        "lastName2": "string",
        "legalEntityName": "string",
        "operationEndDatetime": "2020-04-01T15:50:09.224Z",
        "operationStartDatetime": "2020-04-01T15:50:09.224Z",
        "parentBillId": 0,
        "personType": "string",
        "postalCode": "string",
        "processDate": "2020-04-01T15:50:09.224Z",
        "sellerId": 0,
        "simplified": True,
        "state": "string",
        "status": "string",
        "taxAmount": 0,
        "type": "string",
        "vatNumber": "string",
        "vatNumberType": "string"
    }

    new_contact = async_service.transform_invoice_clorian_to_holded_client(clorian_bill)

    print("New contact", new_contact)


if __name__ == "__main__":
    asyncio.run(main_tes())
