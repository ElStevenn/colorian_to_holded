import aiohttp
import asyncio
import json
import base64
import os

from src.config.settings import HOLDED_API_KEY


class HoldedService:
    def __init__(self):
        self.api_key = HOLDED_API_KEY
        self.base_url = "https://api.holded.com/api"
        self.headers = {
            "Accept": "application/json",
            "Key": self.api_key,
        }

    # IVOICE OPERATIONS
    async def list_invoices(self, doc_type: str):
        """Get Invoices"""
        url = self.base_url + f"/invoicing/v1/documents/{doc_type}"  

        async with aiohttp.ClientSession() as session:
            async with session.get(url, headers=self.headers) as res:
                response_text = await res.text()

                if res.status != 200:
                    print(f"Error: {res.status} - {response_text}")
                    return []

                # Force JSON parsing manually
                try:
                    data = json.loads(response_text)  
              
                    return data  
                except json.JSONDecodeError as e:
                    print(f"JSON Parsing Error: {e}")
                    return []
                
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
                    error = await res.json()
                    print(f"An error ocurred: {res.status} : {error}")
                
                if res.status == 500:
                    print("Interval Server Error ocurred", res.text)
                    return 

                data = await res.json()
                print(data)

    async def check_invoice_exists(self, bill_number: str):
        
        factura_holded = {

        }

    # CONTACTS OPERATIOS
    async def list_contacts(self):
        url = self.base_url + "/invoicing/v1/contacts"

        async with aiohttp.ClientSession() as session:
            async with session.get(url, headers=self.headers) as res:
                if res.status != 200:
            
                    text = await res.text()
                    print(f"Error fetching contacts: {res.status} — {text}")
                    return []

                payload = await res.json() 
        
                return list(payload)
            
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
                if res.status != 200:
                    error_text = await res.text()
                    print(f"Error creating contact: {res.status} — {error_text}")
                    return None

                return await res.json()


    async def search_contact(self, search_term): # Change "search_term" to a specific field
        all_contacts = await self.list_contacts()



    # PRODUCTS OPERATIONS
    

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
    created_contact = await holded.create_contact(new_contact); print("Created Contact:", created_contact)

    # cotacts = await holded.list_contacts(); print(cotacts)

if __name__ == "__main__":
    asyncio.run(main_tests()) 