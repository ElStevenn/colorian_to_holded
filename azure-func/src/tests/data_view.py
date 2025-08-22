import asyncio, pandas as pd, decimal, json
from collections import defaultdict
from datetime import datetime, timedelta
from src.services.clorian_service import ClorianService

decimal.getcontext().rounding = decimal.ROUND_HALF_UP


BILL_COLS = [
    "reservationId", "status", "productName", "salesGroupName",
    "ticketQty", "grossTotal", "netBase", "taxAmount", "taxRate",
    "firstName", "lastName", "email", "telephone", "country"
]


def split_tax(gross: decimal.Decimal, rate: decimal.Decimal) -> tuple[decimal.Decimal, decimal.Decimal]:
    """Return net, tax given gross and VAT rate (e.g. 0.10)."""
    if rate == 0:
        return gross, decimal.Decimal("0.00")
    net = (gross / (1 + rate)).quantize(decimal.Decimal("0.01"))
    tax = (gross - net).quantize(decimal.Decimal("0.01"))
    return net, tax


def purchases_to_bill_csv(purchases: list, path: str = "clorian_bill.csv") -> pd.DataFrame:
    rows = []

    for pur in purchases:
        buyer = {
            "firstName": pur.get("firstName", ""),
            "lastName":  pur.get("lastName", ""),
            "email":     pur.get("email", ""),
            "telephone": pur.get("telephone", ""),
            "country":   pur.get("country", ""),
        }

        for res in pur.get("reservationList", []):
            gross_total = decimal.Decimal("0.00")
            ticket_qty  = 0
            vat_rate    = None

            for tk in res.get("ticketList", []):
                gross_total += decimal.Decimal(str(tk.get("amount", 0)))
                ticket_qty  += 1
                vat_rate     = vat_rate or decimal.Decimal(str(tk.get("taxRate", 0)))

                for comp in tk.get("ticketComplementSet", []):
                    gross_total += decimal.Decimal(str(comp.get("price", 0)))
                for ext in tk.get("ticketExtraSet", []):
                    gross_total += decimal.Decimal(str(ext.get("price", 0)))

            vat_rate = vat_rate or decimal.Decimal("0")
            net, tax = split_tax(gross_total, vat_rate)

            rows.append({
                **buyer,
                "reservationId":  res.get("reservationId"),
                "status":         res.get("status"),
                "productName":    res.get("productName"),
                "salesGroupName": res.get("salesGroupName"),
                "ticketQty":      ticket_qty,
                "grossTotal":     float(gross_total),
                "netBase":        float(net),
                "taxAmount":      float(tax),
                "taxRate":        float(vat_rate),
            })

    df = pd.DataFrame(rows, columns=BILL_COLS)
    df.to_csv(path, index=False)
    return df


async def export_last_10_days():
    cs = ClorianService("Clorian Flamenco Granada")
    await cs.refresh_token()

    purchases = await cs.get_purchases(days_back=10, concurrency=10)
    df = purchases_to_bill_csv(purchases)
    print(f"✓ {len(df)} reservation lines saved to clorian_bill.csv")

    # Also fetch normal bills (test for get_bills_v2)
    bills = await cs.get_bills_v2(days_back=1)
    with open("normal_bills.json", "w") as f:
        json.dump(bills, f, indent=2, ensure_ascii=False)
    print(f"✓ {len(bills)} normal bills saved to normal_bills.json")


if __name__ == "__main__":
    asyncio.run(export_last_10_days())
