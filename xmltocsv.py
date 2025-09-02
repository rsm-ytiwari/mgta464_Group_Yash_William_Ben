#!/usr/bin/env python3
"""
XML → CSV extractor for Supplier Invoices.

Outputs columns matching CORE.SUPPLIER_INVOICES:
  INVOICE_ID, SUPPLIER_ID, PURCHASE_ORDER_ID, INVOICE_DATE,
  AMOUNT_EXCLUDING_TAX, TAX_AMOUNT, TOTAL_AMOUNT, CURRENCY_CODE

Why this way (Snowflake-first mindset):
- We only do minimal shaping outside Snowflake due to time pressure.
- We keep lineage by later loading into RAW first, then casting in CORE.
"""

import csv, os, sys, xml.etree.ElementTree as ET
from typing import Iterable, Optional

# ---- Edit paths if needed
INPUT_XML = os.getenv("INV_XML_PATH", "Data/xml/Supplier_invoices.xml")
OUTPUT_CSV = os.getenv("INV_OUT_CSV", "Data/csv/invoices_extracted.csv")

COLS = [
    "INVOICE_ID",
    "SUPPLIER_ID",
    "PURCHASE_ORDER_ID",
    "INVOICE_DATE",
    "AMOUNT_EXCLUDING_TAX",
    "TAX_AMOUNT",
    "TOTAL_AMOUNT",
    "CURRENCY_CODE",
]

# Map XML tags → our columns (robust to minor label drift)
ALIASES = {
    "INVOICE_ID": ["SupplierTransactionID", "InvoiceID", "TransactionID"],
    "SUPPLIER_ID": ["SupplierID", "VendorID"],
    "PURCHASE_ORDER_ID": ["PurchaseOrderID", "POID"],
    "INVOICE_DATE": ["TransactionDate", "InvoiceDate", "Date"],
    "AMOUNT_EXCLUDING_TAX": ["AmountExcludingTax", "SubTotal", "NetAmount"],
    "TAX_AMOUNT": ["TaxAmount", "VATAmount"],
    "TOTAL_AMOUNT": ["TransactionAmount", "TotalAmount", "GrossAmount"],
    "CURRENCY_CODE": ["CurrencyCode", "Currency"],
}
TYPE_FIELD_NAMES = ["TransactionTypeID", "TypeID", "RecordTypeID"]
INVOICE_TYPE_VALUES = {"5"}  # invoice rows only


def strip_ns(tag: str) -> str:
    return tag.split("}", 1)[1] if "}" in tag else tag


def text(elem: Optional[ET.Element]) -> str:
    return (elem.text or "").strip() if elem is not None else ""


def build_index(row: ET.Element) -> dict:
    return {strip_ns(c.tag).lower(): text(c) for c in list(row)}


def first(idx: dict, names: Iterable[str]) -> Optional[str]:
    for n in names:
        v = idx.get(n.lower())
        if v is not None:
            return v.strip()
    return None


def is_invoice(idx: dict) -> bool:
    v = first(idx, TYPE_FIELD_NAMES) or ""
    return v in INVOICE_TYPE_VALUES


def iter_row_elements(xml_path: str, row_tag: str = "row"):
    # Streaming parse to keep memory low
    for _, elem in ET.iterparse(xml_path, events=("end",)):
        if strip_ns(elem.tag).lower() == row_tag.lower():
            yield elem
            elem.clear()


def main():
    if not os.path.exists(INPUT_XML):
        print(f"ERROR: XML not found at {INPUT_XML}")
        sys.exit(1)

    os.makedirs(os.path.dirname(OUTPUT_CSV), exist_ok=True)
    written = 0

    with open(OUTPUT_CSV, "w", newline="", encoding="utf-8") as f:
        w = csv.writer(f)
        w.writerow(COLS)

        any_rows = False
        for row in iter_row_elements(INPUT_XML, "row"):
            any_rows = True
            idx = build_index(row)
            if not is_invoice(idx):
                continue  # keep only TransactionTypeID == 5

            out = [
                first(idx, ALIASES["INVOICE_ID"]) or "",
                first(idx, ALIASES["SUPPLIER_ID"]) or "",
                first(idx, ALIASES["PURCHASE_ORDER_ID"]) or "",
                first(idx, ALIASES["INVOICE_DATE"]) or "",
                first(idx, ALIASES["AMOUNT_EXCLUDING_TAX"]) or "",
                first(idx, ALIASES["TAX_AMOUNT"]) or "",
                first(idx, ALIASES["TOTAL_AMOUNT"]) or "",
                first(idx, ALIASES["CURRENCY_CODE"]) or "",
            ]
            w.writerow(out)
            written += 1

    if not any_rows:
        print(
            "WARNING: No <row> elements found. If your XML uses a different tag, "
            "change iter_row_elements(..., 'row')."
        )
    print(f"✓ Wrote {written} invoice rows → {OUTPUT_CSV}")


if __name__ == "__main__":
    main()
