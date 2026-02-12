#!/usr/bin/env python3
"""
build_ebay_batch_csv.py

Reads:
  - tmp/match_review.csv            (idx starts at 0)
  - tmp/upload_manifest.jsonl       (listing_index starts at 1)  <-- OFF-BY-ONE FIX APPLIED
  - tmp/card_identifications.jsonl  (listing_index starts at 1; optional fallback)

Writes:
  - tmp/Batch.csv (eBay File Exchange CSV) matching your known-good format:
      * First line unquoted: Info,Version=1.0.0,Template=fx_category_template_EBAY_US
      * Header + rows quoted (csv.QUOTE_ALL)
      * PicURL uses " | " delimiter (space-pipe-space)
      * BestOfferEnabled defaults to "1"
      * CustomLabel is SAME for all rows: "batch-auto"

Filtering:
  - ONLY rows where needs_review is false
  - Require both front_url and back_url
  - Only auto-list if best_ungraded_price < 20 and final_price < 20

Pricing:
  - floor computation at 2.49
  - if < 5.00 => +1.50
  - if >= 5.00 => *1.25
  - cents formatting:
      - cents 00–49 -> end with .49
      - cents 50–99 -> end with .95

NOTE: OFF-BY-ONE JOIN FIX
  match_review idx (0-based) -> manifest/idents listing_index (1-based)
  manifest_idx = idx + 1
"""

import argparse
import csv
import json
import os
import re
from typing import Any, Dict, List, Optional

DEFAULT_MANIFEST = "tmp/upload_manifest.jsonl"
DEFAULT_IDENTS = "tmp/card_identifications.jsonl"
DEFAULT_MATCH_REVIEW = "tmp/match_review.csv"
DEFAULT_OUT = "tmp/Batch.csv"

INFO_LINE = "Info,Version=1.0.0,Template=fx_category_template_EBAY_US"

COLUMNS = [
    "*Action(SiteID=US|Country=US|Currency=USD|Version=1193|CC=UTF-8)",
    "CustomLabel",
    "*Category",
    "StoreCategory",
    "*Title",
    "Subtitle",
    "Relationship",
    "*ConditionID",
    "*C:Graded",
    "*C:Game",
    "*C:Card Name",
    "*C:Card Type",
    "*C:Speciality",
    "*C:Character",
    "*C:Rarity",
    "*C:Finish",
    "*C:Attribute/MTG:Color",
    "*C:Manufacturer",
    "*C:Features",
    "*C:Set",
    "*C:Stage",
    "*C:Age Level",
    "*C:Card Size",
    "*C:Material",
    "*C:Convention/Event",
    "*C:Country/Region of Manufacture",
    "*C:Illustrator",
    "*C:HP",
    "*C:Attack/Power",
    "*C:Defense/Toughness",
    "CD:Grade - (ID: 27502)",
    "CD:Professional Grader - (ID: 27501)",
    "CD:Card Condition - (ID: 40001)",
    "*C:Card Number",
    "CDA:Certification Number - (ID: 27503)",
    "*C:Type",
    "C:Signed By",
    "C:Year Manufactured",
    "C:Language",
    "C:California Prop 65 Warning",
    "PicURL",
    "GalleryType",
    "*Description",
    "*Format",
    "*Duration",
    "*StartPrice",
    "BuyItNowPrice",
    "*Quantity",
    "PayPalAccepted",
    "PayPalEmailAddress",
    "ImmediatePayRequired",
    "PaymentInstructions",
    "*Location",
    "PostalCode",
    "ShippingType",
    "ShippingService-1:Option",
    "ShippingService-1:FreeShipping",
    "ShippingService-1:Cost",
    "ShippingService-1:AdditionalCost",
    "ShippingService-2:Option",
    "ShippingService-2:Cost",
    "*DispatchTimeMax",
    "PromotionalShippingDiscount",
    "ShippingDiscountProfileID",
    "*ReturnsAcceptedOption",
    "ReturnsWithinOption",
    "RefundOption",
    "ShippingCostPaidByOption",
    "AdditionalDetails",
    "ShippingProfileName",
    "ReturnProfileName",
    "PaymentProfileName",
    "TakeBackPolicyID",
    "ProductCompliancePolicyID",
    "ScheduleTime",
    "BestOfferEnabled",
    "MinimumBestOfferPrice",
    "BestOfferAutoAcceptPrice",
]

def normalize_slug(best_slug:str) -> str:
    split_slug = best_slug.split('-')
    split_slug = split_slug[1:] # remove pokemon
    split_slug = [s.capitalize() for s in split_slug]
    final = ' '.join(split_slug)
    return final

def read_jsonl(path: str) -> List[Dict[str, Any]]:
    out: List[Dict[str, Any]] = []
    with open(path, "r", encoding="utf-8") as f:
        for line in f:
            line = line.strip()
            if not line:
                continue
            out.append(json.loads(line))
    return out


def parse_boolish(v: Any) -> bool:
    """Return True if it means 'needs review'."""
    if v is None:
        return False
    if isinstance(v, bool):
        return v
    s = str(v).strip().lower()
    if s in ("1", "true", "t", "yes", "y"):
        return True
    if s in ("0", "false", "f", "no", "n", ""):
        return False
    return True  # unknown -> conservative


def parse_float(val: Any) -> Optional[float]:
    if val is None:
        return None
    if isinstance(val, (int, float)):
        return float(val)
    s = str(val).strip()
    if not s:
        return None
    s = s.replace("$", "").replace(",", "")
    s = re.sub(r"[^\d.]+", "", s)
    if not s:
        return None
    try:
        return float(s)
    except Exception:
        return None


def parse_int(val: Any) -> Optional[int]:
    if val is None or val == "":
        return None
    try:
        return int(float(str(val).strip()))
    except Exception:
        return None


def compute_raw_price(best_ungraded_price: float) -> float:
    """
    Base pricing rule (before cents formatting):
      - floor at 2.49
      - if < 5 => +1.50
      - if >= 5 => *1.25
    """
    m = max(2.49, float(best_ungraded_price))
    if m >= 5.0:
        return round(m * 1.25, 2)
    return round(m + 1.50, 2)


def pretty_cents_49_or_95(x: float) -> float:
    """
    cents 00–49 -> .49
    cents 50–99 -> .95
    """
    dollars = int(x)
    cents = int(round((x - dollars) * 100))
    if cents <= 49:
        return round(dollars + 0.49, 2)
    return round(dollars + 0.95, 2)


def build_title(best_name: str, best_set_slug: str, input_collector: str, condition="NM") -> str:
    parts = [best_name.strip(), input_collector, normalize_slug(best_set_slug).strip(), condition]
    final = " ".join([p for p in parts if p])
    if len(final) < 60:
        return final + " Pokemon Card"
    else:
        return final


def build_description(best_name: str, best_set_slug: str, best_number: str) -> str:
    return f"{best_name} from {best_set_slug}, card number {best_number} in NM condition."


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--manifest", default=DEFAULT_MANIFEST)
    ap.add_argument("--idents", default=DEFAULT_IDENTS)
    ap.add_argument("--match-review", default=DEFAULT_MATCH_REVIEW)
    ap.add_argument("--out", default=DEFAULT_OUT)

    ap.add_argument("--max-ungraded", type=float, default=20.0)
    ap.add_argument("--max-final", type=float, default=20.0)

    # eBay defaults (match your successful file)
    ap.add_argument("--category", default="183454")
    ap.add_argument("--store-category", default="0")
    ap.add_argument("--condition-id", default="4000")
    ap.add_argument("--card-condition", default="Near mint or better - (ID: 400010)")
    ap.add_argument("--location", default="rockville, md")
    ap.add_argument("--postal-code", default="20850")
    ap.add_argument("--dispatch-time", default="1")

    ap.add_argument("--shipping-profile", default="free_shipping_under_20")
    ap.add_argument("--return-profile", default="30_day_returns")
    ap.add_argument("--payment-profile", default="buy_it_now")

    # Match your successful file behavior
    ap.add_argument("--best-offer-enabled", default="0")
    ap.add_argument("--min-best-offer", default="")
    ap.add_argument("--auto-accept-best-offer", default="")

    # CustomLabel fixed value per your request
    ap.add_argument("--customlabel", default="batch-auto")

    args = ap.parse_args()

    manifest_rows = read_jsonl(args.manifest)
    ident_rows = read_jsonl(args.idents)

    # manifest/idents are 1-based listing_index
    man_by_idx: Dict[int, Dict[str, Any]] = {
        int(m["listing_index"]): m for m in manifest_rows if "listing_index" in m
    }
    ident_by_idx: Dict[int, Dict[str, Any]] = {
        int(r["listing_index"]): r for r in ident_rows if "listing_index" in r
    }

    # match_review idx is 0-based
    review_rows: List[Dict[str, Any]] = []
    with open(args.match_review, "r", encoding="utf-8", newline="") as f:
        reader = csv.DictReader(f)
        for row in reader:
            review_rows.append(row)

    approved_rows: List[List[str]] = []
    skipped: List[str] = []

    for r in review_rows:
        idx0 = parse_int(r.get("idx"))
        if idx0 is None:
            continue

        manifest_idx = idx0 + 1  # <-- OFF-BY-ONE FIX

        if parse_boolish(r.get("needs_review")):
            skipped.append(f"[idx0={idx0} -> listing_index={manifest_idx}] needs_review=true")
            #continue

        man = man_by_idx.get(manifest_idx)
        if not man:
            skipped.append(f"[idx0={idx0} -> listing_index={manifest_idx}] missing manifest row")
            continue

        front_url = (man.get("front_url") or "").strip()
        back_url = (man.get("back_url") or "").strip()
        if not front_url or not back_url:
            skipped.append(f"[idx0={idx0} -> listing_index={manifest_idx}] missing front_url/back_url")
            continue

        # build the ebay title
        best_name = (r.get("best_name") or "").strip()
        best_number = (r.get("best_number") or "").strip()
        best_set_slug = (r.get("best_set_slug") or "").strip()
        input_collector = (r.get("input_collector") or "").strip()

        if not best_name or not best_number or not best_set_slug:
            skipped.append(f"[idx0={idx0} -> listing_index={manifest_idx}] missing best fields")
            continue

        ungraded = parse_float(r.get("best_ungraded_price"))
        if ungraded is None:
            skipped.append(f"[idx0={idx0}] missing/invalid best_ungraded_price")
            continue
        if ungraded >= args.max_ungraded:
            skipped.append(f"[idx0={idx0}] ungraded {ungraded:.2f} >= {args.max_ungraded:.2f}")
            continue

        raw_price = compute_raw_price(ungraded)
        final_price = pretty_cents_49_or_95(raw_price)
        if final_price >= args.max_final:
            skipped.append(f"[idx0={idx0}] final {final_price:.2f} >= {args.max_final:.2f}")
            continue

        # Fallback fields from ident file (also 1-based listing_index)
        language = (ident_by_idx.get(manifest_idx, {}).get("language") or "").strip()
        year_manufactured = parse_int(r.get("copyright_year"))
        if year_manufactured is None:
            year_manufactured = parse_int(ident_by_idx.get(manifest_idx, {}).get("copyright_year"))

        title = build_title(best_name, best_set_slug, input_collector)
        desc = build_description(best_name, best_set_slug, best_number)

        # EXACT delimiter to match your successful file
        picurl = f"{front_url} | {back_url}"

        row = {c: "" for c in COLUMNS}
        row.update({
            COLUMNS[0]: "Add",
            "CustomLabel": args.customlabel,  # same for all rows
            "*Category": args.category,
            "StoreCategory": args.store_category,
            "*Title": title,
            "*ConditionID": args.condition_id,
            "*C:Game": "Pokémon TCG",
            "*C:Card Name": best_name,
            "*C:Character": best_name,
            "*C:Set": best_set_slug,
            "CD:Card Condition - (ID: 40001)": args.card_condition,
            "*C:Card Number": best_number,
            "C:Year Manufactured": "" if year_manufactured is None else str(year_manufactured),
            "C:Language": language,
            "PicURL": picurl,
            "GalleryType": "",
            "*Description": desc,
            "*Format": "FixedPrice",
            "*Duration": "GTC",
            "*StartPrice": f"{final_price:.2f}",
            "BuyItNowPrice": "0",
            "*Quantity": "1",
            "*Location": args.location,
            "PostalCode": args.postal_code,
            "*DispatchTimeMax": args.dispatch_time,
            "ShippingProfileName": args.shipping_profile,
            "ReturnProfileName": args.return_profile,
            "PaymentProfileName": args.payment_profile,
            "BestOfferEnabled": args.best_offer_enabled,
            "MinimumBestOfferPrice": args.min_best_offer,
            "BestOfferAutoAcceptPrice": args.auto_accept_best_offer,
        })

        approved_rows.append([row.get(c, "") for c in COLUMNS])

    if not approved_rows:
        print("No rows qualified. Nothing written.")
        print("Some skip reasons:")
        for s in skipped[:30]:
            print(" ", s)
        return

    os.makedirs(os.path.dirname(args.out) or ".", exist_ok=True)

    # Match your successful file: first line unquoted; header/rows quoted
    with open(args.out, "w", encoding="utf-8", newline="") as f:
        f.write(INFO_LINE + "\n")
        w = csv.writer(f, quoting=csv.QUOTE_ALL)
        w.writerow(COLUMNS)
        w.writerows(approved_rows)

    print(f"Wrote: {args.out}")
    print(f"Approved: {len(approved_rows)}")
    print(f"Skipped: {len(skipped)}")
    if skipped:
        print("Top skip reasons:")
        for s in skipped[:30]:
            print(" ", s)


if __name__ == "__main__":
    main()

