#!/usr/bin/env python3
"""refresh_set_prices.py

Refresh PriceCharting prices for the sets involved in a batch, so lookups use
current prices instead of stale ones.

It reads the batch identifications (tmp/card_identifications.jsonl), figures out
which PriceCharting sets those cards could belong to (same set_size + copyright
year filter the matcher uses), then scrapes each of those sets ONCE and upserts
fresh prices into pricecharting.db. Because upsert_cards keys on card_url with
ON CONFLICT, existing rows just get their prices (and image/name/number) updated;
set_slug/set_code/set_name/language are left untouched.

Run this between `identify` and `lookup` so the match prices are fresh.

Examples:
  # Refresh every set the current batch's cards could belong to:
  python refresh_set_prices.py

  # Refresh specific sets only (skip the identifications file):
  python refresh_set_prices.py --slugs pokemon-base-set,pokemon-fossil
"""

import argparse
import json
import os
import sqlite3
import sys
from typing import List, Optional, Set

# Reuse the proven scraping + upsert path from the index builder.
from build_master_pricecharting_index_scroll import (
    scrape_set_with_retry,
    parse_cards_from_html,
    upsert_cards,
)
# Reuse the exact candidate-set logic the matcher uses, so we refresh the same
# sets that lookup will consider.
from lookup_pc_fuzzy import get_candidate_set_slugs

DB_PATH = "pricecharting.db"
IN_IDENTS = "tmp/card_identifications.jsonl"
DEFAULT_CHROMEDRIVER = "chromedriver-linux64/chromedriver"
PC_CONSOLE_BASE = "https://www.pricecharting.com/console/"


def _to_int(x) -> Optional[int]:
    try:
        if x is None:
            return None
        if isinstance(x, int):
            return x
        s = str(x).strip()
        return int(s) if s else None
    except Exception:
        return None


def candidate_slugs_for_idents(con, idents_path: str) -> List[str]:
    """Union of candidate set slugs across every identification in the batch."""
    slugs: Set[str] = set()
    with open(idents_path, "r", encoding="utf-8") as fin:
        for line in fin:
            line = line.strip()
            if not line:
                continue
            rec = json.loads(line)
            set_size = _to_int(rec.get("set_size"))
            year = _to_int(rec.get("copyright_year"))
            # Skip records the matcher itself would treat as unmatchable.
            if set_size is None and year is None:
                continue
            for slug in get_candidate_set_slugs(con, set_size=set_size, copyright_year=year):
                slugs.add(slug)
    return sorted(slugs)


def set_url_for_slug(con, slug: str) -> str:
    """Use the set_url already stored for this slug; fall back to the slug form."""
    row = con.execute(
        "SELECT set_url FROM cards WHERE set_slug = ? AND set_url IS NOT NULL LIMIT 1",
        (slug,),
    ).fetchone()
    if row and row[0]:
        return row[0]
    return f"{PC_CONSOLE_BASE}{slug}"


def refresh_slugs(
    con,
    slugs: List[str],
    chromedriver_path: str = DEFAULT_CHROMEDRIVER,
    headless: bool = True,
    chrome_binary: Optional[str] = None,
) -> None:
    if not slugs:
        print("No sets to refresh.")
        return

    print(f"Refreshing prices for {len(slugs)} set(s):")
    for s in slugs:
        print(f"  - {s}")

    for i, slug in enumerate(slugs, start=1):
        set_url = set_url_for_slug(con, slug)
        print(f"[{i}/{len(slugs)}] {slug} | {set_url}")
        try:
            html = scrape_set_with_retry(
                set_url=set_url,
                chromedriver_path=chromedriver_path,
                headless=headless,
                chrome_binary=chrome_binary,
                max_attempts=3,
            )
            cards = parse_cards_from_html(set_url, html)
            upsert_cards(con, cards)
            print(f"  updated {len(cards)} cards")
        except Exception as e:
            print(f"  ERROR refreshing {slug}: {e}")


def parse_args():
    p = argparse.ArgumentParser(description="Refresh PriceCharting prices for a batch's sets.")
    p.add_argument("--idents", default=IN_IDENTS,
                   help="identifications JSONL to derive sets from (default: %(default)s)")
    p.add_argument("--slugs", default="",
                   help="comma-separated set slugs to refresh instead of deriving from --idents")
    p.add_argument("--db", default=DB_PATH, help="sqlite db path (default: %(default)s)")
    p.add_argument("--chromedriver", default=DEFAULT_CHROMEDRIVER,
                   help="path to chromedriver (default: %(default)s)")
    p.add_argument("--chrome-binary", default=None, help="optional non-system chrome binary")
    p.add_argument("--no-headless", action="store_true", help="run a visible browser")
    return p.parse_args()


def main():
    args = parse_args()
    con = sqlite3.connect(args.db)
    try:
        if args.slugs.strip():
            slugs = sorted({s.strip() for s in args.slugs.split(",") if s.strip()})
        else:
            if not os.path.exists(args.idents):
                print(f"ERROR: missing {args.idents}. Run identify first, or pass --slugs.")
                sys.exit(1)
            slugs = candidate_slugs_for_idents(con, args.idents)

        refresh_slugs(
            con,
            slugs,
            chromedriver_path=args.chromedriver,
            headless=not args.no_headless,
            chrome_binary=args.chrome_binary,
        )
    finally:
        con.close()


if __name__ == "__main__":
    main()
