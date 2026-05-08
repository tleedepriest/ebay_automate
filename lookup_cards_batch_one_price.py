#!/usr/bin/env python3
"""
lookup_cards_batch_price.py

Plugin replacement for lookup_cards.py when you do NOT want to search/match cards.
It assigns a single batch "median" price to every card and preserves the same
output formats so the rest of your pipeline can run unchanged.

Inputs:
  - tmp/card_identifications.jsonl

Outputs (same as lookup_cards.py):
  - tmp/card_matches.jsonl
  - tmp/match_review.csv

Key idea:
  - best and matches are "synthetic" objects but keep required fields:
      best.ungraded_price is the batch median price
      best.card_name, best.card_number, best.set_slug exist (best-effort from input)
"""

import os
import json
import csv
import argparse
from typing import Any, Dict, Optional


IN_JSONL = "tmp/card_identifications.jsonl"
OUT_JSONL = "tmp/card_matches.jsonl"
REVIEW_CSV = "tmp/match_review.csv"


def to_int(x) -> Optional[int]:
    try:
        if x is None:
            return None
        if isinstance(x, int):
            return x
        s = str(x).strip()
        if not s:
            return None
        return int(float(s))
    except Exception:
        return None


def to_float(x) -> Optional[float]:
    try:
        if x is None:
            return None
        if isinstance(x, (int, float)):
            return float(x)
        s = str(x).strip()
        if not s:
            return None
        return float(s)
    except Exception:
        return None


def run(
    median_price: float,
    start_at: int = 0,
    needs_review: bool = False,
    set_slug: str = "",
    note: str = "",
):
    os.makedirs("tmp", exist_ok=True)

    review_rows = []

    with open(IN_JSONL, "r", encoding="utf-8") as fin, open(OUT_JSONL, "w", encoding="utf-8") as fout:
        for idx, line in enumerate(fin):
            if idx < start_at:
                continue

            line = line.strip()
            if not line:
                continue

            rec: Dict[str, Any] = json.loads(line)

            img = rec.get("image") or rec.get("front_local") or rec.get("path")
            name = (rec.get("card_name") or "").strip()
            collector_number = (rec.get("collector_number") or "").strip()
            set_size = to_int(rec.get("set_size"))
            year = to_int(rec.get("copyright_year"))
            oai_conf = to_float(rec.get("confidence"))
            language = rec.get("language")

            # Synthetic "best" match object with the fields your downstream expects.
            best = {
                "card_name": name or (rec.get("set_name") or "Unknown Card"),
                "card_number": collector_number or "",
                "set_slug": set_slug or (rec.get("set_code") or rec.get("set_name") or ""),
                "ungraded_price": float(median_price),
                "score": 100,  # treat as confident since we're bypassing lookup
                "card_url": "",  # unknown / not applicable
                "note": note,    # extra metadata (safe; downstream can ignore)
                "synthetic": True,
            }

            matches = [best]  # keep list type consistent

            out = {
                "image": img,
                "input": {
                    "card_name": name,
                    "collector_number": collector_number,
                    "set_size": set_size,
                    "copyright_year": year,
                    "confidence": oai_conf,
                    "language": language,
                    "set_name_hint": rec.get("set_name"),
                    "set_code_hint": rec.get("set_code"),
                },
                "best": best,
                "matches": matches,
                "needs_review": bool(needs_review),
            }

            fout.write(json.dumps(out, ensure_ascii=False) + "\n")

            review_rows.append({
                "idx": idx,
                "image": img,
                "input_name": name,
                "input_collector": collector_number,
                "set_size": set_size,
                "copyright_year": year,
                "oai_conf": oai_conf,
                "best_name": best.get("card_name", ""),
                "best_number": best.get("card_number", ""),
                "best_set_slug": best.get("set_slug", ""),
                "best_ungraded_price": best.get("ungraded_price", ""),
                "best_score": best.get("score", ""),
                "best_url": best.get("card_url", ""),
                "needs_review": bool(needs_review),
            })

            tag = " **REVIEW**" if needs_review else ""
            print(f"[{idx}] BATCH_PRICE {img}: ${median_price:.2f}{tag}")

    # Write review CSV (same columns)
    if review_rows:
        with open(REVIEW_CSV, "w", encoding="utf-8", newline="") as f:
            w = csv.DictWriter(f, fieldnames=list(review_rows[0].keys()))
            w.writeheader()
            w.writerows(review_rows)

    print("Wrote:", OUT_JSONL)
    print("Wrote:", REVIEW_CSV)


def main():
    ap = argparse.ArgumentParser(description="Bypass lookup and assign a batch median price to every card.")
    ap.add_argument("--median", required=True, type=float, help="Median (ungraded) price to assign to every card.")
    ap.add_argument("--start-at", default=0, type=int, help="Resume from 0-based line index in card_identifications.jsonl.")
    ap.add_argument("--needs-review", default="false", help="true/false. Default false (so rows are auto-approved).")
    ap.add_argument("--set-slug", default="", help="Optional set slug label to store in best_set_slug.")
    ap.add_argument("--note", default="", help="Optional note stored in the synthetic best object.")
    args = ap.parse_args()

    needs_review = str(args.needs_review).strip().lower() in ("1", "true", "t", "yes", "y")

    run(
        median_price=float(args.median),
        start_at=int(args.start_at),
        needs_review=needs_review,
        set_slug=args.set_slug,
        note=args.note,
    )


if __name__ == "__main__":
    main()

