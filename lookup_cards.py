# lookup_cards.py
#
# Reads OpenAI identifications from JSONL, matches against pricecharting.db using
# deterministic filters:
#   - set_meta.base_total == set_size (from OpenAI)
#   - set_meta.released_year in {copyright_year, copyright_year-1}
#   - cards.card_number matches numerator X from collector_number
# Then fuzzy-matches card_name only to break ties.
#
# Outputs:
#   tmp/card_matches.jsonl  (full match objects)
#   tmp/match_review.csv    (easy manual review sheet)

import os
import json
import csv
from typing import Any, Dict, Optional

from lookup_pc_fuzzy import lookup_best_match

DB_PATH = "pricecharting.db"
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
        return int(s)
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
    top_k: int = 10,
    min_name_score: int = 70,
    start_at: int = 0,
):
    """
    min_name_score: if best match score is below this, flag for review.
                    (Score here is name-heavy; tune after first run.)
    start_at: resume from a line index in IN_JSONL (0-based).
    """
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

            matches = lookup_best_match(
                DB_PATH,
                card_name=name,
                collector_number=collector_number,
                set_size=set_size,
                copyright_year=year,
                top_k=top_k,
            )

            best = matches[0] if matches else None

            # Review logic: deterministic pipeline should prefer "no match" over wrong match
            needs_review = (
                best is None
                or best.get("score", 0) < min_name_score
                or set_size is None
                or year is None
                or not collector_number
                or not name
            )

            out = {
                "image": img,
                "input": {
                    "card_name": name,
                    "collector_number": collector_number,
                    "set_size": set_size,
                    "copyright_year": year,
                    "confidence": oai_conf,
                    "language": rec.get("language"),
                    "set_name_hint": rec.get("set_name"),
                    "set_code_hint": rec.get("set_code"),
                },
                "best": best,
                "matches": matches,
                "needs_review": needs_review,
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
                "best_name": best.get("card_name") if best else "",
                "best_number": best.get("card_number") if best else "",
                "best_set_slug": best.get("set_slug") if best else "",
                "best_ungraded_price": best.get("ungraded_price") if best else "",
                "best_score": best.get("score") if best else "",
                "best_url": best.get("card_url") if best else "",
                "needs_review": needs_review,
            })

            if best:
                tag = " **REVIEW**" if needs_review else ""
                print(
                    f"[{idx}] OK  {img}: {best['card_name']} #{best['card_number']} "
                    f"{best.get('set_slug','')}  ${best.get('ungraded_price')}  score={best['score']}{tag}"
                )
            else:
                print(f"[{idx}] MISS {img}: {name} {collector_number}  **REVIEW**")

    # Write review CSV
    if review_rows:
        with open(REVIEW_CSV, "w", encoding="utf-8", newline="") as f:
            w = csv.DictWriter(f, fieldnames=list(review_rows[0].keys()))
            w.writeheader()
            w.writerows(review_rows)

    print("Wrote:", OUT_JSONL)
    print("Wrote:", REVIEW_CSV)


if __name__ == "__main__":
    run()

