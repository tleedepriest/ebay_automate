import os
import json
import sqlite3
from rapidfuzz import fuzz
from lookup_pc_fuzzy import lookup_best_match

DB_PATH = "pricecharting.db"
IN_JSONL = "tmp/card_identifications.jsonl"
OUT_JSONL = "tmp/card_matches.jsonl"


def normalize_number(num: str) -> str:
    num = (num or "").strip()
    if "/" in num:
        num = num.split("/", 1)[0].strip()
    return num


def lookup_best_match(db_path: str, card_name: str, card_number: str, top_k: int = 3):
    """
    Filter candidates by exact card_number (numerator), then fuzzy-rank by card_name.
    Returns list of dicts, best first.
    """
    n = normalize_number(card_number)
    print(n)
    n=53
    if not n:
        return []

    con = sqlite3.connect(db_path)
    cur = con.cursor()

    cur.execute(
        """
        SELECT card_name, card_number, card_url, ungraded_price, grade9_price, psa10_price
        FROM cards
        WHERE card_number = ?
        """,
        (n,),
    )

    candidates = cur.fetchall()
    con.close()

    if not candidates:
        return []

    scored = []
    for cname, cnum, url, ungraded, g9, psa10 in candidates:
        score = fuzz.WRatio(card_name, cname)
        scored.append((score, cname, cnum, url, ungraded, g9, psa10))

    scored.sort(reverse=True, key=lambda x: x[0])

    out = []
    for score, cname, cnum, url, ungraded, g9, psa10 in scored[:top_k]:
        out.append(
            {
                "score": score,
                "card_name": cname,
                "card_number": cnum,
                "card_url": url,
                "ungraded_price": ungraded,
                "grade9_price": g9,
                "psa10_price": psa10,
            }
        )
    return out


def run():
    os.makedirs("tmp", exist_ok=True)

    with open(IN_JSONL, "r", encoding="utf-8") as fin, open(OUT_JSONL, "w", encoding="utf-8") as fout:
        for line in fin:
            rec = json.loads(line)

            img = rec.get("image")
            name = rec.get("card_name", "")
            number = rec.get("collector_number", "")

            matches = lookup_best_match(DB_PATH, name, number, top_k=10)

            out = {
                "image": img,
                "input_card_name": name,
                "input_collector_number": number,
                "matches": matches,
                "best": matches[0] if matches else None,
            }

            fout.write(json.dumps(out) + "\n")

            if out["best"]:
                print(f"OK  {img}: {out['best']['card_name']} #{out['best']['card_number']}  "
                      f"${out['best']['ungraded_price']}  score={out['best']['score']}")
            else:
                print(f"MISS {img}: {name} #{number}")

    print("Wrote", OUT_JSONL)


if __name__ == "__main__":
    run()

