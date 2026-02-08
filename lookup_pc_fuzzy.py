import re
import sqlite3
from rapidfuzz import fuzz

def extract_int(num_text: str):
    """
    Pull the first integer from strings like:
      '053/182' -> 53
      'SV1V 053' -> 53
      '#245' -> 245
    """
    if not num_text:
        return None
    m = re.search(r"(\d{1,4})", str(num_text))
    if not m:
        return None
    return int(m.group(1))

def normalize_int_str(n: int | None):
    return str(n) if n is not None else ""

def number_similarity(input_num_text: str, candidate_num_text: str) -> int:
    """
    Score number match 0..100.
    Compares extracted ints primarily, but falls back to fuzzy on raw strings.
    """
    a = extract_int(input_num_text)
    b = extract_int(candidate_num_text)

    if a is not None and b is not None:
        # exact int match is best
        if a == b:
            return 100
        # close numbers get some credit (rarely needed, but harmless)
        diff = abs(a - b)
        if diff <= 2:
            return 70
        if diff <= 5:
            return 40
        return 0

    # fallback: raw fuzzy
    return fuzz.partial_ratio(str(input_num_text), str(candidate_num_text))

def fetch_candidates(con, input_name: str, input_number_text: str, limit: int = 500):
    """
    Get a candidate pool from SQLite.
    Strategy:
      - If we can extract an int n:
          pull rows where card_number = n OR card_number like '%n' OR card_number like '%#n%' etc
        (we store just '245' typically, but some sets might have '053' in DB depending on parsing)
      - Also add a name-based pool (top rows by LIKE token) as fallback.
    """
    cur = con.cursor()
    n = extract_int(input_number_text)

    candidates = []

    if n is not None:
        n_str = str(n)
        # broaden match: '53', '053', '153' won't match equals, but LIKE can catch bad formatting
        cur.execute(
            """
            SELECT card_name, card_number, card_url, ungraded_price, grade9_price, psa10_price
            FROM cards
            WHERE card_number = ?
               OR card_number = ?
               OR card_number LIKE ?
               OR card_number LIKE ?
            LIMIT ?
            """,
            (
                n_str,
                n_str.zfill(3),           # handle '053' stored (just in case)
                f"%{n_str}%",             # broad catch-all
                f"%{n_str.zfill(3)}%",
                limit,
            ),
        )
        candidates.extend(cur.fetchall())

    # If number gave nothing (or you want extra recall), add a name-based pool:
    # use the first word token to avoid scanning everything
    tok = (input_name or "").strip().split(" ")[0] if input_name else ""
    if tok:
        cur.execute(
            """
            SELECT card_name, card_number, card_url, ungraded_price, grade9_price, psa10_price
            FROM cards
            WHERE card_name LIKE ?
            LIMIT ?
            """,
            (f"%{tok}%", limit),
        )
        candidates.extend(cur.fetchall())

    # de-dupe by card_url
    dedup = {}
    for row in candidates:
        url = row[2]
        dedup[url] = row
    return list(dedup.values())

def lookup_best_match(db_path: str, card_name: str, card_number_text: str, top_k: int = 5):
    """
    Rank by combined score:
      80% name similarity + 20% number similarity
    Returns top_k matches.
    """
    con = sqlite3.connect(db_path)
    try:
        candidates = fetch_candidates(con, card_name, card_number_text, limit=800)
    finally:
        con.close()

    if not candidates:
        return []

    scored = []
    for cname, cnum, url, ungraded, g9, psa10 in candidates:
        name_score = fuzz.WRatio(card_name, cname) if card_name else 0
        num_score = number_similarity(card_number_text, cnum)

        combined = int(round(0.80 * name_score + 0.20 * num_score))

        scored.append((combined, name_score, num_score, cname, cnum, url, ungraded, g9, psa10))

    scored.sort(reverse=True, key=lambda x: x[0])

    out = []
    for combined, name_score, num_score, cname, cnum, url, ungraded, g9, psa10 in scored[:top_k]:
        out.append({
            "score": combined,
            "name_score": name_score,
            "number_score": num_score,
            "card_name": cname,
            "card_number": cnum,
            "card_url": url,
            "ungraded_price": ungraded,
            "grade9_price": g9,
            "psa10_price": psa10,
        })

    return out

