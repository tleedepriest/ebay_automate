# lookup_pc_fuzzy.py
import re
import sqlite3
from rapidfuzz import fuzz

# -----------------------
# Helpers
# -----------------------

def extract_x(collector_text):
    """
    "103/165" -> 103
    "#245" -> 245
    """
    if not collector_text:
        return None
    m = re.search(r"(\d{1,4})", str(collector_text))
    return int(m.group(1)) if m else None

# -----------------------
# Candidate set filtering
# -----------------------

def get_candidate_set_slugs(con,
                            set_size: int | None,
                            copyright_year: int | None):
    where = ["language = 'English'"]
    params = []

    if set_size is not None:
        where.append("base_total = ?")
        params.append(int(set_size))

    if copyright_year is not None:
        where.append("(released_year = ? OR released_year = ?)")
        params.extend([int(copyright_year), int(copyright_year) - 1])

    sql = f"""
        SELECT set_slug
        FROM set_meta
        WHERE {" AND ".join(where)}
    """

    cur = con.cursor()
    cur.execute(sql, params)
    return [r[0] for r in cur.fetchall()]

# -----------------------
# Card lookup
# -----------------------

def fetch_candidates(con,
                     set_slugs,
                     num_x):

    if not set_slugs or num_x is None:
        return []

    placeholders = ",".join(["?"] * len(set_slugs))
    n = str(num_x)
    n3 = n.zfill(3)
    print(n)
    print(n3)
    print(set_slugs)
    
    cur = con.cursor()
    cur.execute(f"""
        SELECT card_name, card_number, card_url,
               ungraded_price, grade9_price, psa10_price,
               set_slug, set_url
        FROM cards
        WHERE set_slug in ({placeholders})
          AND (
                card_number = ?
             OR card_number = ?
             OR card_number LIKE ?
             OR card_number LIKE ?
          )""", (*set_slugs, n, n3, f"%{n}%", f"%{n3}%"))
    
    rows = cur.fetchall()
    #print(rows)
    dedup = {}
    for r in rows:
        dedup[r[2]] = r
    return list(dedup.values())

# -----------------------
# Ranking
# -----------------------

def rank_candidates(card_name, num_x, candidates, top_k=10):
    scored = []
    for cname, cnum, url, ungraded, g9, psa10, set_slug, set_url in candidates:
        name_score = fuzz.WRatio(card_name, cname) if card_name else 0
        cand_x = extract_x(cnum)
        num_score = 100 if cand_x == num_x else 0

        combined = int(round(0.85 * name_score + 0.15 * num_score))

        scored.append((combined, cname, cnum, url, ungraded, g9, psa10, set_slug, set_url))

    scored.sort(reverse=True, key=lambda x: x[0])

    out = []
    for combined, cname, cnum, url, ungraded, g9, psa10, set_slug, set_url in scored[:top_k]:
        out.append({
            "score": combined,
            "card_name": cname,
            "card_number": cnum,
            "card_url": url,
            "ungraded_price": ungraded,
            "grade9_price": g9,
            "psa10_price": psa10,
            "set_slug": set_slug,
            "set_url": set_url,
        })
    return out

# -----------------------
# Public API
# -----------------------

def lookup_best_match(db_path,
                      card_name,
                      collector_number,
                      set_size,
                      copyright_year,
                      top_k=10):

    num_x = extract_x(collector_number)

    con = sqlite3.connect(db_path)
    try:
        set_slugs = get_candidate_set_slugs(
            con,
            set_size=set_size,
            copyright_year=copyright_year
        )
        candidates = fetch_candidates(
            con,
            set_slugs=set_slugs,
            num_x=num_x
        )
    finally:
        con.close()

    if not candidates:
        return []

    return rank_candidates(card_name, num_x, candidates, top_k)

