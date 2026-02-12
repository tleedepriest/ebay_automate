import csv
import sqlite3
from set_map import SET_MAP  # { "pokemon-scarlet-&-violet-151": "Scarlet & Violet - 151 Set", ... }

DB_PATH = "pricecharting.db"
DENOMS_CSV = "pokellector_set_denoms.csv"
TABLE = "set_meta"


def strip_set_suffix(s: str) -> str:
    s = (s or "").strip()
    return s[:-4].strip() if s.endswith(" Set") else s


def digits_int(x):
    if x is None:
        return None
    s = str(x).strip()
    if not s:
        return None
    d = "".join(ch for ch in s if ch.isdigit())
    return int(d) if d else None


def first_nonempty(*vals):
    for v in vals:
        if v is None:
            continue
        s = str(v).strip()
        if s:
            return s
    return None


def load_denoms_csv(path: str):
    """
    Loads pokellector_set_denoms.csv into dict keyed by title.
    We accept either:
      - titles that include trailing " Set"
      - titles without it
    """
    with open(path, "r", encoding="utf-8-sig", newline="") as f:
        r = csv.DictReader(f)
        headers = r.fieldnames or []
        rows = list(r)

    # Find title column
    title_candidates = ["title", "set_title", "set", "name", "pokellector_set_name"]
    title_col = next((c for c in title_candidates if c in headers), None)
    if not title_col:
        raise RuntimeError(f"Could not find title column in {path}. Headers: {headers}")

    by_title = {}
    for row in rows:
        t = (row.get(title_col) or "").strip()
        if not t:
            continue
        by_title[t] = row

    return by_title, headers, title_col


def main():
    poke_by_title, headers, title_col = load_denoms_csv(DENOMS_CSV)
    print(f"Loaded {len(poke_by_title)} pokellector rows. title_col={title_col}")

    con = sqlite3.connect(DB_PATH)
    try:
        cur = con.cursor()

        # Hard check schema (consistency)
        cur.execute(f"PRAGMA table_info({TABLE});")
        cols = [r[1] for r in cur.fetchall()]
        required = [
            "set_slug",
            "pc_list_a_name",
            "pokellector_set_name",
            "pokellector_url",
            "base_total",
            "secret_total",
            "released_md",
            "released_year",
            "released_raw",
            "language",
        ]
        missing = [c for c in required if c not in cols]
        if missing:
            raise RuntimeError(f"{TABLE} missing columns: {missing}\nHave: {cols}")

        # Slugs present in DB
        cur.execute(f"SELECT set_slug FROM {TABLE} WHERE set_slug IS NOT NULL AND set_slug != ''")
        slugs_in_db = set(r[0] for r in cur.fetchall())

        updated = 0
        miss_slug = []
        miss_title = []

        for pc_slug, pok_title in SET_MAP.items():
            if pc_slug not in slugs_in_db:
                miss_slug.append(pc_slug)
                continue

            row = poke_by_title.get(pok_title)
            if row is None:
                row = poke_by_title.get(strip_set_suffix(pok_title))

            if row is None:
                miss_title.append((pc_slug, pok_title))
                continue

            base_total = digits_int(first_nonempty(row.get("base_total"), row.get("cards"), row.get("set_size")))
            secret_total = digits_int(first_nonempty(row.get("secret_total"), row.get("secret"), row.get("secrets")))
            released_md = first_nonempty(row.get("released_md"), row.get("released"), row.get("release_md"))
            released_year = digits_int(first_nonempty(row.get("released_year"), row.get("year"), row.get("release_year")))
            released_raw = first_nonempty(row.get("released_raw"), row.get("released_raw_text"))
            pok_url = first_nonempty(row.get("pokellector_url"), row.get("url"), row.get("href"), row.get("set_url"))

            cur.execute(
                f"""
                UPDATE {TABLE}
                SET
                    pokellector_set_name = ?,
                    pokellector_url = COALESCE(?, pokellector_url),
                    base_total = COALESCE(?, base_total),
                    secret_total = COALESCE(?, secret_total),
                    released_md = COALESCE(?, released_md),
                    released_year = COALESCE(?, released_year),
                    released_raw = COALESCE(?, released_raw),
                    language = COALESCE(?, language)
                WHERE set_slug = ?
                """,
                (
                    pok_title,        # store EXACT mapping value
                    pok_url,
                    base_total,
                    secret_total,
                    released_md,
                    released_year,
                    released_raw,
                    "English",
                    pc_slug,
                )
            )
            updated += 1

        con.commit()

        print("\n--- Summary ---")
        print("SET_MAP keys:", len(SET_MAP))
        print("Updated rows:", updated)
        print("Missing slugs in set_meta:", len(miss_slug))
        print("Missing titles in denoms CSV:", len(miss_title))

        if miss_slug:
            print("\nSample missing slugs (first 20):")
            for s in miss_slug[:20]:
                print(" -", s)

        if miss_title:
            print("\nSample missing titles (first 20):")
            for s, t in miss_title[:20]:
                print(" -", s, "->", t)

    finally:
        con.close()


if __name__ == "__main__":
    main()

