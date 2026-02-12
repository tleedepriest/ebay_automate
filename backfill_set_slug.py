# backfill_set_slug.py
import sqlite3
from urllib.parse import urlparse

DB="pricecharting.db"

def slug_from_set_url(set_url: str) -> str:
    path = urlparse(set_url or "").path
    parts = [p for p in path.split("/") if p]
    return parts[-1] if parts else ""

con = sqlite3.connect(DB)
cur = con.cursor()

cur.execute("SELECT card_url, set_url FROM cards WHERE set_slug IS NULL OR set_slug=''")
rows = cur.fetchall()

for card_url, set_url in rows:
    slug = slug_from_set_url(set_url)
    cur.execute("UPDATE cards SET set_slug=? WHERE card_url=?", (slug, card_url))

# default language if you want
#cur.execute("UPDATE cards SET language=COALESCE(NULLIF(language,''), 'English')")

con.commit()
con.close()
print("Backfilled set_slug (+ language default).")

