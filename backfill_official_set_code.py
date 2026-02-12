import re
import time
import sqlite3
import requests
from bs4 import BeautifulSoup

DB="pricecharting.db"
HEADERS={"User-Agent":"Mozilla/5.0","Accept-Language":"en-US,en;q=0.9"}

def fetch_set_code(set_url: str) -> str | None:
    r = requests.get(set_url, headers=HEADERS, timeout=30)
    r.raise_for_status()
    soup = BeautifulSoup(r.text, "html.parser")

    img = soup.select_one("img.set-logo[alt^='Set Code:'], img.set-logo[title^='Set Code:']")
    if not img:
        return None

    txt = img.get("alt") or img.get("title") or ""
    # "Set Code: JTG" -> "JTG"
    m = re.search(r"Set Code:\s*([A-Z0-9\-]+)", txt)
    return m.group(1).strip() if m else None

def main():
    con = sqlite3.connect(DB)
    cur = con.cursor()

    # Get distinct set_urls we’ve ingested
    cur.execute("""
        SELECT DISTINCT set_url
        FROM cards
        WHERE set_url IS NOT NULL AND set_url != ''
    """)
    set_urls = [r[0] for r in cur.fetchall()]
    print("Distinct sets:", len(set_urls))

    # Build mapping set_url -> set_code
    for i, set_url in enumerate(set_urls, start=1):
        # skip if already filled (fast resume)
        cur.execute("SELECT 1 FROM cards WHERE set_url=? AND set_code IS NOT NULL AND set_code != '' LIMIT 1", (set_url,))
        if cur.fetchone() or "japanese" in set_url:
            continue

        try:
            code = fetch_set_code(set_url)
            if not code:
                print(f"[{i}] NO SET CODE FOUND: {set_url}")
                continue

            cur.execute("UPDATE cards SET set_code=? WHERE set_url=?", (code, set_url))
            con.commit()
            print(f"[{i}] {code}  <- {set_url}")

            time.sleep(0.3)  # be polite

        except Exception as e:
            print(f"[{i}] ERROR {set_url}: {e}")
            time.sleep(1.0)

    con.close()
    print("Done.")

if __name__ == "__main__":
    main()

