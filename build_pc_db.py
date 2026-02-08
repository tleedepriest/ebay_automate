import csv
import time
import re
import sqlite3
import requests
from bs4 import BeautifulSoup
from urllib.parse import urljoin

HEADERS = {"User-Agent": "Mozilla/5.0", "Accept-Language": "en-US,en;q=0.9"}
PRICE_RE = re.compile(r"\$?\s*([0-9]+(?:\.[0-9]{1,2})?)")

def parse_price(text):
    if not text:
        return None
    m = PRICE_RE.search(text)
    return float(m.group(1)) if m else None

def init_db(db_path="pricecharting.db"):
    con = sqlite3.connect(db_path)
    cur = con.cursor()
    cur.execute("""
    CREATE TABLE IF NOT EXISTS cards (
        card_url TEXT PRIMARY KEY,
        set_url TEXT,
        product_id TEXT,
        card_name TEXT,
        card_number TEXT,
        image_url TEXT,
        ungraded_price REAL,
        grade9_price REAL,
        psa10_price REAL
    )
    """)
    cur.execute("CREATE INDEX IF NOT EXISTS idx_cards_number ON cards(card_number)")
    con.commit()
    return con

def scrape_set_cards(set_url):
    r = requests.get(set_url, headers=HEADERS, timeout=30)
    r.raise_for_status()
    soup = BeautifulSoup(r.text, "html.parser")

    table = soup.select_one("table#games_table")
    if not table:
        return []

    out = []
    for tr in table.select("tbody tr[data-product]"):
        product_id = (tr.get("data-product") or "").strip()

        a = tr.select_one("td.title a[href]")
        if not a:
            continue

        card_text = a.get_text(" ", strip=True)  # "Gardevoir ex #245"
        card_url = urljoin(set_url, a.get("href", ""))

        img = tr.select_one("td.image img")
        image_url = (img.get("src") or "").strip() if img else ""

        def get_price(sel):
            el = tr.select_one(sel)
            return parse_price(el.get_text(" ", strip=True)) if el else None

        ungraded = get_price("td.used_price span.js-price")
        grade9 = get_price("td.cib_price span.js-price")
        psa10 = get_price("td.new_price span.js-price")

        card_name, card_number = card_text, ""
        if " #" in card_text:
            card_name, card_number = card_text.rsplit(" #", 1)
            card_name, card_number = card_name.strip(), card_number.strip()

        out.append((
            card_url, set_url, product_id, card_name, card_number,
            image_url, ungraded, grade9, psa10
        ))
    return out

def upsert_cards(con, rows):
    cur = con.cursor()
    cur.executemany("""
    INSERT INTO cards (
        card_url, set_url, product_id, card_name, card_number,
        image_url, ungraded_price, grade9_price, psa10_price
    ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
    ON CONFLICT(card_url) DO UPDATE SET
        set_url=excluded.set_url,
        product_id=excluded.product_id,
        card_name=excluded.card_name,
        card_number=excluded.card_number,
        image_url=excluded.image_url,
        ungraded_price=excluded.ungraded_price,
        grade9_price=excluded.grade9_price,
        psa10_price=excluded.psa10_price
    """, rows)
    con.commit()

def build_db_from_sets(sets_csv="pricecharting_sets.csv", db_path="pricecharting.db"):
    con = init_db(db_path)

    with open(sets_csv, newline="", encoding="utf-8") as f:
        reader = csv.DictReader(f)
        for i, row in enumerate(reader, start=1):
            set_url = row["set_url"]
            print(f"[{i}] {set_url}")

            try:
                cards = scrape_set_cards(set_url)
                upsert_cards(con, cards)
                print(f"  +{len(cards)} cards")
            except Exception as e:
                print(f"  ERROR: {e}")

            time.sleep(1)  # be polite

    con.close()
    print("Done.")

if __name__ == "__main__":
    build_db_from_sets()

