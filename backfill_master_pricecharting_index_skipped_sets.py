import csv
import os
import time
import sqlite3
from bs4 import BeautifulSoup

from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.chrome.options import Options
from selenium.common.exceptions import WebDriverException

# -----------------------
# SQLite
# -----------------------

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


# -----------------------
# Selenium (Option A)
# -----------------------

def make_chrome_driver(chromedriver_path: str, headless: bool = False, chrome_binary: str | None = None):
    opts = Options()
    if headless:
        opts.add_argument("--headless=new")
    opts.add_argument("--window-size=1200,900")
    opts.add_argument("--disable-gpu")
    opts.add_argument("--no-sandbox")
    opts.add_argument("--disable-dev-shm-usage")
    opts.add_argument("--disable-background-networking")
    opts.add_argument("--disable-background-timer-throttling")
    opts.add_argument("--disable-renderer-backgrounding")
    opts.add_argument("--disable-features=Translate,BackForwardCache,AcceptCHFrame")
    opts.add_argument("--blink-settings=imagesEnabled=false")  # huge win

    # If you’re using a non-system Chrome (like chrome-for-testing zip), set this:
    if chrome_binary:
        opts.binary_location = chrome_binary

    return webdriver.Chrome(service=Service(chromedriver_path), options=opts)

def fetch_full_set_html_by_scrolling(driver, set_url: str, max_scrolls: int = 250, settle_rounds: int = 6):
    """
    Loads set_url and scrolls until row count stops increasing.
    """
    print("get_url")
    driver.get(set_url)
    time.sleep(2.0)

    last_count = -1
    stable = 0

    for _ in range(max_scrolls):
        rows = driver.find_elements(By.CSS_SELECTOR, "table#games_table tbody tr[data-product]")
        count = len(rows)
        print(count)

        if count == last_count:
            stable += 1
        else:
            stable = 0
            last_count = count

        if stable >= settle_rounds:
            break

        driver.execute_script("window.scrollTo(0, document.body.scrollHeight);")
        print("scrolling ...")
        time.sleep(5.0)
        print("waiting ...")

    return driver.page_source


def scrape_set_with_retry(
    set_url: str,
    chromedriver_path: str,
    headless: bool,
    chrome_binary: str | None,
    max_attempts: int = 3
):
    last_err = None
    for attempt in range(1, max_attempts + 1):
        print("creating the chromedriver")
        driver = make_chrome_driver(chromedriver_path, headless=headless, chrome_binary=chrome_binary)
        try:
            # timeouts (important)
            print("loading page...")
            driver.set_page_load_timeout(120)
            driver.set_script_timeout(120)
            html = fetch_full_set_html_by_scrolling(driver, set_url, max_scrolls=20, settle_rounds=1)
            print(html)
            return html
        except Exception as e:
            last_err = e
            try:
                driver.quit()
            except Exception:
                pass

            print(f"  retry {attempt}/{max_attempts} failed: {e}")
            time.sleep(2.0 * attempt)  # small backoff
        finally:
            try:
                driver.quit()
            except Exception:
                pass

    raise last_err

# -----------------------
# Parse HTML -> rows
# -----------------------

def _parse_price(text: str):
    if not text:
        return None
    txt = text.replace("$", "").replace(",", "").strip()
    try:
        return float(txt)
    except:
        return None

def parse_cards_from_html(set_url: str, html: str):
    soup = BeautifulSoup(html, "html.parser")
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
        href = (a.get("href") or "").strip()
        card_url = href if href.startswith("http") else f"https://www.pricecharting.com{href}"

        img = tr.select_one("td.image img")
        image_url = (img.get("src") or "").strip() if img else ""

        ungraded = _parse_price(tr.select_one("td.used_price span.js-price").get_text(strip=True)
                               if tr.select_one("td.used_price span.js-price") else "")
        grade9 = _parse_price(tr.select_one("td.cib_price span.js-price").get_text(strip=True)
                             if tr.select_one("td.cib_price span.js-price") else "")
        psa10 = _parse_price(tr.select_one("td.new_price span.js-price").get_text(strip=True)
                            if tr.select_one("td.new_price span.js-price") else "")

        card_name, card_number = card_text, ""
        if " #" in card_text:
            card_name, card_number = card_text.rsplit(" #", 1)
            card_name, card_number = card_name.strip(), card_number.strip()

        out.append((
            card_url, set_url, product_id, card_name, card_number,
            image_url, ungraded, grade9, psa10
        ))

    return out


# -----------------------
# Main build loop
# -----------------------

def _slug_from_set_url(set_url: str) -> str:
    """
    PriceCharting set URLs typically look like:
      https://www.pricecharting.com/console/pokemon-151
    We treat the last path segment as the set_slug.
    """
    path = urlparse(set_url).path.strip("/")
    return path.split("/")[-1] if path else ""

def _get_existing_set_slugs_from_cards(con) -> set[str]:
    """
    Pull distinct set_slug values that are already present in the cards table.
    (Assumes your cards table has a set_slug column.)
    """
    cur = con.cursor()
    cur.execute("SELECT DISTINCT set_slug FROM cards WHERE set_slug IS NOT NULL AND set_slug != ''")
    return {r[0] for r in cur.fetchall()}

def build_db_from_sets_csv(
    sets_csv: str = "pricecharting_sets.csv",
    db_path: str = "pricecharting.db",
    chromedriver_path: str = "/home/leeone/bin/chromedriver",
    headless: bool = True,
    chrome_binary: str | None = None,
    limit_sets: int | None = None,
    start_at: int | None = None,          # optional index-based start within the *skipped list*
    skip_japanese: bool = True,
):
    con = init_db(db_path)

    try:
        # 1) What do we already have in the DB?
        existing_slugs = _get_existing_set_slugs_from_cards(con)
        print(f"DB has cards for {len(existing_slugs)} set_slugs")

        # 2) Load CSV sets and compute which ones were skipped
        all_sets = []
        with open(sets_csv, newline="", encoding="utf-8") as f:
            reader = csv.DictReader(f)
            for row in reader:
                set_url = row.get("set_url", "").strip()
                set_name = row.get("set_name", "").strip()
                if not set_url:
                    continue

                set_slug = row.get("set_slug", "").strip() or _slug_from_set_url(set_url)
                all_sets.append({
                    "set_slug": set_slug,
                    "set_url": set_url,
                    "set_name": set_name,
                })

        # Only those whose slug is NOT in DB
        skipped = [s for s in all_sets if s["set_slug"] and s["set_slug"] not in existing_slugs]

        # Optional: skip Japanese by name
        if skip_japanese:
            skipped_non_jp = []
            for s in skipped:
                if "japanese" in (s["set_name"] or "").lower():
                    continue
                skipped_non_jp.append(s)
            skipped = skipped_non_jp

        print(f"CSV has {len(all_sets)} sets")
        print(f"Skipped sets to scrape: {len(skipped)}")

        # 3) Optionally start at N *within the skipped list*
        if start_at is not None:
            skipped = skipped[max(0, start_at - 1):]

        # 4) Optionally cap how many skipped sets to scrape
        if limit_sets is not None:
            skipped = skipped[:limit_sets]

        # 5) Scrape only skipped sets
        for idx, s in enumerate(skipped, start=1):
            set_url = s["set_url"]
            set_name = s["set_name"]
            set_slug = s["set_slug"]

            print(f"[{idx}/{len(skipped)}] {set_name} ({set_slug}) | {set_url}")

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
                print(f"  +{len(cards)} cards")
            except Exception as e:
                print(f"  ERROR: {e}")

            time.sleep(0.8)

    finally:
        con.close()

if __name__ == "__main__":
    # EDIT THESE PATHS:
    CHROMEDRIVER_PATH =  "chromedriver-linux64/chromedriver"  # <-- change to your actual path

    # If you’re using system Chrome at /opt/google/chrome/chrome, you can leave this as None.
    # If using a chrome-for-testing zip, point to its "chrome" binary.
    CHROME_BINARY = None  # e.g. "/home/leeone/chrome145/chrome-linux64/chrome"

    build_db_from_sets_csv(
        sets_csv="pricecharting_sets.csv",
        db_path="pricecharting.db",
        chromedriver_path=CHROMEDRIVER_PATH,
        headless=True,
        chrome_binary=CHROME_BINARY,
        limit_sets=None,  # set to 5 for a quick test
    )

