import requests
from bs4 import BeautifulSoup
import urllib.parse
import re

PRICE_RE = re.compile(r"\$?\s*([0-9]+(?:\.[0-9]{1,2})?)")

def format_search_query(card_json: dict) -> str:
    name = card_json.get("card_name", "").strip()
    number = card_json.get("collector_number", "").strip()

    if "/" in number:
        number = number.split("/")[0]

    if not name or not number:
        raise ValueError("Missing card_name or collector_number")

    return f"{name} {number}"


def fetch_pricecharting_ungraded_price(product_url: str) -> float:
    headers = {"User-Agent": "Mozilla/5.0"}
    r = requests.get(product_url, headers=headers, timeout=20)
    r.raise_for_status()

    soup = BeautifulSoup(r.text, "html.parser")
    price_el = soup.select_one("table#price_data td#used_price span.price")

    if not price_el:
        raise RuntimeError("Ungraded price not found")

    m = PRICE_RE.search(price_el.text)
    if not m:
        raise RuntimeError("Could not parse price")

    return float(m.group(1))

