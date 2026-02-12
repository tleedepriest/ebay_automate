# scrape_pokellector_sets.py
import csv
import json
import time
import requests
from bs4 import BeautifulSoup
from urllib.parse import urljoin

BASE = "https://www.pokellector.com"
INDEX_URL = f"{BASE}/sets"

HEADERS = {
    "User-Agent": "Mozilla/5.0 (compatible; card-indexer/0.1; +https://example.com)"
}


def parse_sets_from_html(base_url=BASE) -> list[dict]:
    r = requests.get(INDEX_URL, headers=HEADERS, timeout=30)
    r.raise_for_status()
    soup = BeautifulSoup(r.text, "html.parser")

    sets = []
    # EXACT selector per your snippet
    for a in soup.select("div.content.buttonlisting.english a.button[href]"):
        set_code = (a.get("name") or "").strip()
        href = (a.get("href") or "").strip()
        title = (a.get("title") or "").strip()

        # Name is in the <span>
        span = a.find("span")
        set_name = span.get_text(strip=True) if span else a.get_text(" ", strip=True)

        # Images: first img = logo, img.symbol = symbol
        logo_url = ""
        symbol_url = ""
        for img in a.find_all("img"):
            src = (img.get("src") or "").strip()
            if not src:
                continue
            classes = img.get("class") or []
            if "symbol" in classes:
                symbol_url = src
            elif not logo_url:
                logo_url = src

        sets.append({
            "set_code": set_code,
            "set_name": set_name,
            "set_url": urljoin(BASE, href),
            "set_path": href,
            "title": title,
            "logo_url": logo_url,
            "symbol_url": symbol_url,
        })

    # de-dupe by set_url
    dedup = {}
    for s in sets:
        dedup[s["set_url"]] = s
    return list(dedup.values())

def write_csv(path: str, rows: list[dict]):
    if not rows:
        return
    cols = ["set_code", "set_name", "set_url", "set_path", "title", "logo_url", "symbol_url"]
    with open(path, "w", encoding="utf-8", newline="") as f:
        w = csv.DictWriter(f, fieldnames=cols)
        w.writeheader()
        w.writerows(rows)


def write_jsonl(path: str, rows: list[dict]):
    with open(path, "w", encoding="utf-8") as f:
        for r in rows:
            f.write(json.dumps(r, ensure_ascii=False) + "\n")


if __name__ == "__main__":
    sets = parse_sets_from_html()
    print("Found:", len(sets))
    for s in sets:
        print(s["set_code"], s["set_name"], s["set_url"])

    write_csv("pokellector_sets.csv", sets)
    write_jsonl("pokellector_sets.jsonl", sets)
    print("Wrote pokellector_sets.csv and pokellector_sets.jsonl")
