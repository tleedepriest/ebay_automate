import csv
import requests
from bs4 import BeautifulSoup
from urllib.parse import urljoin

HEADERS = {
    "User-Agent": "Mozilla/5.0",
    "Accept-Language": "en-US,en;q=0.9",
}

def scrape_set_links(category_url: str):
    r = requests.get(category_url, headers=HEADERS, timeout=30)
    r.raise_for_status()

    soup = BeautifulSoup(r.text, "html.parser")

    box = soup.select_one("div.home-box.all")
    if not box:
        raise RuntimeError("Could not find div.home-box.all on page")

    rows = []
    seen = set()

    for a in box.select('ul li a[href^="/console/pokemon-"]'):
        href = a.get("href", "").strip()
        name = a.get_text(" ", strip=True)

        full_url = urljoin(category_url, href)

        if full_url in seen:
            continue
        seen.add(full_url)

        rows.append({
            "set_name": name,
            "set_url": full_url,
            "set_slug": href.split("/console/", 1)[1] if "/console/" in href else ""
        })

    return rows

def write_csv(rows, out_path: str):
    with open(out_path, "w", newline="", encoding="utf-8") as f:
        w = csv.DictWriter(f, fieldnames=["set_name", "set_url", "set_slug"])
        w.writeheader()
        w.writerows(rows)

if __name__ == "__main__":
    url = "https://www.pricecharting.com/category/pokemon-cards"
    rows = scrape_set_links(url)
    print(f"Found {len(rows)} sets")
    write_csv(rows, "pricecharting_sets.csv")
    print("Wrote pricecharting_sets.csv")

