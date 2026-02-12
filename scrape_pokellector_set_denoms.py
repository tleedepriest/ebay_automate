import csv
import json
import re
import time
from pathlib import Path

import requests
from bs4 import BeautifulSoup

HEADERS = {
    "User-Agent": "Mozilla/5.0 (compatible; pokellector-denom-scraper/0.1)"
}

def parse_cards_and_release(html: str):
    """
    Parses:
      <div class="cards">
        <span>Cards</span>
        <span>132</span>
        <cite>+56 Secret</cite>
      </div>

    And the NEXT div:
      <div>
        <span>Released</span>
        <span>Sep 1st</span>
        <cite>2025</cite>
      </div>

    Returns dict:
      base_total, secret_total, released_md, released_year, released_raw
    """
    soup = BeautifulSoup(html, "html.parser")
    cards_div = soup.select_one("div.cards")
    if not cards_div:
        return {
            "base_total": None,
            "secret_total": None,
            "released_md": None,
            "released_year": None,
            "released_raw": None,
        }

    # --- Cards ---
    spans = cards_div.find_all("span")
    base_total = None
    if len(spans) >= 2:
        txt = spans[1].get_text(strip=True)
        m = re.search(r"(\d+)", txt)
        base_total = int(m.group(1)) if m else None

    secret_total = None
    cite = cards_div.find("cite")
    if cite:
        m = re.search(r"\+?\s*(\d+)\s*Secret", cite.get_text(" ", strip=True), flags=re.IGNORECASE)
        if m:
            secret_total = int(m.group(1))
        else:
            secret_total = 0  # if cite exists but not secret format, treat as 0

    # --- Released (next div) ---
    released_md = None
    released_year = None
    released_raw = None

    next_div = cards_div.find_next_sibling("div")
    if next_div:
        r_spans = next_div.find_all("span")
        if len(r_spans) >= 2 and r_spans[0].get_text(strip=True).lower() == "released":
            released_md = r_spans[1].get_text(strip=True)  # e.g. "Sep 1st"
            r_cite = next_div.find("cite")
            if r_cite:
                m = re.search(r"(\d{4})", r_cite.get_text(strip=True))
                released_year = int(m.group(1)) if m else None

            if released_md and released_year:
                released_raw = f"{released_md} {released_year}"
            elif released_md:
                released_raw = released_md
            elif released_year:
                released_raw = str(released_year)

    return {
        "base_total": base_total,
        "secret_total": secret_total,
        "released_md": released_md,
        "released_year": released_year,
        "released_raw": released_raw,
    }
    
def fetch_html(url: str, timeout: int = 30) -> str:
    r = requests.get(url, headers=HEADERS, timeout=timeout)
    r.raise_for_status()
    return r.text

def scrape_one_set(url: str):
    html = fetch_html(url)
    soup = BeautifulSoup(html, "html.parser")

    h1 = soup.find("h1")
    set_title = h1.get_text(strip=True) if h1 else ""

    meta = parse_cards_and_release(html)

    return {
        "set_url": url,
        "set_title": set_title,
        **meta,
    }

def load_sets_from_jsonl(path: str):
    sets = []
    with open(path, "r", encoding="utf-8") as f:
        for line in f:
            line = line.strip()
            if not line:
                continue
            rec = json.loads(line)
            # expects your earlier output: {set_url, set_code, set_name, ...}
            if "set_url" in rec:
                sets.append(rec)
    return sets

def load_sets_from_csv(path: str):
    sets = []
    with open(path, "r", encoding="utf-8", newline="") as f:
        r = csv.DictReader(f)
        for row in r:
            if row.get("set_url"):
                sets.append(row)
    return sets

def write_jsonl(path: str, rows: list[dict]):
    with open(path, "w", encoding="utf-8") as f:
        for r in rows:
            f.write(json.dumps(r, ensure_ascii=False) + "\n")

def write_csv(path: str, rows: list[dict]):
    if not rows:
        return
    cols = ["set_code", "set_name", "set_url", "set_title", "base_total", "secret_total", "release_md", "released_year", "released_raw"]

    with open(path, "w", encoding="utf-8", newline="") as f:
        w = csv.DictWriter(f, fieldnames=cols)
        w.writeheader()
        for r in rows:
            w.writerow({k: r.get(k, "") for k in cols})

def main(
    input_path: str = "pokellector_sets.jsonl",
    out_jsonl: str = "pokellector_set_denoms.jsonl",
    out_csv: str = "pokellector_set_denoms.csv",
    sleep_s: float = 0.8,
    start_at: int = 0,
):
    input_path = str(input_path)
    if input_path.endswith(".jsonl"):
        sets = load_sets_from_jsonl(input_path)
    elif input_path.endswith(".csv"):
        sets = load_sets_from_csv(input_path)
    else:
        raise ValueError("input_path must be .jsonl or .csv")

    print(f"Loaded {len(sets)} sets from {input_path}")

    results = []
    for i, s in enumerate(sets[start_at:], start=start_at):
        url = s["set_url"]
        try:
            meta = scrape_one_set(url)
            row = {
                "set_code": s.get("set_code", ""),
                "set_name": s.get("set_name", ""),
                "set_url": url,
                "set_title": meta.get("set_title", ""),
                "base_total": meta.get("base_total"),
                "secret_total": meta.get("secret_total"),
                "released_md": meta.get("released_md"),
                "released_year": meta.get("released_year"),
                "released_raw": meta.get("released_raw")
            }
            results.append(row)
            print(f"[{i}] OK  {row['set_name'] or row['set_title']}  base={row['base_total']}  secret={row['secret_total']}, release_raw={row['released_raw']}")
        except Exception as e:
            row = {
                "set_code": s.get("set_code", ""),
                "set_name": s.get("set_name", ""),
                "set_url": url,
                "set_title": "",
                "base_total": None,
                "secret_total": None,
                "released_md": None,
                "released_year": None,
                "released_raw": None,
                "error": str(e),
            }
            results.append(row)
            print(f"[{i}] FAIL {s.get('set_name','')}  {url}  err={e}")

        time.sleep(sleep_s)

        # write progress every 25 sets so you can resume safely
        if (i + 1) % 25 == 0:
            write_jsonl(out_jsonl, results)
            write_csv(out_csv, results)
            print(f"Checkpoint wrote {len(results)} rows")

    write_jsonl(out_jsonl, results)
    write_csv(out_csv, results)
    print("Done.")
    print("Wrote:", out_jsonl)
    print("Wrote:", out_csv)

if __name__ == "__main__":
    main()

