# ebay_automate

Automated pipeline for listing **Pokémon trading cards on eBay** — from scanned
card images to a ready-to-upload eBay File Exchange batch CSV.

Given a folder of card scans, the pipeline:

1. Uploads the images to a public Google Cloud Storage bucket (for eBay image hosting).
2. Identifies each card with the OpenAI vision API (name, number, set size, year, language).
3. Matches each identified card against a local PriceCharting database to pull market prices.
4. Builds an eBay bulk-upload CSV with computed listing prices, condition, and shipping/return profiles.

---

## Pipeline overview

The whole flow is orchestrated by the `makefile`. The four stages communicate
through intermediate files in `tmp/`:

```
 images/                                     tmp/
 <set>/<finish>/<date>/*.png
        │
        │  make upload
        ▼
   upload_and_manifest.py  ───────────────▶  upload_manifest.jsonl
        │  (GCS upload, front/back pairing)
        │
        │  make identify
        ▼
   identify_from_manifest.py  ────────────▶  card_identifications.jsonl
        │  (OpenAI vision → structured JSON)
        │
        │  make lookup
        ▼
   lookup_cards.py + lookup_pc_fuzzy.py  ─▶  card_matches.jsonl
        │  (match vs pricecharting.db)         match_review.csv
        │
        │  make build
        ▼
   build_ebay_batch_csv.py  ──────────────▶  Batch.csv  (eBay File Exchange)
```

Run the whole thing at once:

```bash
make all IMAGES_DIR=cards/breakpoint/reverseholos/2026-02-13
```

…or one stage at a time (`make upload`, `make identify`, `make lookup`, `make build`).
`make clean` removes the intermediate files in `tmp/`.

---

## Stages in detail

### 1. `upload` — `upload_and_manifest.py`
- Lists images in a folder (sorted by filename) and pairs them `(0,1) (2,3) …` =
  `(front, back)`. `listing_index` starts at **1**.
- Uploads to the GCS bucket **`ebay-automate-picture-hosting`** (must be
  public-readable so eBay can fetch `PicURL`s).
- Object paths derive from the local folder structure (`set/finish/date/`) plus a
  deterministic content hash, so re-running is **idempotent** — existing objects are
  skipped, never overwritten.
- Output: `tmp/upload_manifest.jsonl`.

### 2. `identify` — `identify_from_manifest.py`
- Sends each image to the **OpenAI** vision API with a strict JSON schema.
- Extracts: `card_name`, `language`, `collector_number` (e.g. `"103/165"`),
  `set_size`, `copyright_year` (nullable), `year_in_range`, and `confidence`.
- Optional args: `MIN_YEAR` and `EXTRA_PROMPT_INFORMATION` (passed through the makefile).
- Output: `tmp/card_identifications.jsonl`.

### 3. `lookup` — `lookup_cards.py` (+ `lookup_pc_fuzzy.py`)
- Matches identifications against **`pricecharting.db`** using deterministic filters:
  - `set_meta.base_total == set_size`
  - `set_meta.released_year ∈ {copyright_year, copyright_year − 1}`
  - `cards.card_number` matches the numerator of `collector_number`
- Fuzzy-matches `card_name` (via `rapidfuzz`) only to break ties.
- Outputs: `tmp/card_matches.jsonl` (full match objects) and `tmp/match_review.csv`
  (human-readable review sheet).

### 4. `build` — `build_ebay_batch_csv.py`
- Joins `match_review.csv` + `upload_manifest.jsonl` + `card_identifications.jsonl`
  into `tmp/Batch.csv` (eBay File Exchange format).
- **Off-by-one join fix:** `match_review` idx is 0-based, manifest/idents
  `listing_index` is 1-based → `manifest_idx = idx + 1`.
- Filters: only rows where `needs_review` is false, that have both front & back URLs,
  and where `best_ungraded_price < 20` and `final_price < 20` (auto-list cap).
- Pricing rules: floor at `2.49`; if `< 5.00` add `1.50`, else multiply by `1.25`;
  cents formatting → `.49` (cents 00–49) or `.95` (cents 50–99).
- eBay specifics: `QUOTE_ALL` CSV, `PicURL` joined with `" | "`, condition mapped
  from the makefile, shipping/return/payment profiles, `CustomLabel` shared per batch.

---

## The PriceCharting database (`pricecharting.db`)

Local SQLite DB (~23 MB) with two tables:

- **`cards`** (~46,500 rows): `card_url` (PK), `set_url`, `product_id`, `card_name`,
  `card_number`, `image_url`, `ungraded_price`, `grade9_price`, `psa10_price`,
  `set_slug`, `set_code`, `set_name`, `language`.
- **`set_meta`** (~300 rows): `set_slug` (PK), `pc_list_a_name`,
  `pokellector_set_name`, `pokellector_url`, `base_total`, `secret_total`,
  `released_md`, `released_year`, `released_raw`, `language`.

### Building / maintaining the DB
- `build_master_pricecharting_index_scroll.py`, `scrape_pricecharting_sets.py` —
  Selenium scrapers that populate `cards` and PriceCharting set lists.
- `scrape_pokellector_sets.py`, `scrape_pokellector_set_denoms.py` — scrape set
  totals/denominators from Pokellector.
- `set_map.py` — high-certainty manual mapping from PC set slugs → set names.
- `add_set_meta.py`, `create_set_meta.py`, `insert_pc_sets.py`,
  `backfill_*.py`, `update_journey_together.py` — add/backfill set metadata
  (slugs, codes, release years, totals).
- `export_db_to_csv.py` — dump tables to CSV.

These scrapers require **Selenium + ChromeDriver** (bundled in `chromedriver-linux64/`).

---

## Setup

```bash
./setup.sh                       # installs tesseract-ocr (used by legacy OCR path)
python3 -m venv .venv && . .venv/bin/activate
pip install openai google-cloud-storage selenium rapidfuzz
```

Required environment / credentials:
- **OpenAI** — `OPENAI_API_KEY` (used by `identify_from_manifest.py`).
- **Google Cloud Storage** — application-default credentials with write access to
  the `ebay-automate-picture-hosting` bucket.
- **ChromeDriver** — only needed when (re)building the database via the scrapers.

> There is no `requirements.txt` yet — dependencies above are inferred from imports.
> Consider pinning them with `pip freeze > requirements.txt`.

---

## Makefile configuration

Key variables (override on the `make` command line):

| Variable | Default | Purpose |
|----------|---------|---------|
| `IMAGES_DIR` | _(required for `upload`/`build`)_ | Folder of card scans |
| `MIN_YEAR` / `EXTRA_PROMPT_INFORMATION` | _(empty)_ | Extra hints for identify stage |
| `CATEGORY` | `183454` | eBay category ID |
| `CONDITION_ID` | `4000` | eBay condition ID |
| `CARD_CONDITION_SHORT` | `MP` | `NM` / `LP` / `MP` / `HP` → mapped to full eBay condition string |
| `CARD_CONDITION` | _(derived)_ | Override the mapped condition directly |
| `LOCATION` / `POSTAL_CODE` | `rockville, md` / `20850` | Item location |
| `SHIPPING_PROFILE` / `RETURN_PROFILE` / `PAYMENT_PROFILE` | `free_shipping_under_20` / `30_day_returns` / `buy_it_now` | eBay business policies |
| `BEST_OFFER_ENABLED` | `0` | Enable best offers |
| `CUSTOM_LABEL` | `batch-auto` | Shared custom label for the batch |

Card-condition short codes map as:

| Code | eBay condition |
|------|----------------|
| `NM` | Near mint or better (ID 400010) |
| `LP` | Lightly Played / Excellent (ID 400015) |
| `MP` | Moderately played / Very good (ID 400016) |
| `HP` | Heavily played / Poor (ID 400017) |

---

## Legacy / prototype scripts

An earlier single-card prototype predates the batch pipeline and is kept for reference:
`main.py`, `identify_card.py`, `scan_cards.py`, `google_pc_chrome.py`,
`pricecharting.py`, `lookup_cards_batch_one_price.py`. These used OCR (tesseract) and a
Google → PriceCharting scrape for one card at a time.

---

## Repo notes

- `tmp/`, `*.csv`, `*.jsonl`, `*.db`, `*.png`, `*.zip` are git-ignored — the database
  and data exports live untracked in the working tree.
- `chromedriver-linux64.zip.1` is a duplicate download and can be deleted.
