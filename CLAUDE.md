# CLAUDE.md

Guidance for Claude Code working in this repo. See `README.md` for the full project overview.

## What this is
A 4-stage pipeline that turns scanned Pokémon card images into an eBay File Exchange
upload CSV: **upload → identify → lookup → build**, orchestrated by the `makefile`.
Stages hand off via JSONL/CSV files in `tmp/`.

```
upload_and_manifest.py    → tmp/upload_manifest.jsonl       (GCS upload, front/back pairing)
identify_from_manifest.py → tmp/card_identifications.jsonl  (OpenAI vision → JSON schema)
lookup_cards.py           → tmp/card_matches.jsonl          (match vs pricecharting.db)
  (+ lookup_pc_fuzzy.py)    tmp/match_review.csv
build_ebay_batch_csv.py   → tmp/Batch.csv                   (eBay File Exchange CSV)
```

Run: `make all IMAGES_DIR=<folder>` (or individual stages `upload`/`identify`/`lookup`/`build`).

## Key facts to remember
- **Index convention:** `match_review.csv` idx is **0-based**; manifest/idents
  `listing_index` is **1-based**. Joining requires `manifest_idx = idx + 1`. Don't
  reintroduce this off-by-one bug.
- **Data store:** `pricecharting.db` (SQLite) with tables `cards` (~46.5k rows) and
  `set_meta` (~300 rows). It is git-ignored — never assume it's reproducible from the
  repo alone; the scrapers rebuild it.
- **External dependencies:** OpenAI API (`OPENAI_API_KEY`), GCS bucket
  `ebay-automate-picture-hosting` (public-read), and Selenium/ChromeDriver for scrapers.
- **Pricing/filter rules** live in `build_ebay_batch_csv.py`: auto-list only when prices
  `< 20`; floor `2.49`; `<5` → `+1.50`, else `×1.25`; cents snap to `.49`/`.95`.

## Conventions
- Plain Python 3 scripts, no framework. Each stage is a standalone script with a
  module-level docstring explaining its I/O — keep that docstring accurate when editing.
- Config flows through the `makefile` as CLI args; prefer adding new knobs there rather
  than hardcoding.
- `tmp/`, `*.csv`, `*.jsonl`, `*.db`, `*.png`, `*.zip` are git-ignored.

## Gotchas
- No `requirements.txt` — deps (`openai`, `google-cloud-storage`, `selenium`,
  `rapidfuzz`) are only implied by imports.
- The `build` makefile target passes `--manifest` twice (redundant, harmless).
- Legacy single-card prototype (`main.py`, `identify_card.py`, `scan_cards.py`,
  `google_pc_chrome.py`, `pricecharting.py`) predates the batch pipeline — not part of
  the current flow.
