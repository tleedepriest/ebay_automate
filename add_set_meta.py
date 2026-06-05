#!/usr/bin/env python3
"""add_set_meta.py

Add or update set metadata in pricecharting.db and optionally add a manual mapping
that will be picked up by set_map (via manual_set_map.py).

Examples:
  python add_set_meta.py --set-slug pokemon-custom-set --pc-name "Custom Set" --released-year 2026 --base-total 100 --add-map
  python add_set_meta.py  # interactive prompts

This intentionally avoids pokellector data by default so you can maintain local metadata
without depending on external sources.
"""
import argparse
import sqlite3
import sys
import json
import os

DB_PATH = "pricecharting.db"

CREATE_TABLE_SQL = """
CREATE TABLE IF NOT EXISTS set_meta (
    set_slug TEXT PRIMARY KEY,
    pc_list_a_name TEXT,
    pokellector_set_name TEXT,
    pokellector_url TEXT,
    base_total INTEGER,
    secret_total INTEGER,
    released_md TEXT,
    released_year INTEGER,
    released_raw TEXT,
    language TEXT
);
"""


def ensure_table(con):
    cur = con.cursor()
    cur.execute(CREATE_TABLE_SQL)
    con.commit()


def safe_input(prompt_text):
    try:
        return input(prompt_text).strip() or None
    except (EOFError, KeyboardInterrupt):
        print()
        sys.exit(1)


def parse_args():
    p = argparse.ArgumentParser(description="Insert/update or show a set_meta record.")
    p.add_argument("--set-slug", help="Primary key set slug (e.g. pokemon-custom-set).")
    p.add_argument("--pc-name", help="PriceCharting list name (pc_list_a_name).")
    p.add_argument("--base-total", type=int, help="Number of base cards.")
    p.add_argument("--secret-total", type=int, help="Number of secret cards.")
    p.add_argument("--released-year", type=int, help="Released year.")
    p.add_argument("--released-md", help="Released month/day (optional).")
    p.add_argument("--released-raw", help="Raw release string (optional).")
    p.add_argument("--language", help="Language code (e.g., en).")
    p.add_argument("--show", action="store_true", help="Show the set_meta row for --set-slug instead of editing.")
    p.add_argument("--force", action="store_true", help="Overwrite existing DB row entirely (uses INSERT OR REPLACE).")
    return p.parse_args()


def upsert_db(con, row, force=False):
    cur = con.cursor()
    ensure_table(con)

    cur.execute("SELECT * FROM set_meta WHERE set_slug = ?", (row['set_slug'],))
    exists = cur.fetchone()
    if exists and not force:
        # update provided non-None fields
        updates = []
        params = []
        for k in ['pc_list_a_name','pokellector_set_name','pokellector_url','base_total','secret_total','released_md','released_year','released_raw','language']:
            if row.get(k) is not None:
                updates.append(f"{k} = ?")
                params.append(row.get(k))
        if not updates:
            print('No new values provided for DB update; use --force to overwrite or provide fields.')
            return
        params.append(row['set_slug'])
        sql = f"UPDATE set_meta SET {', '.join(updates)} WHERE set_slug = ?"
        cur.execute(sql, params)
        con.commit()
        print(f"Updated set_meta for {row['set_slug']}")
        return

    # Insert or replace
    fields = ['set_slug','pc_list_a_name','pokellector_set_name','pokellector_url','base_total','secret_total','released_md','released_year','released_raw','language']
    vals = [row.get(f) for f in fields]
    cur.execute("INSERT OR REPLACE INTO set_meta (" + ",".join(fields) + ") VALUES (" + ",".join(['?']*len(fields)) + ")", vals)
    con.commit()
    print(f"Inserted set_meta for {row['set_slug']}")




def main():
    args = parse_args()

    # If user asked to show, print row and exit
    if args.show:
        if not args.set_slug:
            args.set_slug = safe_input('Set slug to show (required): ')
        if not args.set_slug:
            print('set_slug is required for --show.')
            sys.exit(1)
        con = sqlite3.connect(DB_PATH)
        con.row_factory = sqlite3.Row
        try:
            ensure_table(con)
            cur = con.cursor()
            cur.execute("SELECT * FROM set_meta WHERE set_slug = ?", (args.set_slug,))
            row = cur.fetchone()
            if not row:
                print(f"No set_meta row found for '{args.set_slug}'")
            else:
                print(json.dumps(dict(row), indent=2, ensure_ascii=False))
        finally:
            con.close()
        return

    # Interactive prompts if required for insert/update
    if not args.set_slug:
        args.set_slug = safe_input('Set slug (required): ')
    if not args.set_slug:
        print('set_slug is required.')
        sys.exit(1)

    if not args.pc_name:
        args.pc_name = safe_input('PriceCharting name (pc_list_a_name) [optional]: ')

    if args.base_total is None:
        v = safe_input('base_total (integer) [optional]: ')
        args.base_total = int(v) if v and v.isdigit() else None
    if args.secret_total is None:
        v = safe_input('secret_total (integer) [optional]: ')
        args.secret_total = int(v) if v and v.isdigit() else None
    if args.released_year is None:
        v = safe_input('released_year (integer) [optional]: ')
        args.released_year = int(v) if v and v.isdigit() else None
    if args.released_md is None:
        args.released_md = safe_input('released_md (e.g. 04/15 or Apr 2021) [optional]: ')
    if args.released_raw is None:
        args.released_raw = safe_input('released_raw [optional]: ')
    if args.language is None:
        args.language = safe_input('language (e.g., en) [optional]: ')

    row = {
        'set_slug': args.set_slug,
        'pc_list_a_name': args.pc_name,
        'pokellector_set_name': None,
        'pokellector_url': None,
        'base_total': args.base_total,
        'secret_total': args.secret_total,
        'released_md': args.released_md,
        'released_year': args.released_year,
        'released_raw': args.released_raw,
        'language': args.language,
    }

    con = sqlite3.connect(DB_PATH)
    try:
        upsert_db(con, row, force=args.force)
    finally:
        con.close()

    print('Done.')

if __name__ == '__main__':
    main()
