#!/usr/bin/env python3
"""generate_manual_review.py

Generate a CSV for manual mapping review from tmp/card_matches.jsonl.
Columns: idx,image,input_name,collector_number,set_size,year, then top N candidate fields.

Usage:
  python generate_manual_review.py --target-set pokemon-paldean-fates --top 5
"""
import argparse
import json
import csv

DEFAULT_IN = "tmp/card_matches.jsonl"
DEFAULT_OUT = "tmp/manual_mapping_review.csv"


def parse_args():
    p = argparse.ArgumentParser()
    p.add_argument("--in", dest="infile", default=DEFAULT_IN)
    p.add_argument("--out", dest="outfile", default=DEFAULT_OUT)
    p.add_argument("--top", type=int, default=5)
    p.add_argument("--target-set", help="If provided, only include review rows relevant to this set_slug")
    return p.parse_args()


def row_for_obj(obj, top):
    inp = obj.get('input', {})
    base = [
        obj.get('idx',''),
        inp.get('image',''),
        inp.get('card_name',''),
        inp.get('collector_number',''),
        inp.get('set_size',''),
        inp.get('copyright_year',''),
    ]
    matches = obj.get('matches') or []
    # take up to top
    for m in matches[:top]:
        base.extend([m.get('card_name',''), m.get('card_number',''), m.get('set_slug',''), m.get('score',''), m.get('card_url','')])
    # pad
    for _ in range(top - len(matches)):
        base.extend(['','','','',''])
    return base


def main():
    args = parse_args()
    top = args.top
    target = args.target_set

    # If target provided, try to read set_meta to get base_total and released_year
    base_total = None
    released_year = None
    if target:
        try:
            import sqlite3
            con = sqlite3.connect('pricecharting.db')
            cur = con.cursor()
            cur.execute('SELECT base_total, released_year FROM set_meta WHERE set_slug = ?', (target,))
            r = cur.fetchone()
            if r:
                base_total, released_year = r[0], r[1]
            con.close()
        except Exception:
            base_total, released_year = None, None

    headers = ['idx','image','input_name','collector_number','set_size','year']
    for i in range(1, top+1):
        headers.extend([f'cand_{i}_name', f'cand_{i}_number', f'cand_{i}_set', f'cand_{i}_score', f'cand_{i}_url'])

    out_rows = []
    with open(args.infile, 'r', encoding='utf-8') as fin:
        for idx, line in enumerate(fin):
            line = line.strip()
            if not line:
                continue
            obj = json.loads(line)
            if not obj.get('needs_review'):
                continue

            # if target provided, filter rows relevant to that set
            if target:
                # include when any candidate already references the target
                cand_sets = set(m.get('set_slug') for m in (obj.get('matches') or []) if m.get('set_slug'))
                best = obj.get('best')
                if best and best.get('set_slug'):
                    cand_sets.add(best.get('set_slug'))

                # include when input set_size and year match the set_meta base_total/released_year
                inp = obj.get('input', {})
                inp_size = inp.get('set_size')
                inp_year = inp.get('copyright_year')
                matches_meta = False
                if base_total is not None and inp_size is not None and int(inp_size) == int(base_total):
                    if released_year is None or inp_year in (released_year, (released_year - 1) if released_year else None):
                        matches_meta = True

                if not cand_sets and not matches_meta:
                    # also allow when the input set_name_hint contains the target slug
                    hint = (obj.get('input',{}).get('set_name_hint') or '').lower()
                    if target not in hint:
                        continue

            # attach idx from file for reference
            obj['idx'] = idx
            out_rows.append(row_for_obj(obj, top))

    with open(args.outfile, 'w', encoding='utf-8', newline='') as fout:
        w = csv.writer(fout)
        w.writerow(headers)
        w.writerows(out_rows)

    print(f'Wrote {len(out_rows)} rows to {args.outfile}')

if __name__ == '__main__':
    main()
