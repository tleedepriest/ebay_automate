#!/usr/bin/env python3
"""
upload_and_manifest.py

Same behavior as your original script:
  - List images in a folder, sort by filename
  - Pair as (0,1) (2,3) ... = (front, back)
  - listing_index starts at 1
  - Writes tmp/upload_manifest.jsonl with the SAME schema as before

Scaling + safety improvements:
  - Organize uploaded objects under prefix derived from local folder structure: set/finish/date
  - Add deterministic content hash to object names to prevent overwriting prior uploads
  - Skip upload if object already exists (idempotent). This lets you re-run safely.

Expected local folder structure (what YOU described):
  <root>/<set>/<finish>/<date>/   <-- script reads THIS leaf folder
and filenames are whatever scanner produces (we just sort).

Example:
  cards/breakpoint/reverseholos/2026-02-13/<scanner files...>

Object path in bucket becomes:
  breakpoint/reverseholos/2026-02-13/0001_front_a1b2c3d4e5f6.jpg
"""

import os
import sys
import json
import mimetypes
import hashlib
from pathlib import Path
from typing import List, Tuple, Optional
from google.cloud import storage

# ---- config ----
BUCKET = "ebay-automate-picture-hosting"   # bucket should be public-readable for eBay PicURL
OUT_MANIFEST = "tmp/upload_manifest.jsonl"

IMAGE_EXTS = {".jpg", ".jpeg", ".png", ".webp"}


def list_images_in_order(folder: str) -> List[Path]:
    files = [
        p for p in Path(folder).iterdir()
        if p.is_file() and p.suffix.lower() in IMAGE_EXTS
    ]
    files.sort(key=lambda p: p.name)  # deterministic order by filename sort
    return files


def pair_front_back(files: List[Path]) -> List[Tuple[Path, Path]]:
    if len(files) % 2 != 0:
        raise ValueError(f"Expected even number of images (front/back pairs). Got {len(files)}")
    pairs = []
    for i in range(0, len(files), 2):
        pairs.append((files[i], files[i + 1]))  # front, back
    return pairs


def sha256_12(path: Path) -> str:
    h = hashlib.sha256()
    with open(path, "rb") as f:
        for chunk in iter(lambda: f.read(1024 * 1024), b""):
            h.update(chunk)
    return h.hexdigest()[:12]


def guess_content_type(local_path: Path) -> str:
    ctype, _ = mimetypes.guess_type(str(local_path))
    return ctype or "image/jpeg"


def sanitize(seg: str) -> str:
    seg = (seg or "").strip().lower().replace(" ", "-")
    seg = "".join(ch for ch in seg if ch.isalnum() or ch in "-_./")
    seg = seg.strip("/").strip(".")
    return seg or "unknown"


def infer_prefix_from_path(images_dir: str) -> str:
    """
    If images_dir looks like .../<set>/<finish>/<date>, return 'set/finish/date'
    Otherwise fall back to using the last directory name.
    """
    p = Path(images_dir).resolve()
    parts = p.parts
    if len(parts) >= 3:
        set_slug = sanitize(parts[-3])
        finish = sanitize(parts[-2])
        dt = sanitize(parts[-1])
        return f"{set_slug}/{finish}/{dt}"
    return sanitize(p.name)


def upload_file_if_missing(bucket, local_path: Path, object_name: str) -> str:
    """
    Upload only if the object doesn't already exist.
    Returns the public URL either way.
    """
    blob = bucket.blob(object_name)

    # Idempotency: if exists, do NOT overwrite.
    # This prevents existing eBay listings from changing.
    if blob.exists():
        return f"https://storage.googleapis.com/{bucket.name}/{object_name}"

    blob.upload_from_filename(
        str(local_path),
        content_type=guess_content_type(local_path)
    )
    return f"https://storage.googleapis.com/{bucket.name}/{object_name}"


def main(images_dir: str, out_manifest: str = OUT_MANIFEST, prefix: Optional[str] = None):
    """
    images_dir: leaf directory containing scanner images for ONE batch (e.g. set/finish/date)
    out_manifest: output manifest path
    prefix: optional override for the bucket folder prefix; if None we infer from images_dir
    """
    os.makedirs("tmp", exist_ok=True)

    files = list_images_in_order(images_dir)
    pairs = pair_front_back(files)

    client = storage.Client()
    bucket = client.bucket(BUCKET)

    prefix = sanitize(prefix) if prefix else infer_prefix_from_path(images_dir)

    with open(out_manifest, "w", encoding="utf-8") as f:
        for idx, (front, back) in enumerate(pairs, start=1):
            # deterministic, unique object names
            front_hash = sha256_12(front)
            back_hash = sha256_12(back)

            front_ext = front.suffix.lower()
            back_ext = back.suffix.lower()

            front_obj = f"{prefix}/{idx:04d}_front_{front_hash}{front_ext}"
            back_obj  = f"{prefix}/{idx:04d}_back_{back_hash}{back_ext}"

            front_url = upload_file_if_missing(bucket, front, front_obj)
            back_url  = upload_file_if_missing(bucket, back, back_obj)

            rec = {
                "listing_index": idx,          # stays 1-based (same as before)
                "front_local": str(front),
                "back_local": str(back),
                "front_object": front_obj,
                "back_object": back_obj,
                "front_url": front_url,
                "back_url": back_url,
            }
            f.write(json.dumps(rec) + "\n")
            print(f"[{idx}] uploaded/exists -> front/back")

    print("Wrote", out_manifest)
    print("Prefix used:", prefix)


if __name__ == "__main__":
    # Usage:
    #   python upload_and_manifest.py <images_dir>
    # Optional:
    #   python upload_and_manifest.py <images_dir> --out tmp/upload_manifest.jsonl --prefix breakpoint/reverseholos/2026-02-13
    args = sys.argv[1:]
    if not args:
        raise SystemExit("Usage: python upload_and_manifest.py <images_dir> [--out PATH] [--prefix PREFIX]")

    images_dir = args[0]
    out_manifest = OUT_MANIFEST
    prefix = None

    i = 1
    while i < len(args):
        if args[i] == "--out":
            if i + 1 >= len(args):
                raise SystemExit("--out requires a path")
            out_manifest = args[i + 1]
            i += 2
            continue
        if args[i] == "--prefix":
            if i + 1 >= len(args):
                raise SystemExit("--prefix requires a value")
            prefix = args[i + 1]
            i += 2
            continue
        i += 1

    main(images_dir, out_manifest=out_manifest, prefix=prefix)

