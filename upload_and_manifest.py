import os, json, mimetypes
from pathlib import Path
from google.cloud import storage

# ---- config ----
BUCKET = "ebay-automate-picture-hosting"   # bucket should be public-readable for eBay PicURL
IMAGES_DIR = "/media/sf_VM_shared/cards/fb-market-place-lot-2026-02-03/english"            # your scanned images folder
OUT_MANIFEST = "tmp/upload_manifest.jsonl"

def list_images_in_order(folder: str):
    exts = {".jpg", ".jpeg", ".png", ".webp"}
    files = [p for p in Path(folder).iterdir() if p.is_file() and p.suffix.lower() in exts]
    files.sort(key=lambda p: p.name)  # "in order" via filename sort
    return files

def pair_front_back(files):
    if len(files) % 2 != 0:
        raise ValueError(f"Expected even number of images (front/back pairs). Got {len(files)}")
    pairs = []
    for i in range(0, len(files), 2):
        pairs.append((files[i], files[i+1]))  # front, back
    return pairs

def upload_file(bucket, local_path: Path, object_name: str):
    blob = bucket.blob(object_name)
    ctype, _ = mimetypes.guess_type(str(local_path))
    blob.upload_from_filename(str(local_path), content_type=ctype or "image/jpeg")
    # Public URL format (works if bucket/objects are publicly readable)
    return f"https://storage.googleapis.com/{bucket.name}/{object_name}"

def main():
    os.makedirs("tmp", exist_ok=True)

    files = list_images_in_order(IMAGES_DIR)
    pairs = pair_front_back(files)

    client = storage.Client()
    bucket = client.bucket(BUCKET)

    with open(OUT_MANIFEST, "w", encoding="utf-8") as f:
        for idx, (front, back) in enumerate(pairs, start=1):
            # object names don’t rely on prefix; they’re deterministic by index
            front_obj = f"{idx:04d}_front{front.suffix.lower()}"
            back_obj  = f"{idx:04d}_back{back.suffix.lower()}"

            front_url = upload_file(bucket, front, front_obj)
            back_url  = upload_file(bucket, back, back_obj)

            rec = {
                "listing_index": idx,
                "front_local": str(front),
                "back_local": str(back),
                "front_object": front_obj,
                "back_object": back_obj,
                "front_url": front_url,
                "back_url": back_url,
            }
            f.write(json.dumps(rec) + "\n")
            print(f"[{idx}] uploaded -> front/back")

    print("Wrote", OUT_MANIFEST)

if __name__ == "__main__":
    main()

