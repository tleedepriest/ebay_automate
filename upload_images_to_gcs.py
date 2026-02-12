import csv
import mimetypes
from pathlib import Path
from google.cloud import storage

BUCKET = "ebay-automate-picture-hosting"
LOCAL_DIR = "/media/sf_VM_shared/cards/"
PREFIX = "cards/"  # you can change to "batch-001/" per run if you want
OUT_CSV = "pic_urls.csv"

ALLOWED_EXT = {".jpg", ".jpeg", ".png", ".webp"}

def public_url(bucket: str, blob_name: str) -> str:
    # Works when bucket objects are publicly readable
    return f"https://storage.googleapis.com/{bucket}/{blob_name}"

def upload_folder(bucket_name: str, local_dir: str, prefix: str, out_csv: str):
    client = storage.Client()
    bucket = client.bucket(bucket_name)

    local_dir = Path(local_dir)
    files = sorted([p for p in local_dir.iterdir() if p.is_file() and p.suffix.lower() in ALLOWED_EXT])

    rows = []
    for p in files:
        blob_name = f"{prefix}{p.name}"
        blob = bucket.blob(blob_name)

        content_type, _ = mimetypes.guess_type(str(p))
        if not content_type:
            content_type = "image/jpeg"

        # Upload
        blob.upload_from_filename(str(p), content_type=content_type)

        url = public_url(bucket_name, blob_name)
        rows.append({"filename": p.name, "gcs_path": blob_name, "picurl": url})
        print("Uploaded:", p.name, "->", url)

    # Write mapping CSV
    with open(out_csv, "w", newline="", encoding="utf-8") as f:
        w = csv.DictWriter(f, fieldnames=["filename", "gcs_path", "picurl"])
        w.writeheader()
        w.writerows(rows)

    print(f"\nWrote {out_csv} with {len(rows)} rows.")

if __name__ == "__main__":
    upload_folder(BUCKET, LOCAL_DIR, PREFIX, OUT_CSV)

