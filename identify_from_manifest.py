import os
import sys
import io
import time
import threading
import base64
import json
from concurrent.futures import ThreadPoolExecutor, as_completed
from openai import OpenAI
from PIL import Image

# max_retries lets the SDK absorb transient 429s (tokens-per-min bursts) on its own:
# it backs off with jitter and honors the Retry-After the rate-limit error returns
# (e.g. "try again in 229ms"), so spikes self-heal instead of failing the card.
client = OpenAI(max_retries=int(os.environ.get("OPENAI_MAX_RETRIES", "8")))

IN_MANIFEST = "tmp/upload_manifest.jsonl"
TMP_OUT = "tmp/card_identifications.jsonl"

# Identify calls are independent network round-trips, so we fan them out across a
# thread pool. The OpenAI client is thread-safe. More workers = higher instantaneous
# token rate, so if you hit tokens-per-min (TPM) limits, lower IDENTIFY_WORKERS.
# Output is still written in manifest order (downstream stages join by line
# position), so concurrency does not change the result file.
MAX_WORKERS = int(os.environ.get("IDENTIFY_WORKERS", "8"))

# Pacing: enforce a minimum gap between request *starts* so a burst of workers
# doesn't slam the tokens-per-min (TPM) limit at once. At high detail each image is
# ~25.6k tokens, so a 200k TPM cap sustains only ~7.8 req/min -> ~8s apart. That is
# the binding constraint at high detail (workers can't beat the TPM cap), so we pace
# to stay just under it. If your account has a higher TPM tier, lower this for more
# speed; raise it if you still see 429s. Set to 0 to disable. The SDK's max_retries
# backstops any residual spikes.
REQUEST_INTERVAL = float(os.environ.get("IDENTIFY_REQUEST_INTERVAL", "8"))
_throttle_lock = threading.Lock()
_last_send = [0.0]

def _throttle():
    """Block until at least REQUEST_INTERVAL has elapsed since the last request start."""
    if REQUEST_INTERVAL <= 0:
        return
    with _throttle_lock:
        now = time.monotonic()
        wait = _last_send[0] + REQUEST_INTERVAL - now
        if wait > 0:
            time.sleep(wait)
            now = time.monotonic()
        _last_send[0] = now

# These scans are ~0.8-1.7 MB PNGs. Re-encoding to JPEG is the main payload win;
# downscaling to a long edge of MAX_EDGE px trims vision tiles further. 1024 keeps
# small printed text (collector number, © year) legible while still cutting payload
# and latency. Set IDENTIFY_MAX_EDGE=0 to disable downscaling and send originals.
MAX_EDGE = int(os.environ.get("IDENTIFY_MAX_EDGE", "1024"))
JPEG_QUALITY = int(os.environ.get("IDENTIFY_JPEG_QUALITY", "85"))

# Vision detail level drives BOTH accuracy and token cost on gpt-4o-mini:
#   high/auto -> ~25.6k tokens/image (~8 images/min under a 200k TPM cap), best
#                at reading small text (collector number, © year).
#   low       -> ~2.9k tokens/image (~67/min), much faster / no rate limits, but
#                less reliable on small text (year + occasional set_size misreads).
# Set IDENTIFY_IMAGE_DETAIL=low|high|auto. Default high: we prioritize accurate
# reading of the collector number and © year over speed.
IMAGE_DETAIL = os.environ.get("IDENTIFY_IMAGE_DETAIL", "high").lower()

def to_data_url(path: str) -> str:
    # Fast path: downscaling disabled -> send original bytes untouched.
    if MAX_EDGE <= 0:
        with open(path, "rb") as f:
            b64 = base64.b64encode(f.read()).decode("utf-8")
        ext = os.path.splitext(path)[1].lower()
        if ext in [".jpg", ".jpeg"]:
            mime = "image/jpeg"
        elif ext == ".webp":
            mime = "image/webp"
        else:
            mime = "image/png"
        return f"data:{mime};base64,{b64}"

    # Downscale to MAX_EDGE on the long side and re-encode as JPEG.
    with Image.open(path) as im:
        im = im.convert("RGB")
        im.thumbnail((MAX_EDGE, MAX_EDGE))  # preserves aspect ratio, no upscaling
        buf = io.BytesIO()
        im.save(buf, "JPEG", quality=JPEG_QUALITY)
    b64 = base64.b64encode(buf.getvalue()).decode("utf-8")
    return f"data:image/jpeg;base64,{b64}"

# Updated schema:
# - copyright_year can be null (instead of forcing wrong integer guesses)
# - year_in_range included for easy downstream filtering
schema = {
    "type": "object",
    "properties": {
        "card_name": {"type": "string"},
        "language": {"type": "string"},
        "collector_number": {"type": "string"},   # "103/165"
        "set_size": {"type": "integer"},          # 165
        "copyright_year": {"type": ["integer", "null"]},  # 2023 or null
        "year_in_range": {"type": "boolean"},
        "confidence": {"type": "number"}
    },
    "required": [
        "card_name",
        "language",
        "collector_number",
        "set_size",
        "copyright_year",
        "year_in_range",
        "confidence"
    ],
    "additionalProperties": False
}

def identify_card(image_path: str, min_copyright_year: int | None = None, extra_prompt_information: str = ""):
    img_url = to_data_url(image_path)

    # Build year constraint instructions (prompt-level)
    if min_copyright_year is not None:
        year_block = (
            f"Hard constraint: copyright year must be >= {min_copyright_year}.\n"
            "If the copyright year is not clearly readable, set copyright_year=null and year_in_range=false.\n"
            "If the year is readable but < min_copyright_year, keep the extracted year and set year_in_range=false.\n"
            "Do NOT guess the year.\n"
        )
    else:
        year_block = (
            "If the copyright year is not clearly readable, set copyright_year=null and year_in_range=false.\n"
            "If the year is readable, set year_in_range=true.\n"
            "Do NOT guess the year.\n"
        )

    prompt = (
        "Identify this Pokémon card.\n"
        "Extract EXACTLY as printed on the card:\n"
        "- collector_number in X/Y format (e.g., 103/165)\n"
        "- set_size as Y\n"
        "- copyright_year as the © year printed on the card (NOT the set release year)\n\n"
        f"{year_block}\n"
        "If uncertain about any field, lower confidence (0 to 1).\n"
    )

    prompt = prompt + extra_prompt_information

    image_part = {"type": "input_image", "image_url": img_url}
    if IMAGE_DETAIL in ("low", "high"):
        image_part["detail"] = IMAGE_DETAIL

    _throttle()  # pace request starts to stay under the TPM limit
    resp = client.responses.create(
        model="gpt-4o-mini",
        input=[{
            "role": "user",
            "content": [
                {"type": "input_text", "text": prompt},
                image_part,
            ]
        }],
        text={
            "format": {
                "type": "json_schema",
                "name": "card_id",
                "schema": schema,
                "strict": True
            }
        }
    )

    return json.loads(resp.output_text)

def main():
    os.makedirs("tmp", exist_ok=True)

    # CLI:
    # python script.py [min_copyright_year]
    min_year = None
    extra_prompt_information = ""
    if len(sys.argv) >= 2:
        try:
            min_year = int(sys.argv[1])
            extra_prompt_information = sys.argv[2]
        except ValueError:
            print("Usage: python script.py [min_copyright_year]")
            print("  min_copyright_year must be an integer, e.g. 2020")
            sys.exit(1)

    print(f"Reading manifest: {IN_MANIFEST}")
    print(f"Writing output:   {TMP_OUT}")
    print(f"Min year filter:  {min_year if min_year is not None else '(none)'}")
    print(f"Workers:          {MAX_WORKERS}")

    # Read the whole manifest first so we can keep the output in manifest order
    # while processing identifications concurrently.
    with open(IN_MANIFEST, "r", encoding="utf-8") as fin:
        records = [json.loads(l) for l in fin if l.strip()]

    def process(rec):
        """Run a single identification. Returns the JSONL-ready dict; never raises."""
        idx = rec.get("listing_index")
        front_local = rec.get("front_local")

        if not front_local or not os.path.exists(front_local):
            print(f"[{idx}] FAIL missing front image:", front_local)
            return {**rec, "error": f"front_local missing or not found: {front_local}"}

        try:
            data = identify_card(
                front_local,
                min_copyright_year=min_year,
                extra_prompt_information=extra_prompt_information,
            )
            cy = data.get("copyright_year")
            yr_ok = data.get("year_in_range")
            print(
                f"[{idx}] OK {data['card_name']} | ©{cy} | year_ok={yr_ok} | "
                f"set_size={data['set_size']} | #{data['collector_number']} | conf={data['confidence']}"
            )
            return {**rec, **data, "image": front_local, "min_copyright_year": min_year}
        except Exception as e:
            print(f"[{idx}] FAIL {front_local} -> {e}")
            return {**rec, "error": str(e), "image": front_local, "min_copyright_year": min_year}

    # Fan out across the pool; collect results by position to preserve manifest order.
    results = [None] * len(records)
    with ThreadPoolExecutor(max_workers=MAX_WORKERS) as ex:
        futs = {ex.submit(process, rec): i for i, rec in enumerate(records)}
        for fut in as_completed(futs):
            results[futs[fut]] = fut.result()

    with open(TMP_OUT, "w", encoding="utf-8") as fout:
        for out in results:
            fout.write(json.dumps(out) + "\n")

    print("Wrote:", TMP_OUT)

if __name__ == "__main__":
    main()

