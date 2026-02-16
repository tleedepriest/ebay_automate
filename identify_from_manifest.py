import os
import sys
import base64
import json
from openai import OpenAI

client = OpenAI()

IN_MANIFEST = "tmp/upload_manifest.jsonl"
TMP_OUT = "tmp/card_identifications.jsonl"

def to_data_url(path: str) -> str:
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

    resp = client.responses.create(
        model="gpt-4o-mini",
        input=[{
            "role": "user",
            "content": [
                {"type": "input_text", "text": prompt},
                {"type": "input_image", "image_url": img_url},
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

    with open(IN_MANIFEST, "r", encoding="utf-8") as fin, \
         open(TMP_OUT, "w", encoding="utf-8") as fout:

        for line in fin:
            line = line.strip()
            if not line:
                continue

            rec = json.loads(line)

            idx = rec.get("listing_index")
            front_local = rec.get("front_local")

            if not front_local or not os.path.exists(front_local):
                out = {**rec, "error": f"front_local missing or not found: {front_local}"}
                fout.write(json.dumps(out) + "\n")
                print(f"[{idx}] FAIL missing front image:", front_local)
                continue

            try:
                data = identify_card(front_local, min_copyright_year=min_year, extra_prompt_information=extra_prompt_information)

                out = {
                    **rec,
                    **data,
                    "image": front_local,
                    "min_copyright_year": min_year,  # record what rule was used
                }

                fout.write(json.dumps(out) + "\n")

                cy = data.get("copyright_year")
                yr_ok = data.get("year_in_range")
                print(
                    f"[{idx}] OK {data['card_name']} | ©{cy} | year_ok={yr_ok} | "
                    f"set_size={data['set_size']} | #{data['collector_number']} | conf={data['confidence']}"
                )

            except Exception as e:
                out = {**rec, "error": str(e), "image": front_local, "min_copyright_year": min_year}
                fout.write(json.dumps(out) + "\n")
                print(f"[{idx}] FAIL {front_local} -> {e}")

    print("Wrote:", TMP_OUT)

if __name__ == "__main__":
    main()

