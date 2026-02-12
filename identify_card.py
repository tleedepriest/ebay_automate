import sys
import os
import base64
import time
import json
from openai import OpenAI

client = OpenAI()

TMP_OUT = "tmp/card_identifications.jsonl"

def to_data_url(path: str) -> str:
    with open(path, "rb") as f:
        b64 = base64.b64encode(f.read()).decode("utf-8")
    return f"data:image/png;base64,{b64}"

schema = {
    "type": "object",
    "properties": {
        "card_name": {"type": "string"},
        "language": {"type": "string"},
        "collector_number": {"type": "string"},   # "103/165"
        "set_size": {"type": "integer"},          # 165

        # allow null instead of forcing wrong guesses
        "copyright_year": {
            "type": ["integer", "null"],
            "description": "Copyright year printed on the card (e.g., 2023). Null if unreadable."
        },

        "set_name": {"type": "string"},
        "confidence": {"type": "number"},

        # optional but useful for downstream logic
        "year_in_range": {"type": "boolean"}
    },
    "required": [
        "card_name",
        "language",
        "collector_number",
        "set_size",
        "copyright_year",
        "set_name",
        "confidence",
        "year_in_range"
    ],
    "additionalProperties": False
}

def identify_card(
    image_path: str,
    min_copyright_year: int | None = None,
    max_copyright_year: int | None = None,
):
    img_url = to_data_url(image_path)

    # Build a tight instruction block for the year constraint.
    # The key is "do not guess" + "null if unreadable" + "range gate".
    year_rules = []
    if min_copyright_year is not None:
        year_rules.append(f"copyright year must be >= {min_copyright_year}")
    if max_copyright_year is not None:
        year_rules.append(f"copyright year must be <= {max_copyright_year}")

    year_rule_text = ""
    if year_rules:
        year_rule_text = (
            "Year constraint: " + " and ".join(year_rules) + ".\n"
            "Set year_in_range=true only if the year is readable AND satisfies the constraint.\n"
            "If the year is unreadable, set copyright_year=null and year_in_range=false.\n"
            "If the year is readable but out of range, keep the extracted year and set year_in_range=false.\n"
        )
    else:
        year_rule_text = (
            "If the copyright year is unreadable, set copyright_year=null and year_in_range=false.\n"
            "If readable, set year_in_range=true.\n"
        )

    prompt = (
        "Identify this Pokémon card from the image.\n"
        "Extract EXACTLY as printed:\n"
        "- collector_number in the format X/Y (e.g., 103/165)\n"
        "- set_size as Y\n"
        "- copyright_year as the © year printed on the card\n\n"
        "IMPORTANT:\n"
        "- Do NOT guess the copyright year. If you cannot read it confidently, return null.\n"
        "- If multiple years appear, use the © copyright year (not set release year).\n"
        f"{year_rule_text}"
        "Set confidence from 0 to 1 (lower if anything is uncertain).\n"
    )

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

if __name__ == "__main__":
    os.makedirs("tmp", exist_ok=True)

    # Usage:
    # python script.py /path/to/image.png 2020
    #
    # argv[1] -> image path
    # argv[2] -> min copyright year (optional)

    if len(sys.argv) < 2:
        print("Usage: python script.py <image_path> [min_copyright_year]")
        sys.exit(1)

    img = sys.argv[1]

    min_year = None
    if len(sys.argv) >= 3:
        try:
            min_year = int(sys.argv[2])
        except ValueError:
            print("min_copyright_year must be an integer")
            sys.exit(1)

    data = identify_card(
        img,
        min_copyright_year=min_year
    )

    data["image"] = img

    time.sleep(0.5)  # prevent too many API requests

    with open(TMP_OUT, "w") as out:
        out.write(json.dumps(data) + "\n")

    print("something:", img, data)

