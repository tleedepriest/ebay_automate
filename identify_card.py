import os
import base64
import json
from openai import OpenAI

client = OpenAI()

TMP_OUT = "tmp/card_identifications.jsonl"

def to_data_url(path: str) -> str:
    with open(path, "rb") as f:
        b64 = base64.b64encode(f.read()).decode("utf-8")
    # jpeg also fine if your files are jpg
    return f"data:image/png;base64,{b64}"

schema = {
    "type": "object",
    "properties": {
        "card_name": {"type": "string"},
        "language": {"type": "string"},
        "set_name": {"type": "string"},
        "set_code": {"type": "string"},
        "collector_number": {"type": "string"},
        "confidence": {"type": "number"}
    },
    "required": ["card_name", "language", "set_name", "set_code", "collector_number", "confidence"],
    "additionalProperties": False
}

def identify_card(image_path: str):
    img_url = to_data_url(image_path)

    resp = client.responses.create(
        model="gpt-4o-mini",
        input=[{
            "role": "user",
            "content": [
                {"type": "input_text", "text":
                    "Identify this Pokémon card. Return set + collector number exactly as printed. "
                    "If uncertain, make best guess and lower confidence."
                },
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

    return json.loads(resp.output_text)  # JSON string

if __name__ == "__main__":
    #identify_card(sys.argv[1])
    os.makedirs("tmp", exist_ok=True)
    img = "/media/sf_VM_shared/cards/img20260203_22323119.png"
    data = identify_card(img)
    data["image"] = img
    with open(TMP_OUT, "w") as out:
        out.write(json.dumps(data) + "\n")


    print("something:", img, data)
    

