import cv2
import pytesseract
import pandas as pd
import re
import glob

# Uncomment if Windows + tesseract not on PATH
# pytesseract.pytesseract.tesseract_cmd = r"C:\Program Files\Tesseract-OCR\tesseract.exe"

SET_RE = re.compile(r'([A-Z]{2,4})\s*([A-Z]{0,2})?\s*([0-9]{1,3}/[0-9]{1,3})')

def crop_rel(img, x0, y0, x1, y1):
    h, w = img.shape[:2]
    return img[int(y0*h):int(y1*h), int(x0*w):int(x1*w)]

def preprocess(img):
    gray = cv2.cvtColor(img, cv2.COLOR_BGR2GRAY)
    gray = cv2.resize(gray, None, fx=2, fy=2, interpolation=cv2.INTER_CUBIC)
    blur = cv2.GaussianBlur(gray, (5,5), 0)
    #thr = cv2.adaptiveThreshold(
    #    blur, 255,
    #    cv2.ADAPTIVE_THRESH_GAUSSIAN_C,
    #    cv2.THRESH_BINARY,
    #    31, 7
    #)
    return blur

def ocr_region(img):
    return pytesseract.image_to_string(
        img,
        config="--oem 3 --psm 7"
    ).strip()

def crop_bottom_left(img, width_pct=0.4, height_pct=0.25):
    h, w = img.shape[:2]
    return img[
        int(h*(1-height_pct)):h,
        0:int(w*width_pct)
    ]

def extract_set_and_number(path):
    img = cv2.imread(path)
    crop = crop_bottom_left(img, 0.40, 0.15)
    prep = preprocess(crop)
    cv2.imwrite("debug_crop.png", prep)
    text = ocr_region(prep)
    print(text)

    match = SET_RE.search(text)
    if match:
        print(match)
        set_code = match.group(1)
        region = match.group(2) or ""
        number = match.group(3)
        return set_code, region, number, text
    else:
        print("no match")

    return None, None, None, text

if __name__ == "__main__":
    rows = []

    for p in glob.glob("/media/sf_VM_shared/cards/*.png"):
        set_code, region, number, raw = extract_set_and_number(p)

        rows.append({
            "image": p,
            "set_code": set_code,
            "region": region,
            "collector_number": number,
            "ocr_raw": raw
        })

    df = pd.DataFrame(rows)
    df.to_csv("card_index.csv", index=False)
    print(df)

