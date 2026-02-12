import sys
import re
import requests
from bs4 import BeautifulSoup

from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.common.keys import Keys
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC


PRICE_RE = re.compile(r"\$?\s*([0-9]+(?:\.[0-9]{1,2})?)")


def google_first_pricecharting_url(query, chromedriver_path):
    opts = Options()
    #opts.add_argument("--headless=new")

    service = Service(chromedriver_path)
    driver = webdriver.Chrome(service=service, options=opts)

    try:
        driver.get("https://www.google.com")

        box = WebDriverWait(driver, 20).until(
            EC.presence_of_element_located((By.NAME, "q"))
        )

        box.send_keys(f"site:pricecharting.com/game {query}")
        box.send_keys(Keys.ENTER)

        WebDriverWait(driver, 20).until(
            EC.presence_of_element_located((By.ID, "search"))
        )

        for a in driver.find_elements(By.CSS_SELECTOR, "a"):
            href = a.get_attribute("href")
            if href and "pricecharting.com/game/" in href:
                return href

        raise Exception("No PriceCharting link found")

    finally:
        driver.quit()


def fetch_ungraded_price(url):
    r = requests.get(url, headers={"User-Agent": "Mozilla/5.0"}, timeout=20)
    r.raise_for_status()

    soup = BeautifulSoup(r.text, "html.parser")
    el = soup.select_one("table#price_data td#used_price span.price")

    if not el:
        raise Exception("Price element not found")

    return float(el.text.replace("$", "").strip())


if __name__ == "__main__":
    if len(sys.argv) < 3:
        print("Usage: python google_pricecharting_chrome.py /path/to/chromedriver \"SEARCH QUERY\"")
        sys.exit(1)

    chromedriver = sys.argv[1]
    query = sys.argv[2]

    url = google_first_pricecharting_url(query, chromedriver)
    price = fetch_ungraded_price(url)

    print("PriceCharting URL:", url)
    print("Ungraded Price:", price)

