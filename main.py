import sys
import json
from identify_card import identify_card
from query_google import google_first_pricecharting_game_url
from pricecharting import (
    format_search_query,
    fetch_pricecharting_ungraded_price
)

def main():
    if len(sys.argv) < 2:
        print("Usage: python main.py path/to/card_image.png")
        sys.exit(1)

    image_path = sys.argv[1]

    # 1) Identify card
    card = identify_card(image_path)
    print("Card JSON:", card)

    # convert string output into json
    card = json.loads(card)
    # 2) Build PriceCharting query
    query = format_search_query(card)
    print("Search Query:", query)

    # 3) Find PriceCharting product page
    product_url = google_first_pricecharting_game_url(query)
    print("PriceCharting URL:", product_url)

    # 4) Scrape price
    price = fetch_pricecharting_ungraded_price(product_url)
    print("Ungraded Price:", price)

if __name__ == "__main__":
    main()

