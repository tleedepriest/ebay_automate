# Makefile

PY ?= python3

# ---------- upload ----------
IMAGES_DIR ?=
PREFIX ?=
OUT_MANIFEST ?= tmp/upload_manifest.jsonl

# ---------- identify ----------
IN_MANIFEST ?= tmp/upload_manifest.jsonl
OUT_IDENTS ?= tmp/card_identifications.jsonl
MIN_YEAR ?=
EXTRA_PROMPT_INFORMATION ?=

# ---------- lookup ----------
IN_IDENTS ?= tmp/card_identifications.jsonl
OUT_MATCHES ?= tmp/card_matches.jsonl
OUT_REVIEW ?= tmp/match_review.csv

# ---------- build ----------
OUT_CSV ?= tmp/Batch.csv

CATEGORY ?= 183454
STORE_CATEGORY ?= 0
CONDITION_ID ?= 4000
#CARD_CONDITION ?= Near mint or better - (ID: 400010)
CARD_CONDITION ?= Lightly Played (Excellent) - (ID: 400015)

LOCATION ?= rockville, md
POSTAL_CODE ?= 20850
DISPATCH_TIME ?= 1

SHIPPING_PROFILE ?= free_shipping_under_20
RETURN_PROFILE ?= 30_day_returns
PAYMENT_PROFILE ?= buy_it_now

BEST_OFFER_ENABLED ?= 0
CUSTOM_LABEL ?= batch-auto

.PHONY: upload identify lookup build all clean

upload:
	@if [ -z "$(IMAGES_DIR)" ]; then \
	  echo "ERROR: IMAGES_DIR is required"; \
	  exit 1; \
	fi
	$(PY) upload_and_manifest.py "$(IMAGES_DIR)" --out "$(OUT_MANIFEST)"

identify:
	@if [ ! -f "$(IN_MANIFEST)" ]; then \
	  echo "ERROR: missing $(IN_MANIFEST). Run upload first."; \
	  exit 1; \
	fi
	$(PY) identify_from_manifest.py \
		$(if $(MIN_YEAR), $(MIN_YEAR)) \
		$(if $(EXTRA_PROMPT_INFORMATION), "$(EXTRA_PROMPT_INFORMATION)")

lookup:
	@if [ ! -f "$(OUT_IDENTS)" ]; then \
	  echo "ERROR: missing $(OUT_IDENTS). Run identify first."; \
	  exit 1; \
	fi
	$(PY) lookup_cards.py

build:
	@if [ ! -f "$(OUT_REVIEW)" ]; then \
	  echo "ERROR: missing $(OUT_REVIEW). Run lookup first."; \
	  exit 1; \
	fi
	$(PY) build_ebay_batch_csv.py \
		--manifest "$(OUT_MANIFEST)" \
		--idents "$(OUT_IDENTS)" \
		--match-review "$(OUT_REVIEW)" \
		--out "$(OUT_CSV)" \
		--category "$(CATEGORY)" \
		--store-category "$(STORE_CATEGORY)" \
		--condition-id "$(CONDITION_ID)" \
		--card-condition "$(CARD_CONDITION)" \
		--location "$(LOCATION)" \
		--postal-code "$(POSTAL_CODE)" \
		--dispatch-time "$(DISPATCH_TIME)" \
		--shipping-profile "$(SHIPPING_PROFILE)" \
		--return-profile "$(RETURN_PROFILE)" \
		--payment-profile "$(PAYMENT_PROFILE)" \
		--best-offer-enabled "$(BEST_OFFER_ENABLED)" \
		--customlabel "$(CUSTOM_LABEL)"

all: upload identify lookup build

clean:
	rm -f tmp/upload_manifest.jsonl
	rm -f tmp/card_identifications.jsonl
	rm -f tmp/card_matches.jsonl
	rm -f tmp/match_review.csv
	rm -f tmp/Batch.csv
