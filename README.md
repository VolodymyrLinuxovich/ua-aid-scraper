# ua-aid-scraper
Big Data Preparation (URAP) tool to turn Kiel’s bilateral aid data + web evidence (HTML/PDF) into a monthly, delivery-focused Excel workbook with military inventory transfers, humanitarian aid, and loan instruments.

## Abstract

This project was built for the URAP “Big Data Preparation” track to reproduce and extend donor-side evidence on aid to Ukraine. The tool ingests the Kiel Institute bilateral dataset, discovers corroborating sources on vetted government and major media domains, extracts concrete evidence of deliveries, items and quantities, normalizes monetary values into EUR, and compiles a clean Excel output suitable for QC and econometric analysis. When sources state quantities without prices, the pipeline estimates values using a maintainable catalog of unit costs and marks the result as an estimate rather than a reported figure.



## Problem Statement

Public reporting often blends announcements and commitments with actual deliveries. Monetary amounts, if present, are expressed in different currencies, formats, and languages. Military, humanitarian and financial instruments are mixed in the same streams, which obscures comparative analysis. Evidence is widely dispersed across government portals, PDFs, and media articles. Analysts need a reproducible way to isolate deliveries, reconcile currencies, and summarize the facts by month and donor with clear sourcing.

## Solution Overview

The pipeline starts from Kiel’s bilateral aid table and narrows rows to a chosen donor. It constructs scoped search queries against whitelisted domains, follows the first organic result per query, and fetches either HTML or PDFs. From the text it infers status (“Delivered/Disbursed” vs “Commitment/Other”), identifies an evidence month using multilingual date parsing, extracts item names and quantities, and pulls the largest money mention with currency tags or words. Values are converted to EUR using a live FX API with a conservative fallback table. If a credible price is not present, the system estimates from quantities using unit cost heuristics and labels the calculation accordingly. A monthly “Military Inventory Transfer” sheet is then produced with a US-style layout and a total line; humanitarian and loan sheets are written in parallel, along with a de-duplicated “Sources To Check” list and a small QC dashboard.

## Methodology and Pipeline

1. **Normalize Kiel data**
   - Standardize headers and measures; compute `month` from dates.
   - Classify each row into coarse buckets: military inventory, humanitarian, loans, other.

2. **Build targeted search queries**
   - Compose deterministic Google queries from donor, item text, month, and amount.
   - Apply a country profile (language + trusted domains) to scope results.

3. **Resolve and fetch sources**
   - Open the first organic result per query; fetch HTML or PDF.
   - Use short timeouts and minimal retries for deterministic, fast runs.

4. **Extract facts from text**
   - Detect delivery verbs in context windows to infer status (Delivered vs Commitment).
   - Derive an **evidence month** via `dateparser` (multilingual, 2022–2026).
   - Recognize weapons/calibers with curated patterns; capture quantities (e.g., “31 Bradley”, “5,000 155 mm rounds”).
   - Pull money mentions with symbols or worded currencies and multipliers.

5. **Normalize & estimate values**
   - Convert extracted amounts to **EUR** (live FX with conservative fallbacks).
   - If no price is present, estimate from **quantity × unit cost** (transparent heuristics).
   - Depreciate **stockpile** transfers using useful-life class heuristics.

6. **Write outputs**
   - Keep row-level **Military Raw (auto)** with links and fields for auditability.
   - Aggregate to monthly **Military Inventory Transfer (MIT)** with a total line and compact descriptions.
   - Produce parallel **Loans** and **Humanitarian** sheets, plus **Sources To Check** and a small **QC** view.

---

## Parsing Details

**Status inference.** The text is scanned for delivery verbs (“delivered”, “handed over”, “arrived”, “transferred”) versus announcement language (“announced”, “pledged”, “authorized”). Presence of the former sets the row to Delivered/Disbursed; otherwise it remains Commitment/Other until better evidence is found.

**Evidence month.** The parser searches for dates in the vicinity of delivery verbs and then across the article body, prioritizing months in 2022-2026. The month string becomes the evidence anchor for aggregation.

**Items and quantities.** Items are detected using robust patterns for common systems (Patriot, NASAMS, HIMARS, AMRAAM, ATACMS, Bradley, Leopard, Abrams, M113, CV90, F-16 support, 155/152/122/120/105 mm ammo, loitering munitions, Stinger/Javelin/NLAW, demining kits, night-vision, etc.). A complementary grammar extracts compact quantity phrases like “31 Bradley”, “2 Patriot batteries”, “5,000 155 mm rounds”, or “12 M113”.

**Money extraction.** Currency symbols before numbers and currency codes/words after numbers are supported. Multipliers such as million/billion (and their multilingual variants) are recognized, and the largest coherent amount is taken as the package value. When the source lacks a figure, the pipeline falls back to quantity × unit cost estimation.

**Currency normalization.** Values are converted to EUR using `exchangerate.host` where available; if offline, a baked-in conservative table prevents failure and keeps magnitudes reasonable.

**Useful life and depreciation.** Stockpile transfers are depreciated with simple straight-line logic using class heuristics: heavy systems at roughly 25-30 years, drones around 6, non-lethal gear ~5, and munitions set to zero useful life. If the send year is known, half-life approximations are applied; otherwise the model defaults to no depreciation.

## Configuration

Environment variables control network timeouts, parallelism, PDF handling, and scraping breadth. Reasonable defaults balance speed with coverage and can be tightened for quick trials or relaxed for deeper runs.

| Variable               | Default | Purpose                                                     |
|------------------------|:-------:|-------------------------------------------------------------|
| `AID_REQ_TIMEOUT`      | `7.0`   | Per-request timeout for page/PDF fetch (seconds).           |
| `AID_GOOGLE_TIMEOUT`   | `5.0`   | Timeout for Google result pages (seconds).                  |
| `AID_SCRAPE_LIMIT`     | `8`     | Upper bound for bootstrap/enrichment rows per run.          |
| `AID_THREADS`          | `8`     | Worker threads for concurrent enrichment.                   |
| `AID_TOTAL_BUDGET_SEC` | `90`    | Soft global budget for the scrape stage (seconds).          |
| `AID_SKIP_PDF`         | `1`     | If `1`, skip PDFs for speed; set `0` to enable PDF parsing. |

> Tip: For quick smoke tests, try smaller `AID_SCRAPE_LIMIT` and keep `AID_SKIP_PDF=1`. For deeper recall, increase both `AID_SCRAPE_LIMIT` and `AID_THREADS`.

## Usage

### Interactive run

Point the script to your Kiel workbook (or a folder with it), select a donor, and the tool will write `<donor>_compiled.xlsx`.

```bash
python build_and_enrich.py

