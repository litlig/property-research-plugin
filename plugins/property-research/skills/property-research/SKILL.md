---
name: property-research
description: Research a US residential property from its address — 5-year market trend for its ZIP (home values, listing supply/demand), nearby sales history, comparable sales from the last 6 months with a comp-based value range, and current competing listings. Use when the user gives an address and asks for comps, comparable sales, "what is this house worth", market trend, neighborhood price history, or whether to buy/sell/list at a price.
---

# Property research

One script does the data pulls; your job is to run it, sanity-check what it found, fill gaps, and
write a clear answer.

## 1. Run the report

Requires [uv](https://docs.astral.sh/uv/) (`curl -LsSf https://astral.sh/uv/install.sh | sh`). The
script declares its own dependencies, so `uv run` installs them on first use.

```bash
uv run "${CLAUDE_SKILL_DIR}/scripts/property_research.py" report "<full address with city, state, ZIP>" --out ./property-reports
```

If `${CLAUDE_SKILL_DIR}` isn't expanded in your version, use the base directory shown when this
skill loaded. Output goes to `./property-reports` under the current directory; if that's a code
repo the user may not want it there, so pick another `--out` (e.g. `~/property-reports`) when in doubt.

Useful flags: `--type sfr|condo|townhouse|multi`, `--beds 3 --baths 2 --sqft 1850 --year-built 1962`
(override or fill the subject's specs), `--months 6` (comp window), `--radius 0.5` (starting comp
radius in miles; it auto-shrinks at the 350-row cap and auto-widens to 2 mi when fewer than 5 comps
match), `--trend-radius 0.5`, `--years 5` (1–20; local sales cap at 5), `--sqft-pct 0.25`,
`--beds-diff 1`, `--max-comps 10`, `--zip` (changes only the ZIP used for market data, not where
Redfin searches), `--no-redfin` (market trend only).
Include the unit for condos, preferably as `#4`, and confirm the matched unit in the report.

The first run downloads ~1 GB of Zillow and Realtor.com files into `~/.cache/property-research`
(about 1 minute on a fast connection; run it in the background if needed). Later runs take
10–20 seconds and recheck for new data once a day. `refresh` forces a re-download.

Output: markdown on stdout and a folder with `report.md`, `data.json`, `local_sales.csv`
(5 years of nearby same-type sales), `comps_all_matched.csv`, and `zhvi_monthly.csv`. The report also
lists **same-model sales** (same beds, sqft ±5%, built ±5 yrs) when the subject's specs are known.

## 2. Check before you trust it

Read the **Data notes** section first.

- **Subject specs.** If the subject wasn't matched (the note lists the units Redfin does have),
  isn't residential, or has no beds/sqft, ask the user for beds/baths/sqft/type (or the unit
  number) and rerun. The script skips the value estimate when it has no confirmed specs; if only
  beds are known it still estimates without size adjustment, so treat that as rough.
- **Comps.** Scan the table for things the score can't see: a different neighborhood or school
  boundary (a different ZIP across a freeway or river), new construction vs. original condition,
  multi-unit buildings labeled as houses, extreme $/sqft outliers. If the set is poor, rerun with
  tighter or looser flags. Note when the subject itself sold recently. Its own price, time-adjusted,
  is strong evidence.
- **Time adjustment** uses the ZIP's 12-month ZHVI change. If that's very large (say >15%/yr), say
  the estimate is sensitive to it.
- **Coverage.** Redfin's download leaves out some MLS listings. In non-disclosure states (TX, UT,
  NM, WY, ID, KS, MS, MO in part, ND, LA, AK, MT) prices come only from the MLS, so off-market
  sales are missing.

## 3. Fill gaps with the web (optional, brief)

The trend files are monthly. Zillow's lands mid-month for the prior month, and Realtor.com's on
the 1st. For anything newer or contextual, use WebSearch:
- `"<city or neighborhood> housing market <current month year>"` for local reports.
- Current 30-year mortgage rate (Freddie Mac PMMS, weekly).
- Any specific comp the user asks about, or news affecting the area (rezoning, a new employer).

redfin.com, zillow.com and realtor.com HTML pages block scripted fetches (403/429). Don't retry
them with WebFetch.

## 4. Present

Lead with the answer, then the evidence:
1. **Bottom line.** Comp-based value range and where the subject likely falls in it, plus a
   one-sentence market direction.
2. **Subject.** Specs and last sale.
3. **5-year trend.** ZHVI path (peak, trough, 1/3/5-yr change, recent momentum), the local
   quarterly $/sqft from actual nearby sales, and listing-market shifts (inventory, days on
   market, price cuts, pending ratio YoY).
4. **Comps.** The best 5–8, why they're comparable, and adjustments. When the report has a
   **Same-model sales** section (common in tract neighborhoods), lead with it: identical homes
   beat adjusted ones. Note same-model sales with different bath counts or additions.
5. **Active competition.** What a buyer would compare it against right now.
6. **Caveats and data as-of dates.** Make clear this is not an appraisal.

Link every individual sale or listing you cite to its Redfin URL. The URLs are in `report.md`,
`data.json`, `local_sales.csv` and `comps_all_matched.csv`, so never drop them when you trim a table.

If the user wants something visual or shareable, build an HTML artifact. Chart the ZHVI line from
`zhvi_monthly.csv` and quarterly $/sqft from `local_sales.csv`, and include a comps table/map.

## Data sources

| Need | Source | Freshness |
|---|---|---|
| Geocode, ZIP | US Census geocoder (Nominatim fallback) | live |
| Value trend | Zillow Research ZHVI by ZIP (all homes + SFR or condo) | monthly, ~mid-month |
| Supply/demand | Realtor.com Research inventory by ZIP | monthly, ~1st of month |
| Subject record, comps, local sales, active listings | Redfin `stingray/api/gis-csv` (the "Download All" endpoint; MLS + public record) | days |

Advanced: if `REDFIN_TRACKER_PARQUET` points to a local Parquet extract of Redfin Data Center's ZIP
market tracker, the report adds sale-to-list and homes-sold stats. That file updates irregularly;
the report shows its period end date.

## If Redfin blocks the endpoint

The report's notes say `Redfin unavailable (...)`. Don't retry in a loop. Fallbacks, in order:
1. **Ask the user** to export a Sold search from redfin.com themselves ("Download All" under the
   map), then analyze that CSV with pandas. It has the same columns the script parses.
2. **RentCast API** (`RENTCAST_API_KEY`, 50 free calls/month):
   `GET https://api.rentcast.io/v1/properties` with header `X-Api-Key: <key>` and params
   `latitude`, `longitude`, `radius` (miles) and `saleDateRange` (days). Records carry only the
   last sale.
3. County assessor/recorder open-data portals for sale records.

Redfin's CSV endpoint is unofficial. The script identifies itself, spaces requests 1.5 s apart,
caches them for 12 h and stops after 20 per run. Keep it that way: don't script extra Redfin calls.

The Zillow and Realtor.com trend sections still work without Redfin.
