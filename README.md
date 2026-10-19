# property-research

A Claude Code plugin that researches a US residential property from its address:

- **5-year market trend** for the ZIP: Zillow home value index (ZHVI) and Realtor.com listing
  supply/demand (inventory, days on market, price cuts, pending ratio).
- **Nearby sales history**: quarterly median price and $/sqft for same-type homes within 0.5 mi.
- **Same-model sales**: near-identical homes (same beds, sqft ±5%, built ±5 yrs), restated in
  today's dollars.
- **Comparable sales** from the last 6 months, scored for similarity and adjusted for size and
  time, with a weighted value range.
- **Active listings** competing with the subject.

## Install

```
/plugin marketplace add litlig/property-research-plugin
/plugin install property-research@property-research
```

Then ask Claude, e.g. "research 123 Main St, Springfield, IL 62701", or run
`/property-research:property-research <address>`.

Requires [uv](https://docs.astral.sh/uv/). The first run downloads about 1 GB of public CSVs
and converts them to Parquet in `~/.cache/property-research` (override with
`PROPERTY_RESEARCH_CACHE`); the cache settles around 150 MB. Later runs take seconds and check for
new data once a day.

## Data sources

| Data | Source |
|---|---|
| Geocoding | US Census Bureau geocoder, OpenStreetMap Nominatim fallback |
| Home value index | [Zillow Research](https://www.zillow.com/research/data/) ZHVI by ZIP |
| Listing inventory | [Realtor.com Research](https://www.realtor.com/research/data/) inventory by ZIP |
| Sold homes, active listings | Redfin's public "Download All" CSV endpoint |

**Redfin's CSV endpoint is undocumented and unofficial.** It may change or block requests at any
time, and automated use may conflict with Redfin's terms of use; review them and decide for
yourself. The script identifies itself with its own User-Agent, spaces requests 1.5 s apart,
caches responses for 12 h and makes at most 20 requests per report (about 6–10 normally). Pass
`--no-redfin` to use only Zillow and Realtor.com data (no subject record, comps or listings).

Zillow and Realtor.com research data are free to use with attribution; each report ends with a
sources line. Keep it when you share results.

Output is an automated estimate, not an appraisal.

## License

MIT
