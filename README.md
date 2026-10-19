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
/plugin marketplace add <owner>/property-research-plugin
/plugin install property-research@property-research
```

Then ask Claude, e.g. "research 123 Main St, Springfield, IL 62701" or run `/property-research <address>`.

Requires [uv](https://docs.astral.sh/uv/). The first run downloads about 1 GB of public datasets
into `~/.cache/property-research` (override with `PROPERTY_RESEARCH_CACHE`); later runs take
seconds.

## Data sources

| Data | Source |
|---|---|
| Geocoding | US Census Bureau geocoder, OpenStreetMap Nominatim fallback |
| Home value index | [Zillow Research](https://www.zillow.com/research/data/) ZHVI by ZIP |
| Listing inventory | [Realtor.com Research](https://www.realtor.com/research/data/) inventory by ZIP |
| Sold homes, active listings | Redfin's public "Download All" CSV endpoint |

Redfin's CSV endpoint is undocumented and unofficial; it may change or block requests, and its
use is subject to Redfin's terms. The script rate-limits and caches requests. Zillow and
Realtor.com data are used under their research-data terms; attribute them when you republish
results.

Output is an automated estimate, not an appraisal.

## License

MIT
