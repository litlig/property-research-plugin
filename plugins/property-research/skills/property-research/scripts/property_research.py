# /// script
# requires-python = ">=3.11"
# dependencies = ["pandas>=2.2", "numpy>=1.26", "requests>=2.32", "duckdb>=1.1"]
# ///
"""Address-level real estate research: 5-year market trend + recent comparable sales.

Usage:
    uv run property_research.py report "123 Main St, Austin, TX 78704" [options]
    uv run property_research.py refresh          # force re-download of market datasets

Data sources (all free, no API key):
  * US Census geocoder (OSM Nominatim fallback)           address -> lat/lon/ZIP
  * Zillow Research ZHVI by ZIP (monthly, updated ~mid-month)   home value trend
  * Realtor.com Research inventory by ZIP (monthly, updated ~1st)   supply/demand
  * Redfin sold-homes CSV (the "Download All" endpoint, MLS-fed, days old)
        subject record, comps, local 5-year sales, active listings
  * Redfin Data Center ZIP tracker, if REDFIN_TRACKER_PARQUET points to a local extract (lags months)
"""

from __future__ import annotations

import argparse
import hashlib
import json
import math
import os
import re
import sys
import time
import urllib.parse
from dataclasses import asdict, dataclass
from datetime import datetime, timezone
from pathlib import Path

import duckdb
import numpy as np
import pandas as pd
import requests

CACHE = Path(os.environ.get("PROPERTY_RESEARCH_CACHE", Path.home() / ".cache" / "property-research"))
UA = ("Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 "
      "(KHTML, like Gecko) Chrome/130.0 Safari/537.36")
TODAY = pd.Timestamp.today().normalize()

ZILLOW_URL = "https://files.zillowstatic.com/research/public_csvs/zhvi/Zip_zhvi_uc_{t}_tier_0.33_0.67_sm_sa_month.csv"
REALTOR_URL = "https://econdata.s3-us-west-2.amazonaws.com/Reports/Core/RDC_Inventory_Core_Metrics_Zip_History.csv"
REDFIN_GIS = "https://www.redfin.com/stingray/api/gis-csv"
REDFIN_MAX_ROWS = 350  # the endpoint returns nothing if num_homes > 350
# Optional: a Parquet extract of Redfin's zip_code_market_tracker (columns as in Redfin's TSV).
REDFIN_TRACKER_PARQUET = os.environ.get("REDFIN_TRACKER_PARQUET")

# subject type -> (Redfin uipt filter, Zillow ZHVI series, Redfin tracker PROPERTY_TYPE)
TYPES = {
    "sfr": ("1", "sfr", "Single Family Residential"),
    "condo": ("2", "condo", "Condo/Co-op"),
    "townhouse": ("3", "sfrcondo", "Townhouse"),
    "multi": ("4", "sfrcondo", "Multi-Family (2-4 Unit)"),
}
REDFIN_TYPE = {
    "Single Family Residential": "sfr", "Condo/Co-op": "condo", "Townhouse": "townhouse",
    "Multi-Family (2-4 Unit)": "multi",
}
SIZE_ELASTICITY = 0.7  # a home 10% larger sells for ~7% more, all else equal


def log(msg: str) -> None:
    print(msg, file=sys.stderr, flush=True)


# ------------------------------------------------------------------ geocoding

def geocode(address: str) -> dict:
    try:
        r = requests.get(
            "https://geocoding.geo.census.gov/geocoder/locations/onelineaddress",
            params={"address": address, "benchmark": "Public_AR_Current", "format": "json"},
            timeout=20,
        )
        r.raise_for_status()
        m = r.json()["result"]["addressMatches"]
        if m:
            c = m[0]["addressComponents"]
            return {"matched": m[0]["matchedAddress"], "lat": m[0]["coordinates"]["y"],
                    "lon": m[0]["coordinates"]["x"], "zip": c.get("zip"),
                    "city": (c.get("city") or "").title(), "state": c.get("state"),
                    "source": "US Census geocoder"}
    except requests.RequestException as e:
        log(f"Census geocoder failed: {e}")
    r = requests.get("https://nominatim.openstreetmap.org/search",
                     params={"q": address, "format": "json", "addressdetails": 1,
                             "countrycodes": "us", "limit": 1},
                     headers={"User-Agent": "property-research-skill/1.0"}, timeout=20)
    r.raise_for_status()
    if not r.json():
        sys.exit(f"Could not geocode: {address}")
    h = r.json()[0]
    a = h.get("address", {})
    return {"matched": h["display_name"], "lat": float(h["lat"]), "lon": float(h["lon"]),
            "zip": (a.get("postcode") or "")[:5], "city": a.get("city") or a.get("town"),
            "state": a.get("ISO3166-2-lvl4", "US-")[-2:], "source": "OpenStreetMap Nominatim"}


# ------------------------------------------------------------------ dataset cache

def _meta() -> dict:
    p = CACHE / "meta.json"
    return json.loads(p.read_text()) if p.exists() else {}


def _save_meta(meta: dict) -> None:
    CACHE.mkdir(parents=True, exist_ok=True)
    (CACHE / "meta.json").write_text(json.dumps(meta, indent=1))


def ensure_dataset(url: str, parquet: Path, build, force: bool = False) -> dict:
    """Download `url` and convert it with `build(raw, parquet)` when the remote copy is newer.

    The remote Last-Modified header is checked at most once a day.
    """
    meta = _meta()
    m = meta.get(url, {})
    now = time.time()
    if parquet.exists() and not force and now - m.get("checked", 0) < 86400:
        return m
    remote = requests.head(url, timeout=30).headers.get("Last-Modified")
    m["checked"] = now
    if parquet.exists() and not force and remote == m.get("last_modified"):
        meta[url] = m
        _save_meta(meta)
        return m
    raw = parquet.with_suffix(".download")
    log(f"Downloading {url} ...")
    with requests.get(url, stream=True, timeout=60) as r, open(raw, "wb") as f:
        r.raise_for_status()
        for chunk in r.iter_content(1 << 20):
            f.write(chunk)
    build(raw, parquet)
    raw.unlink()
    m["last_modified"] = remote
    meta[url] = m
    _save_meta(meta)
    return m


def _build_zillow(raw: Path, parquet: Path) -> None:
    duckdb.execute(
        f"COPY (SELECT * FROM read_csv('{raw}', header=true, types={{'RegionName': 'VARCHAR'}})) "
        f"TO '{parquet}' (FORMAT parquet)"
    )


REALTOR_COLS = [
    "median_listing_price", "active_listing_count", "median_days_on_market", "new_listing_count",
    "pending_listing_count", "price_reduced_share", "median_listing_price_per_square_foot",
    "pending_ratio", "total_listing_count",
]


def _build_realtor(raw: Path, parquet: Path) -> None:
    casts = ", ".join(f"TRY_CAST({c} AS DOUBLE) AS {c}" for c in REALTOR_COLS)
    duckdb.execute(
        f"""COPY (
            SELECT TRY_CAST(month_date_yyyymm AS INTEGER) AS month,
                   lpad(CAST(postal_code AS VARCHAR), 5, '0') AS zip, zip_name, {casts}
            FROM read_csv('{raw}', header=true, all_varchar=true, ignore_errors=true)
            WHERE TRY_CAST(month_date_yyyymm AS INTEGER) IS NOT NULL
            ORDER BY zip, month
        ) TO '{parquet}' (FORMAT parquet, COMPRESSION zstd)"""
    )


def zillow_series(zip_code: str, series: str, force: bool = False) -> tuple[pd.Series, dict]:
    url = ZILLOW_URL.format(t=series)
    pq = CACHE / f"zillow_zhvi_{series}.parquet"
    CACHE.mkdir(parents=True, exist_ok=True)
    m = ensure_dataset(url, pq, _build_zillow, force)
    row = duckdb.execute(
        f"SELECT * FROM read_parquet('{pq}') WHERE lpad(RegionName, 5, '0') = ?", [zip_code]
    ).df()
    if row.empty:
        return pd.Series(dtype=float), m
    dates = [c for c in row.columns if re.fullmatch(r"\d{4}-\d{2}-\d{2}", c)]
    s = row.iloc[0][dates].astype(float)
    s.index = pd.to_datetime(dates)
    return s.dropna(), m


def realtor_frame(zip_code: str, force: bool = False) -> tuple[pd.DataFrame, dict]:
    pq = CACHE / "realtor_zip_history.parquet"
    CACHE.mkdir(parents=True, exist_ok=True)
    m = ensure_dataset(REALTOR_URL, pq, _build_realtor, force)
    df = duckdb.execute(f"SELECT * FROM read_parquet('{pq}') WHERE zip = ? ORDER BY month",
                        [zip_code]).df()
    df["date"] = pd.to_datetime(df["month"].astype(str), format="%Y%m") + pd.offsets.MonthEnd(0)
    return df, m


def redfin_tracker(zip_code: str, ptype: str) -> pd.DataFrame:
    if not REDFIN_TRACKER_PARQUET or not Path(REDFIN_TRACKER_PARQUET).expanduser().exists():
        return pd.DataFrame()
    return duckdb.execute(
        f"SELECT * FROM read_parquet('{Path(REDFIN_TRACKER_PARQUET).expanduser()}') WHERE REGION = ? AND PROPERTY_TYPE = ? "
        "ORDER BY PERIOD_END", [f"Zip Code: {zip_code}", ptype]
    ).df()


# ------------------------------------------------------------------ Redfin sold/active CSV

class RedfinBlocked(RuntimeError):
    pass


_last_call = 0.0


def circle_poly(lat: float, lon: float, miles: float, n: int = 24) -> str:
    dlat = miles / 69.0
    dlon = miles / (69.17 * math.cos(math.radians(lat)))
    pts = [(lon + dlon * math.cos(2 * math.pi * i / n), lat + dlat * math.sin(2 * math.pi * i / n))
           for i in range(n)]
    pts.append(pts[0])
    return ",".join(f"{x:.6f} {y:.6f}" for x, y in pts)


def redfin_csv(lat: float, lon: float, miles: float, *, sold_days: int | None,
               uipt: str = "1,2,3,4", ttl_hours: float = 12) -> pd.DataFrame:
    """Homes in a circle. sold_days=None -> active listings; otherwise sold within N days."""
    global _last_call
    params = {"al": 1, "num_homes": REDFIN_MAX_ROWS, "uipt": uipt, "v": 8,
              "poly": circle_poly(lat, lon, miles)}
    if sold_days is None:
        params["status"] = 1
    else:
        params.update(status=9, sold_within_days=sold_days)
    url = f"{REDFIN_GIS}?{urllib.parse.urlencode(params)}"
    cache = CACHE / "redfin" / (hashlib.md5(url.encode()).hexdigest() + ".csv")
    if cache.exists() and time.time() - cache.stat().st_mtime < ttl_hours * 3600:
        text = cache.read_text()
    else:
        time.sleep(max(0.0, 1.5 - (time.time() - _last_call)))
        r = requests.get(url, headers={"User-Agent": UA, "Accept": "text/csv,*/*"}, timeout=30)
        _last_call = time.time()
        if r.status_code != 200 or not r.text.startswith("SALE TYPE"):
            raise RedfinBlocked(f"Redfin returned HTTP {r.status_code} ({r.text[:120]!r})")
        text = r.text
        cache.parent.mkdir(parents=True, exist_ok=True)
        cache.write_text(text)
    return _parse_redfin(text)


def _parse_redfin(text: str) -> pd.DataFrame:
    from io import StringIO
    raw = pd.read_csv(StringIO(text))
    raw = raw[raw["SALE TYPE"].isin(["PAST SALE", "MLS Listing", "New Construction Home"])]
    url_col = next(c for c in raw.columns if c.startswith("URL"))
    df = pd.DataFrame({
        "status": raw["STATUS"], "sale_type": raw["SALE TYPE"],
        "sold_date": pd.to_datetime(raw["SOLD DATE"], format="%B-%d-%Y", errors="coerce"),
        "property_type": raw["PROPERTY TYPE"], "address": raw["ADDRESS"], "city": raw["CITY"],
        "zip": raw["ZIP OR POSTAL CODE"].astype(str).str[:5],
        "price": pd.to_numeric(raw["PRICE"], errors="coerce"),
        "beds": pd.to_numeric(raw["BEDS"], errors="coerce"),
        "baths": pd.to_numeric(raw["BATHS"], errors="coerce"),
        "sqft": pd.to_numeric(raw["SQUARE FEET"], errors="coerce"),
        "lot_sqft": pd.to_numeric(raw["LOT SIZE"], errors="coerce"),
        "year_built": pd.to_numeric(raw["YEAR BUILT"], errors="coerce"),
        "dom": pd.to_numeric(raw["DAYS ON MARKET"], errors="coerce"),
        "hoa": pd.to_numeric(raw["HOA/MONTH"], errors="coerce"),
        "lat": pd.to_numeric(raw["LATITUDE"], errors="coerce"),
        "lon": pd.to_numeric(raw["LONGITUDE"], errors="coerce"),
        "url": raw[url_col], "mls": raw.get("MLS#"), "source": raw.get("SOURCE"),
    })
    df["ppsf"] = df["price"] / df["sqft"].where(df["sqft"] > 0)
    return df.reset_index(drop=True)


def haversine_miles(lat1, lon1, lat2, lon2):
    lat1, lon1, lat2, lon2 = map(np.radians, (lat1, lon1, lat2, lon2))
    a = np.sin((lat2 - lat1) / 2) ** 2 + np.cos(lat1) * np.cos(lat2) * np.sin((lon2 - lon1) / 2) ** 2
    return 3958.8 * 2 * np.arcsin(np.sqrt(a))


# ------------------------------------------------------------------ subject

@dataclass
class Subject:
    address: str
    lat: float
    lon: float
    zip: str
    ptype: str | None = None          # key of TYPES
    beds: float | None = None
    baths: float | None = None
    sqft: float | None = None
    year_built: float | None = None
    lot_sqft: float | None = None
    last_sale_price: float | None = None
    last_sale_date: str | None = None
    url: str | None = None
    record_source: str = "none"


def _tokens(s: str) -> list[str]:
    return re.findall(r"[a-z0-9]+", s.lower())


def _unit(s: str) -> str | None:
    m = re.search(r"(?:#|\bapt\b|\bunit\b|\bste\b)\s*([a-z0-9-]+)", s.lower())
    return m.group(1) if m else None


STREET_WORDS = {"st", "street", "ave", "avenue", "rd", "road", "dr", "drive", "blvd", "ln", "lane",
                "ct", "court", "pl", "place", "way", "ter", "terrace", "cir", "circle", "pkwy", "hwy",
                "n", "s", "e", "w", "ne", "nw", "se", "sw", "north", "south", "east", "west"}


def find_subject_record(address: str, lat: float, lon: float) -> pd.Series | None:
    """Match the subject in a small Redfin search around its coordinates (public record + MLS)."""
    toks = _tokens(address.split(",")[0])
    if not toks or not toks[0].isdigit():
        return None
    number = toks[0]
    unit = _unit(address)
    street = [t for t in toks[1:] if t not in STREET_WORDS and t != unit][:1]

    def match(a: str) -> float:
        t = _tokens(str(a))
        if not t or t[0] != number or (street and street[0] not in t[1:5]):
            return -1
        u = _unit(str(a))
        return 2 if u == unit else (1 if unit is None and u is None else 0)

    # Census geocodes are interpolated along the block, so widen once if needed.
    for miles in (0.05, 0.12):
        df = redfin_csv(lat, lon, miles, sold_days=36500, uipt="1,2,3,4,5,6,7,8")
        if df.empty:
            continue
        df["m"] = df["address"].map(match)
        df = df[df["m"] >= 0]
        if len(df):
            df["filled"] = df[["beds", "baths", "sqft", "year_built"]].notna().sum(axis=1)
            return df.sort_values(["m", "filled", "sold_date"], ascending=False).iloc[0]
    return None


# ------------------------------------------------------------------ market analysis

def _chg(a, b):
    return None if a is None or b is None or pd.isna(a) or pd.isna(b) or b == 0 else float(a / b - 1)


def _at(s: pd.Series, when: pd.Timestamp):
    s = s[s.index <= when + pd.Timedelta(days=5)]
    return float(s.iloc[-1]) if len(s) else None


def zhvi_summary(s: pd.Series, years: int) -> dict:
    if s.empty:
        return {}
    last_d, last = s.index[-1], float(s.iloc[-1])
    out = {"as_of": last_d.date().isoformat(), "value": last,
           "chg_1m": _chg(last, _at(s, last_d - pd.DateOffset(months=1))),
           "chg_3m_annualized": None, "chg_1y": _chg(last, _at(s, last_d - pd.DateOffset(years=1))),
           "chg_3y": _chg(last, _at(s, last_d - pd.DateOffset(years=3)))}
    v3 = _at(s, last_d - pd.DateOffset(months=3))
    if v3:
        out["chg_3m_annualized"] = (last / v3) ** 4 - 1
    start = _at(s, last_d - pd.DateOffset(years=years))
    out[f"chg_{years}y"] = _chg(last, start)
    out["cagr"] = (last / start) ** (1 / years) - 1 if start else None
    window = s[s.index > last_d - pd.DateOffset(years=years)]
    out["peak"], out["peak_date"] = float(window.max()), window.idxmax().date().isoformat()
    out["trough"], out["trough_date"] = float(window.min()), window.idxmin().date().isoformat()
    out["vs_peak"] = _chg(last, out["peak"])
    out["annual"] = []
    for k in range(years, -1, -1):
        d = last_d - pd.DateOffset(years=k)
        v, p = _at(s, d), _at(s, d - pd.DateOffset(years=1))
        out["annual"].append({"date": d.date().isoformat(), "value": v, "yoy": _chg(v, p)})
    return out


def realtor_summary(df: pd.DataFrame, years: int) -> dict:
    if df.empty:
        return {}
    df = df.set_index("date")
    last_d = df.index[-1]
    cur = df.iloc[-1]
    prev = df[df.index <= last_d - pd.DateOffset(years=1) + pd.Timedelta(days=5)]
    prev = prev.iloc[-1] if len(prev) else None
    out = {"as_of": last_d.strftime("%Y-%m"), "latest": {}, "annual": []}
    for c in REALTOR_COLS:
        v = cur[c]
        p = prev[c] if prev is not None else None
        yoy = None
        if p is not None and pd.notna(p) and pd.notna(v):
            yoy = float(v - p) if c in ("price_reduced_share", "pending_ratio") else _chg(v, p)
        out["latest"][c] = {"value": None if pd.isna(v) else float(v), "yoy": yoy}
    for k in range(years, -1, -1):
        d = last_d - pd.DateOffset(years=k)
        r = df[df.index <= d + pd.Timedelta(days=5)]
        if len(r) and abs((r.index[-1] - d).days) < 40:
            r = r.iloc[-1]
            out["annual"].append({"month": r.name.strftime("%Y-%m"),
                                  **{c: (None if pd.isna(r[c]) else float(r[c])) for c in REALTOR_COLS}})
    return out


def redfin_tracker_summary(df: pd.DataFrame) -> dict:
    if df.empty:
        return {}
    cur = df.iloc[-1]
    prev = df[pd.to_datetime(df["PERIOD_END"]) <= pd.Timestamp(cur["PERIOD_END"]) - pd.DateOffset(years=1)
              + pd.Timedelta(days=5)]
    prev = prev.iloc[-1] if len(prev) else None
    keys = ["MEDIAN_SALE_PRICE", "MEDIAN_PPSF", "HOMES_SOLD", "AVG_SALE_TO_LIST", "SOLD_ABOVE_LIST",
            "MEDIAN_DOM", "MONTHS_OF_SUPPLY", "INVENTORY"]
    out = {"as_of": str(pd.Timestamp(cur["PERIOD_END"]).date()), "window_days": int(cur["PERIOD_DURATION"])}
    for k in keys:
        v, p = cur.get(k), (prev.get(k) if prev is not None else None)
        ratio = k in ("AVG_SALE_TO_LIST", "SOLD_ABOVE_LIST")
        yoy = None if p is None or pd.isna(p) or pd.isna(v) else (float(v - p) if ratio else _chg(v, p))
        out[k.lower()] = {"value": None if pd.isna(v) else float(v), "yoy": yoy}
    return out


def local_sales_history(subj: Subject, miles: float, uipt: str, years: int) -> tuple[pd.DataFrame, list[str]]:
    """Union of nested sold windows so the 350-row cap bites as late as possible."""
    notes, frames = [], []
    windows = sorted({365, 1095, 1825 if years >= 5 else 1095 if years >= 3 else 365})
    for w in windows:
        d = redfin_csv(subj.lat, subj.lon, miles, sold_days=w, uipt=uipt)
        if len(d) >= REDFIN_MAX_ROWS:
            notes.append(f"Sold-within-{w}-days search hit Redfin's {REDFIN_MAX_ROWS}-row cap; "
                         f"older periods are a sample, not every sale.")
        frames.append(d)
    df = pd.concat(frames).dropna(subset=["sold_date", "price"])
    df = df.drop_duplicates(subset=["url", "sold_date"])
    df = df[df["price"] > 10_000]
    return df.sort_values("sold_date").reset_index(drop=True), notes


def quarterly(df: pd.DataFrame) -> list[dict]:
    if df.empty:
        return []
    g = df.groupby(df["sold_date"].dt.to_period("Q"))
    return [{"quarter": str(q), "sales": int(len(x)), "median_price": float(x["price"].median()),
             "median_ppsf": None if x["ppsf"].isna().all() else float(x["ppsf"].median())}
            for q, x in g]


def same_model_sales(subj: Subject, sales: pd.DataFrame, zhvi: pd.Series) -> list[dict]:
    """Sales of near-identical homes (same beds, sqft ±5%, built ±5 yrs), e.g. the subject's tract model.

    Each price is also restated in today's dollars with the ZIP ZHVI index.
    """
    if sales.empty or not subj.sqft or subj.beds is None:
        return []
    d = sales[(sales["beds"] == subj.beds) & sales["sqft"].between(subj.sqft * 0.95, subj.sqft * 1.05)]
    if subj.year_built:
        d = d[d["year_built"].isna() | ((d["year_built"] - subj.year_built).abs() <= 5)]
    d = d[~d["address"].fillna("").str.lower().eq(subj.address.split(",")[0].lower().strip())]
    now = float(zhvi.iloc[-1]) if len(zhvi) else None
    out = []
    for _, x in d.sort_values("sold_date").iterrows():
        then = _at(zhvi, x["sold_date"]) if now else None
        out.append({"sold_date": x["sold_date"].date().isoformat(), "address": x["address"],
                    "price": float(x["price"]), "beds": x["beds"], "baths": x["baths"], "sqft": x["sqft"],
                    "year_built": x["year_built"], "url": x["url"],
                    "price_today": float(x["price"] * now / then) if then else None})
    return out


# ------------------------------------------------------------------ comps

@dataclass
class Criteria:
    radius: float = 0.5
    months: int = 6
    sqft_pct: float = 0.25
    beds_diff: int = 1
    year_built_diff: int | None = 30
    max_comps: int = 10
    min_comps: int = 5
    max_radius: float = 2.0


def filter_comps(subj: Subject, df: pd.DataFrame, c: Criteria) -> pd.DataFrame:
    df = df.dropna(subset=["sold_date", "price"]).copy()
    df["distance_mi"] = haversine_miles(subj.lat, subj.lon, df["lat"], df["lon"])
    df["months_ago"] = (TODAY - df["sold_date"]).dt.days / 30.44
    keep = (df["months_ago"] <= c.months) & (df["months_ago"] >= 0) & (df["price"] > 10_000)
    keep &= df["distance_mi"] <= c.radius
    keep &= ~df["address"].fillna("").str.lower().eq(subj.address.split(",")[0].lower().strip())
    if subj.ptype:
        keep &= df["property_type"].map(REDFIN_TYPE) == subj.ptype
    if subj.sqft:
        keep &= df["sqft"].isna() | df["sqft"].between(subj.sqft * (1 - c.sqft_pct), subj.sqft * (1 + c.sqft_pct))
    if subj.beds is not None:
        keep &= df["beds"].isna() | ((df["beds"] - subj.beds).abs() <= c.beds_diff)
    if subj.year_built and c.year_built_diff is not None:
        keep &= df["year_built"].isna() | ((df["year_built"] - subj.year_built).abs() <= c.year_built_diff)
    return df[keep.fillna(False)].copy()


def score(subj: Subject, df: pd.DataFrame, c: Criteria) -> pd.Series:
    """Similarity 0-100; each term is a penalty in [0, 1] times its weight."""
    pen = 25 * (df["distance_mi"] / c.radius).clip(0, 1)
    pen += 20 * (df["months_ago"] / c.months).clip(0, 1)
    if subj.sqft:
        pen += 25 * ((df["sqft"] / subj.sqft - 1).abs() / c.sqft_pct).clip(0, 1).fillna(0.6)
    if subj.beds is not None:
        pen += 10 * ((df["beds"] - subj.beds).abs() / 2).clip(0, 1).fillna(0.5)
    if subj.baths is not None:
        pen += 10 * ((df["baths"] - subj.baths).abs() / 2).clip(0, 1).fillna(0.5)
    if subj.year_built:
        pen += 10 * ((df["year_built"] - subj.year_built).abs() / 30).clip(0, 1).fillna(0.5)
    return (100 - pen).round(1)


def weighted_quantile(values, weights, q):
    values, weights = np.asarray(values, float), np.asarray(weights, float)
    order = np.argsort(values)
    values, weights = values[order], weights[order]
    cum = np.cumsum(weights) - 0.5 * weights
    return float(np.interp(q * weights.sum(), cum, values))


def find_comps(subj: Subject, c: Criteria, uipt: str, monthly_growth: float) -> dict:
    notes = []
    sold_days = next(w for w in (30, 90, 180, 365, 730, 1095, 1825) if w >= c.months * 30.44 - 1)
    radius = c.radius
    while True:
        raw = redfin_csv(subj.lat, subj.lon, radius, sold_days=sold_days, uipt=uipt)
        if len(raw) >= REDFIN_MAX_ROWS and radius > 0.15:
            radius = round(radius * 0.6, 2)
            notes.append(f"Search hit the {REDFIN_MAX_ROWS}-row cap; shrank radius to {radius} mi.")
            continue
        c.radius = radius
        pool = filter_comps(subj, raw, c)
        if len(pool) >= c.min_comps or radius >= c.max_radius:
            break
        radius = min(c.max_radius, radius * 2)
        notes.append(f"Only {len(pool)} comps found; widened radius to {radius} mi.")
    if pool.empty:
        return {"radius_mi": radius, "candidates": len(raw), "comps": [], "notes": notes, "pool": pool}
    pool["similarity"] = score(subj, pool, c)
    pool["time_adj"] = (1 + monthly_growth) ** pool["months_ago"]
    size_adj = (subj.sqft / pool["sqft"]) ** SIZE_ELASTICITY if subj.sqft else 1.0
    pool["adj_price"] = pool["price"] * pool["time_adj"] * size_adj
    comps = pool.sort_values("similarity", ascending=False).head(c.max_comps).reset_index(drop=True)
    est = {}
    v = comps.dropna(subset=["adj_price"])
    if not subj.sqft and subj.beds is None:
        notes.append("No subject sqft/beds, so comps can't be size-matched; value estimate skipped. "
                     "Pass --sqft and --beds.")
    elif len(v):
        if not subj.sqft:
            notes.append("Subject sqft unknown: comps matched on beds only and not size-adjusted. Pass --sqft.")
        w = v["similarity"].clip(lower=1)
        est = {"estimate": weighted_quantile(v["adj_price"], w, 0.5),
               "low": weighted_quantile(v["adj_price"], w, 0.25),
               "high": weighted_quantile(v["adj_price"], w, 0.75), "n": len(v)}
        if subj.sqft:
            est["implied_ppsf"] = est["estimate"] / subj.sqft
    return {"radius_mi": radius, "sold_within_days": sold_days, "candidates": int(len(raw)),
            "matched": int(len(pool)), "comps": comps, "estimate": est, "notes": notes, "pool": pool}


# ------------------------------------------------------------------ report

def money(x):
    return "—" if x is None or pd.isna(x) else f"${x:,.0f}"


def pct(x, signed=True):
    return "—" if x is None or pd.isna(x) else (f"{x:+.1%}" if signed else f"{x:.1%}")


def num(x, fmt="{:,.0f}"):
    return "—" if x is None or pd.isna(x) else fmt.format(x)


def render(r: dict) -> str:
    s, L, yrs = r["subject"], [], r["years"]
    L.append(f"# Property research: {s['address']}")
    L.append(f"_Generated {r['generated']} · ZIP {s['zip']} · {r['geocode']['matched']}_\n")
    L.append("## Subject")
    L.append(f"- Type: {s['ptype'] or 'unknown'} · {num(s['beds'], '{:g}')} bd / {num(s['baths'], '{:g}')} ba · "
             f"{num(s['sqft'])} sqft · built {num(s['year_built'], '{:.0f}')} · lot {num(s['lot_sqft'])} sqft")
    L.append(f"- Last sale: {money(s['last_sale_price'])} on {s['last_sale_date'] or '—'}  "
             f"(record: {s['record_source']}{', ' + s['url'] if s['url'] else ''})")

    for key, label in (("zhvi_type", "type-specific"), ("zhvi_all", "all homes")):
        z = r["market"].get(key)
        if not z:
            continue
        L.append(f"\n## {r['years']}-year value trend — Zillow ZHVI, ZIP {s['zip']} ({label}, as of {z['as_of']})")
        L.append(f"- Typical value **{money(z['value'])}**; 1-yr {pct(z['chg_1y'])}, 3-yr {pct(z['chg_3y'])}, "
                 f"{yrs}-yr {pct(z.get(f'chg_{yrs}y'))} "
                 f"(CAGR {pct(z['cagr'])}); last 3 mo annualized {pct(z['chg_3m_annualized'])}")
        L.append(f"- {r['years']}-yr peak {money(z['peak'])} ({z['peak_date']}), now {pct(z['vs_peak'])} vs peak; "
                 f"low {money(z['trough'])} ({z['trough_date']})")
        L.append("\n| Date | ZHVI | YoY |\n|---|---:|---:|")
        L += [f"| {a['date'][:7]} | {money(a['value'])} | {pct(a['yoy'])} |" for a in z["annual"]]

    rt = r["market"].get("realtor")
    if rt:
        lt = rt["latest"]
        L.append(f"\n## Listing market — Realtor.com, ZIP {s['zip']} (as of {rt['as_of']})")
        L.append("| Metric | Latest | YoY |\n|---|---:|---:|")
        rows = [("Median list price", "median_listing_price", money, pct),
                ("Median list $/sqft", "median_listing_price_per_square_foot", money, pct),
                ("Active listings", "active_listing_count", num, pct),
                ("New listings", "new_listing_count", num, pct),
                ("Pending listings", "pending_listing_count", num, pct),
                ("Median days on market", "median_days_on_market", num, pct),
                ("Share with price cut", "price_reduced_share", lambda v: pct(v, False), lambda v: num(v * 100 if v is not None else None, "{:+.1f} pts")),
                ("Pending ratio", "pending_ratio", lambda v: num(v, "{:.2f}"), lambda v: num(v, "{:+.2f}"))]
        for name, k, f1, f2 in rows:
            L.append(f"| {name} | {f1(lt[k]['value'])} | {f2(lt[k]['yoy'])} |")
        L.append(f"\nSame month, last {r['years']} years:\n")
        L.append("| Month | List price | Active | New | DOM | Price-cut share |\n|---|---:|---:|---:|---:|---:|")
        L += [f"| {a['month']} | {money(a['median_listing_price'])} | {num(a['active_listing_count'])} | "
              f"{num(a['new_listing_count'])} | {num(a['median_days_on_market'])} | {pct(a['price_reduced_share'], False)} |"
              for a in rt["annual"]]

    rf = r["market"].get("redfin_tracker")
    if rf:
        L.append(f"\n## Sold-market stats — Redfin Data Center, ZIP {s['zip']} "
                 f"({rf['window_days']}-day window ending {rf['as_of']} — lagging)")
        L.append(f"- Median sale {money(rf['median_sale_price']['value'])} ({pct(rf['median_sale_price']['yoy'])} YoY), "
                 f"{money(rf['median_ppsf']['value'])}/sqft; {num(rf['homes_sold']['value'])} sold "
                 f"({pct(rf['homes_sold']['yoy'])}); sale-to-list {pct(rf['avg_sale_to_list']['value'], False)}; "
                 f"{pct(rf['sold_above_list']['value'], False)} sold above list; median DOM {num(rf['median_dom']['value'])}")

    ls = r.get("local_sales")
    if ls:
        L.append(f"\n## Nearby sold homes by quarter — Redfin MLS/public record, {ls['radius_mi']} mi, same type")
        L.append("| Quarter | Sales | Median price | Median $/sqft |\n|---|---:|---:|---:|")
        L += [f"| {q['quarter']} | {q['sales']} | {money(q['median_price'])} | {money(q['median_ppsf'])} |"
              for q in ls["quarterly"]]
        if ls.get("trend"):
            t = ls["trend"]
            L.append(f"\nLog-linear fit of $/sqft over {t['n']} sales: {pct(t['annual_growth'])}/yr (R² {t['r2']:.2f}).")

    sm = r.get("same_model") or []
    if sm:
        L.append(f"\n## Same-model sales — {r['years']} yrs, same beds, sqft ±5%, built ±5 yrs ({len(sm)} sales)")
        L.append("Often the strongest evidence in tract neighborhoods. \"Today\" restates each price with the "
                 "ZIP ZHVI index.\n")
        L.append("| Sold | Address | Price | Bd/Ba | Sqft | Built | Today |\n|---|---|---:|---|---:|---:|---:|")
        L += [f"| {x['sold_date']} | [{x['address']}]({x['url']}) | {money(x['price'])} | "
              f"{num(x['beds'], '{:g}')}/{num(x['baths'], '{:g}')} | {num(x['sqft'])} | "
              f"{num(x['year_built'], '{:.0f}')} | {money(x['price_today'])} |" for x in sm]
        recent = [x["price_today"] for x in sm if x["price_today"]
                  and pd.Timestamp(x["sold_date"]) >= pd.Timestamp(r["generated"]) - pd.DateOffset(months=12)]
        if recent:
            L.append(f"\nLast 12 months: {len(recent)} sales, median {money(float(np.median(recent)))} in today's terms.")

    cp = r.get("comps")
    if cp:
        L.append(f"\n## Comparable sales — last {r['criteria']['months']} months, within {cp['radius_mi']} mi")
        L.append(f"{cp['candidates']} sales in the search area, {cp.get('matched', 0)} matched the criteria "
                 f"(same type, ±{r['criteria']['sqft_pct']:.0%} sqft, ±{r['criteria']['beds_diff']} bed). "
                 f"Time adjustment {pct(r['time_adj_annual'])}/yr from {r['time_adj_source']}.\n")
        if cp["comps"]:
            L.append("| # | Sold | Address | Price | Bd/Ba | Sqft | $/sqft | Built | Dist mi | Score | Adj. price |")
            L.append("|---|---|---|---:|---|---:|---:|---:|---:|---:|---:|")
            for i, x in enumerate(cp["comps"], 1):
                L.append(f"| {i} | {x['sold_date'][:10]} | [{x['address']}]({x['url']}) | {money(x['price'])} | "
                         f"{num(x['beds'], '{:g}')}/{num(x['baths'], '{:g}')} | {num(x['sqft'])} | {money(x['ppsf'])} | "
                         f"{num(x['year_built'], '{:.0f}')} | {x['distance_mi']:.2f} | "
                         f"{x['similarity']:.0f} | {money(x['adj_price'])} |")
        e = cp.get("estimate") or {}
        if e:
            L.append(f"\n**Comp-based value: {money(e['estimate'])}** (weighted IQR {money(e['low'])} – {money(e['high'])}, "
                     f"{e['n']} comps{', ' + money(e['implied_ppsf']) + '/sqft' if e.get('implied_ppsf') else ''}).")
            if e["n"] < 4:
                L.append("Few comps — low confidence.")
    act = r.get("active")
    if act:
        L.append(f"\n## Active competition — {act['count']} same-type listings within {act['radius_mi']} mi")
        if act["count"]:
            L.append(f"Median ask {money(act['median_price'])}, {money(act['median_ppsf'])}/sqft, median DOM {num(act['median_dom'])}.")
    L.append("\n## Data notes")
    L += [f"- {n}" for n in r["notes"]]
    return "\n".join(L) + "\n"


def jsonable(o):
    if isinstance(o, pd.DataFrame):
        return json.loads(o.to_json(orient="records", date_format="iso"))
    if isinstance(o, (pd.Timestamp, datetime)):
        return o.isoformat()
    if isinstance(o, (np.integer,)):
        return int(o)
    if isinstance(o, (np.floating,)):
        return None if np.isnan(o) else float(o)
    raise TypeError(type(o))


def report(a) -> None:
    years = a.years
    notes: list[str] = []
    geo = geocode(a.address)
    zip_code = a.zip or geo["zip"]
    if not zip_code:
        sys.exit("Could not determine ZIP; pass --zip.")
    subj = Subject(address=a.address, lat=geo["lat"], lon=geo["lon"], zip=zip_code)

    redfin_ok = True
    try:
        rec = find_subject_record(a.address, subj.lat, subj.lon)
    except RedfinBlocked as e:
        rec, redfin_ok = None, False
        notes.append(f"Redfin blocked the request ({e}). Comps/local sales unavailable — see SKILL.md fallbacks.")
    if rec is not None:
        subj.ptype = REDFIN_TYPE.get(rec["property_type"])
        for f in ("beds", "baths", "sqft", "year_built", "lot_sqft"):
            v = rec[f]
            setattr(subj, f, None if pd.isna(v) else float(v))
        if pd.notna(rec["sold_date"]):
            subj.last_sale_price = None if pd.isna(rec["price"]) else float(rec["price"])
            subj.last_sale_date = rec["sold_date"].date().isoformat()
        subj.url, subj.record_source = rec["url"], f"Redfin ({rec['address']})"
        if rec["m"] < 1 or _tokens(rec["address"]) != _tokens(a.address.split(",")[0]):
            notes.append(f"Subject matched Redfin record '{rec['address']}' (not an exact match); confirm "
                         "it is the right property/unit or pass specs explicitly.")
        if pd.isna(rec["sqft"]) and pd.isna(rec["beds"]):
            notes.append("Redfin record has no beds/sqft (often a multi-unit building parcel); pass the unit "
                         "in the address or --beds/--baths/--sqft/--type.")
    elif redfin_ok:
        notes.append("Subject not found in Redfin records; specs come from command-line flags only.")
    for f in ("beds", "baths", "sqft", "year_built"):
        if getattr(a, f) is not None:
            setattr(subj, f, float(getattr(a, f)))
    if a.type:
        subj.ptype = a.type
    if not subj.ptype:
        subj.ptype = "sfr"
        notes.append("Property type unknown; assumed single-family. Pass --type to override.")
    uipt, zseries, rtype = TYPES[subj.ptype]

    market = {}
    zs_type, m = zillow_series(zip_code, zseries, a.refresh)
    zs_all = zs_type
    if zseries != "sfrcondo":
        zs_all, _ = zillow_series(zip_code, "sfrcondo", a.refresh)
    if zs_type.empty and not zs_all.empty:
        zs_type = zs_all
    if not zs_type.empty:
        market["zhvi_type"] = zhvi_summary(zs_type, years)
        if zseries != "sfrcondo" and not zs_all.empty:
            market["zhvi_all"] = zhvi_summary(zs_all, years)
        notes.append(f"Zillow ZHVI ({zseries}) file last modified {m.get('last_modified')}; ZHVI is a smoothed, "
                     "seasonally adjusted typical value (35th–65th percentile homes), not a median sale price.")
    else:
        notes.append(f"Zillow has no ZHVI series for ZIP {zip_code}.")

    try:
        rdf, m = realtor_frame(zip_code, a.refresh)
        if not rdf.empty:
            market["realtor"] = realtor_summary(rdf, years)
            notes.append(f"Realtor.com ZIP inventory file last modified {m.get('last_modified')} (all property types).")
    except Exception as e:  # big download; don't lose the rest of the report
        notes.append(f"Realtor.com data unavailable: {e}")

    tracker = redfin_tracker(zip_code, rtype)
    if tracker.empty and rtype != "All Residential":
        tracker = redfin_tracker(zip_code, "All Residential")
    if not tracker.empty:
        market["redfin_tracker"] = redfin_tracker_summary(tracker)
        notes.append("Redfin Data Center tracker comes from REDFIN_TRACKER_PARQUET; Redfin's public file "
                     "has not been refreshed since early June 2026, so it lags.")

    # Time adjustment: ZIP-level ZHVI growth over 12 months.
    g, gsrc = 0.0, "none"
    if len(zs_type) >= 13:
        g = (zs_type.iloc[-1] / zs_type.iloc[-13]) ** (1 / 12) - 1
        gsrc = f"ZIP ZHVI ({zseries}) last 12 months"

    result = {"generated": TODAY.date().isoformat(), "years": years, "geocode": geo,
              "market": market, "notes": notes, "time_adj_annual": (1 + g) ** 12 - 1, "time_adj_source": gsrc}
    crit = Criteria(radius=a.radius, months=a.months, sqft_pct=a.sqft_pct, beds_diff=a.beds_diff,
                    max_comps=a.max_comps)
    result["criteria"] = asdict(crit)

    sales_hist = pd.DataFrame()
    comps_df = pd.DataFrame()
    if redfin_ok:
        try:
            sales_hist, n2 = local_sales_history(subj, a.trend_radius, uipt, years)
            notes += n2
            ls = {"radius_mi": a.trend_radius, "n": len(sales_hist), "quarterly": quarterly(sales_hist)}
            d = sales_hist.dropna(subset=["ppsf"])
            if len(d) >= 12:
                x = (d["sold_date"] - TODAY).dt.days.to_numpy() / 365.25
                y = np.log(d["ppsf"].to_numpy())
                slope, icpt = np.polyfit(x, y, 1)
                pred = slope * x + icpt
                r2 = 1 - ((y - pred) ** 2).sum() / max(((y - y.mean()) ** 2).sum(), 1e-12)
                ls["trend"] = {"n": len(d), "annual_growth": float(np.expm1(slope)), "r2": float(r2)}
            result["local_sales"] = ls
            result["same_model"] = same_model_sales(subj, sales_hist, zs_type)

            cp = find_comps(subj, crit, uipt, g)
            notes += cp.pop("notes")
            comps_df = cp.pop("pool")
            result["comps"] = cp
            result["criteria"] = asdict(crit)

            act = redfin_csv(subj.lat, subj.lon, crit.radius, sold_days=None, uipt=uipt)
            act = act[act["status"].isin(["Active", "Coming Soon"])]
            result["active"] = {"radius_mi": crit.radius, "count": int(len(act)),
                                "median_price": float(act["price"].median()) if len(act) else None,
                                "median_ppsf": float(act["ppsf"].median()) if act["ppsf"].notna().any() else None,
                                "median_dom": float(act["dom"].median()) if act["dom"].notna().any() else None,
                                "listings": act}
            notes.append("Redfin sold data is MLS-fed (typically days old) plus public records; some MLSs "
                         "exclude listings from the download, and non-disclosure states (e.g. TX, UT) may omit prices.")
        except RedfinBlocked as e:
            notes.append(f"Redfin blocked the request mid-run ({e}).")
    result["subject"] = asdict(subj)

    out = Path(a.out).expanduser() / f"{re.sub(r'[^a-z0-9]+', '-', a.address.lower()).strip('-')[:60]}-{TODAY:%Y%m%d}"
    out.mkdir(parents=True, exist_ok=True)
    md = render(json.loads(json.dumps(result, default=jsonable)))
    (out / "report.md").write_text(md)
    (out / "data.json").write_text(json.dumps(result, default=jsonable, indent=1))
    if not sales_hist.empty:
        sales_hist.to_csv(out / "local_sales.csv", index=False)
    if not comps_df.empty:
        comps_df.sort_values("similarity", ascending=False).to_csv(out / "comps_all_matched.csv", index=False)
    series = pd.DataFrame({"zhvi_type": zs_type, "zhvi_all": zs_all})
    series[series.index > TODAY - pd.DateOffset(years=years + 1)].rename_axis("date").to_csv(out / "zhvi_monthly.csv")
    print(md)
    print(f"\nFiles written to {out}", file=sys.stderr)


def main(argv=None):
    p = argparse.ArgumentParser(prog="property_research")
    sub = p.add_subparsers(dest="cmd", required=True)
    r = sub.add_parser("report", help="market trend + comps for an address")
    r.add_argument("address")
    r.add_argument("--type", choices=list(TYPES), help="override property type")
    r.add_argument("--beds", type=float)
    r.add_argument("--baths", type=float)
    r.add_argument("--sqft", type=float)
    r.add_argument("--year-built", dest="year_built", type=float)
    r.add_argument("--zip", help="override ZIP if geocoding picks the wrong one")
    r.add_argument("--years", type=int, default=5, help="trend lookback (default 5)")
    r.add_argument("--months", type=int, default=6, help="comp sale window in months (default 6)")
    r.add_argument("--radius", type=float, default=0.5, help="starting comp radius in miles (auto-widens)")
    r.add_argument("--trend-radius", dest="trend_radius", type=float, default=0.5,
                   help="radius for the local quarterly sales trend")
    r.add_argument("--sqft-pct", dest="sqft_pct", type=float, default=0.25)
    r.add_argument("--beds-diff", dest="beds_diff", type=int, default=1)
    r.add_argument("--max-comps", dest="max_comps", type=int, default=10)
    r.add_argument("--out", default="./property-reports")
    r.add_argument("--refresh", action="store_true", help="force re-download of market datasets")
    sub.add_parser("refresh", help="re-download Zillow and Realtor.com datasets")
    a = p.parse_args(argv)
    if a.cmd == "refresh":
        for t in ("sfrcondo", "sfr", "condo"):
            ensure_dataset(ZILLOW_URL.format(t=t), CACHE / f"zillow_zhvi_{t}.parquet", _build_zillow, True)
        ensure_dataset(REALTOR_URL, CACHE / "realtor_zip_history.parquet", _build_realtor, True)
        print(f"Datasets refreshed in {CACHE}")
    else:
        report(a)


if __name__ == "__main__":
    main()
