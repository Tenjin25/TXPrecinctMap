from __future__ import annotations

import argparse
import json
import re
import zipfile
from collections import Counter, defaultdict
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Dict, Iterable, List, Optional, Set, Tuple

import geopandas as gpd
import pandas as pd
import pyogrio

try:
    from build_contests_from_tx_shapefiles import (
        aggregate_contest_rows_from_shapefile,
        load_existing_candidate_tokens,
    )
except Exception:
    aggregate_contest_rows_from_shapefile = None
    load_existing_candidate_tokens = None


def normalize_text(value: str) -> str:
    return re.sub(r"\s+", " ", re.sub(r"[^a-z0-9 .\-]", "", str(value).lower())).strip().upper()


PRECINCT_LABEL_TOKENS: Set[str] = {
    "PCT",
    "PCT.",
    "PRECINCT",
    "PRECINCT.",
    "VTD",
    "DIST",
    "DISTRICT",
    "NO",
    "NO.",
    "NUMBER",
    "#",
}


def county_key_variants(raw_county: str) -> Set[str]:
    county = normalize_text(raw_county)
    if not county:
        return set()
    out = {county}
    squashed = county.replace(" ", "")
    if squashed:
        out.add(squashed)
    return out


def normalize_district_id(value: object) -> str:
    s = str(value or "").strip()
    if not s:
        return ""
    m = re.search(r"\d+", s)
    if not m:
        return s
    return str(int(m.group(0)))


def parse_votes(value: object) -> float:
    if value is None:
        return 0.0
    s = str(value).strip().replace(",", "")
    if not s or s.lower() == "nan":
        return 0.0
    try:
        return float(s)
    except ValueError:
        return 0.0


def is_non_geographic_precinct(code: str) -> bool:
    c = (code or "").strip().upper()
    if not c:
        return True
    if c in {"EV", "ABSEN", "PROVI", "TRANS"}:
        return True
    if c.startswith(("EV", "OS", "MAIL", "ABS", "PROV", "CURB")):
        return True
    if any(x in c for x in ("ABSENTEE", "PROVISIONAL", "ONE STOP", "VOTE CENTER", "EARLY")):
        return True
    return False


def extract_precinct_code(raw_precinct: str) -> str:
    p = str(raw_precinct or "").strip().upper()
    if not p:
        return ""
    if is_non_geographic_precinct(p):
        return ""

    # Keep only the first segment in composite strings, then parse usable code tokens.
    p = p.split("/", 1)[0]
    p = p.split("_", 1)[0]
    p = re.sub(r"\s+", " ", p).strip()

    # Strip leading labels like "PCT", "PRECINCT", "VTD", and optional NO/# marker.
    p = re.sub(
        r"^(?:PCT\.?|PRECINCT\.?|VTD|DIST(?:RICT)?)\s*(?:NO\.?|NUMBER|#)?\s*",
        "",
        p,
        flags=re.I,
    ).strip()
    if not p:
        return ""

    tokens = re.split(r"\s+", p)
    for tok in tokens:
        token = re.sub(r"[^A-Z0-9.\-]", "", tok.strip().upper())
        if not token or token in PRECINCT_LABEL_TOKENS:
            continue
        if is_non_geographic_precinct(token):
            return ""
        if re.fullmatch(r"[0-9]{1,6}[A-Z]{0,2}", token):
            return token
        if re.fullmatch(r"[0-9]{1,6}[.\-][0-9]{1,4}[A-Z]{0,2}", token):
            return token
        if re.fullmatch(r"[A-Z]{1,2}[0-9]{1,6}", token):
            return token

    # Fallback: pull the first plausible numeric-ish run from the remaining string.
    m = re.search(r"([0-9]{1,6}(?:[.\-][0-9]{1,4})?[A-Z]{0,2})", p)
    if m:
        return m.group(1)
    return ""


def code_variants(code: str) -> Set[str]:
    token = re.sub(r"[^A-Z0-9.\-]", "", str(code or "").strip().upper())
    if not token:
        return set()
    out: Set[str] = {token}

    m_num = re.fullmatch(r"0*([0-9]+)", token)
    if m_num:
        n = str(int(m_num.group(1)))
        out.update({n, n.zfill(2), n.zfill(3), n.zfill(4), n.zfill(5), n.zfill(6)})

    m_alpha = re.fullmatch(r"0*([0-9]+)([A-Z]{1,2})", token)
    if m_alpha:
        n = str(int(m_alpha.group(1)))
        suf = m_alpha.group(2)
        for base in (n, n.zfill(2), n.zfill(3), n.zfill(4), n.zfill(5), n.zfill(6)):
            out.add(f"{base}{suf}")

    m_dec = re.fullmatch(r"0*([0-9]{1,3})\.([0-9]{1,3})", token)
    if m_dec:
        a = str(int(m_dec.group(1))).zfill(2)
        b = str(int(m_dec.group(2)))
        out.add(f"{a}.{b}")

    m_split = re.fullmatch(r"0*([0-9]{1,6})[\-\.]0*([0-9]{1,4})([A-Z]{0,2})", token)
    if m_split:
        a = str(int(m_split.group(1)))
        b = str(int(m_split.group(2)))
        suf = m_split.group(3)
        joined = f"{a}{b}{suf}"
        out.update({joined, f"{a}.{b}{suf}", f"{a}-{b}{suf}"})
        out.add(f"{a}{suf}")
        if joined.isdigit():
            n = str(int(joined))
            out.update({n, n.zfill(4), n.zfill(5), n.zfill(6)})

    # Variant with punctuation removed.
    compact = re.sub(r"[.\-]", "", token)
    if compact and compact != token:
        out.add(compact)
        if compact.isdigit():
            n = str(int(compact))
            out.update({n, n.zfill(2), n.zfill(3), n.zfill(4), n.zfill(5), n.zfill(6)})

    return {v for v in out if v}


OFFICE_PATTERNS: List[Tuple[re.Pattern[str], str]] = [
    (re.compile(r"PRESIDENT", re.I), "president"),
    (re.compile(r"(U\.?\s*S\.?|US|UNITED STATES)\s*SENATE", re.I), "us_senate"),
    (re.compile(r"LIEUTENANT\s+GOVERNOR|LT\.?\s*GOVERNOR", re.I), "lieutenant_governor"),
    (re.compile(r"ATTORNEY\s+GENERAL", re.I), "attorney_general"),
    (re.compile(r"GENERAL\s+LAND\s+OFFICE|LAND\s+OFFICE", re.I), "land_commissioner"),
    (re.compile(r"AGRICULTURE\s+COMMISSIONER|COMMISSIONER\s+OF\s+AGRICULTURE", re.I), "agriculture_commissioner"),
    (re.compile(r"RAILROAD\s+COMMISSIONER", re.I), "railroad_commissioner"),
    (re.compile(r"COMPTROLLER", re.I), "comptroller"),
    (re.compile(r"\bGOVERNOR\b", re.I), "governor"),
]


def parse_place_number(office_upper: str) -> Optional[int]:
    text = str(office_upper or "").upper()
    if not text:
        return None

    patterns = [
        r"\bPLACE\s*(?:NO\.?|NUMBER)?\s*([0-9]{1,2})\b",
        r"\bPL(?:ACE)?\.?\s*([0-9]{1,2})\b",
        r"\bP[LI]\.?\s*([0-9]{1,2})\b",
        r"\bP([0-9]{1,2})\b",
    ]
    for pat in patterns:
        m = re.search(pat, text, flags=re.I)
        if m:
            try:
                return int(m.group(1))
            except ValueError:
                return None

    m_tail = re.search(r"CRIMINAL\s+APPEALS[^0-9]{0,16}([0-9]{1,2})\b", text, flags=re.I)
    if m_tail:
        try:
            return int(m_tail.group(1))
        except ValueError:
            return None
    return None


def map_supreme_court_office(office_upper: str) -> Optional[str]:
    if not re.search(r"SUPREME\s+(?:COURT|CRT|CT)\b", office_upper, flags=re.I):
        return None
    place = parse_place_number(office_upper)
    if place is None or not (1 <= place <= 9):
        return None
    suffix = "_unexpired" if "UNEXPIRED" in office_upper else ""
    return f"supreme_court_place_{place}{suffix}"


def map_criminal_appeals_office(office_upper: str) -> Optional[str]:
    if not re.search(r"(COURT\s+OF\s+CRIMINAL\s+APPEALS|CRIMINAL\s+APPEALS)", office_upper, flags=re.I):
        return None
    if re.search(r"PRESIDING\s+JUDGE", office_upper, flags=re.I):
        return "court_of_criminal_appeals_presiding_judge"
    place = parse_place_number(office_upper)
    if place is None or not (1 <= place <= 9):
        return None
    return f"court_of_criminal_appeals_place_{place}"


def map_office_to_contest(office: str) -> Optional[str]:
    o = str(office or "").strip()
    if not o:
        return None

    o_upper = o.upper()
    criminal_appeals = map_criminal_appeals_office(o_upper)
    if criminal_appeals:
        return criminal_appeals

    supreme_court = map_supreme_court_office(o_upper)
    if supreme_court:
        return supreme_court

    for pattern, contest in OFFICE_PATTERNS:
        if contest == "governor" and re.search(r"LIEUTENANT|LT\.?\s*GOVERNOR", o, flags=re.I):
            continue
        if pattern.search(o):
            return contest
    return None


def choose_general_file_set(openelections_root: Path) -> Dict[int, List[Path]]:
    groups: Dict[Tuple[int, str], List[Path]] = defaultdict(list)
    pattern = re.compile(r"^(\d{8})__tx__general__.+__precinct\.csv$", re.I)

    for p in openelections_root.rglob("*__precinct.csv"):
        name = p.name
        if "__special__" in name.lower():
            continue
        m = pattern.match(name)
        if not m:
            continue
        date = m.group(1)
        year = int(date[:4])
        groups[(year, date)].append(p)

    out: Dict[int, List[Path]] = {}
    for year in sorted({k[0] for k in groups.keys()}):
        date_candidates = [(d, files) for (y, d), files in groups.items() if y == year]
        if not date_candidates:
            continue
        best_date, best_files = max(date_candidates, key=lambda x: len(x[1]))
        print(f"[files] {year}: selected {best_date} with {len(best_files)} county precinct files")
        out[year] = sorted(best_files)
    return out


def read_precinct_csv_standardized(path: Path) -> Optional[pd.DataFrame]:
    try:
        header = pd.read_csv(path, nrows=0, dtype=str, low_memory=False)
    except Exception as err:
        print(f"[warn] failed to read header {path.name}: {err}")
        return None

    colmap = {str(c).strip().lower(): c for c in header.columns}

    def pick(*candidates: str) -> Optional[str]:
        for c in candidates:
            if c in colmap:
                return colmap[c]
        return None

    county_col = pick("county")
    precinct_col = pick("precinct", "pct", "vtd", "vtdst", "precinct_name", "election district")
    office_col = pick("office")
    party_col = pick("party")
    candidate_col = pick("candidate")
    votes_col = pick("votes")

    if not all([county_col, precinct_col, office_col, party_col, candidate_col, votes_col]):
        print(f"[skip] {path.name}: no usable precinct schema")
        return None

    usecols = [county_col, precinct_col, office_col, party_col, candidate_col, votes_col]
    try:
        df = pd.read_csv(path, usecols=usecols, dtype=str, keep_default_na=False, low_memory=False)
    except Exception:
        # Fallback for odd parser cases.
        df = pd.read_csv(path, usecols=usecols, dtype=str, keep_default_na=False, low_memory=False, engine="python")

    df = df.rename(
        columns={
            county_col: "county",
            precinct_col: "precinct",
            office_col: "office",
            party_col: "party",
            candidate_col: "candidate",
            votes_col: "votes",
        }
    )
    return df


@dataclass
class PrecinctAggNode:
    dem: float = 0.0
    rep: float = 0.0
    other: float = 0.0
    dem_candidate_votes: Counter = None
    rep_candidate_votes: Counter = None

    def __post_init__(self) -> None:
        if self.dem_candidate_votes is None:
            self.dem_candidate_votes = Counter()
        if self.rep_candidate_votes is None:
            self.rep_candidate_votes = Counter()


def build_precinct_contest_rows(files_for_year: Iterable[Path], year: int) -> Dict[str, List[dict]]:
    buckets: Dict[str, Dict[str, PrecinctAggNode]] = defaultdict(dict)

    for f in files_for_year:
        df = read_precinct_csv_standardized(f)
        if df is None or df.empty:
            continue

        for row in df.itertuples(index=False):
            contest = map_office_to_contest(row.office)
            if not contest:
                continue

            county = normalize_text(str(row.county))
            code = extract_precinct_code(str(row.precinct))
            if not county or not code:
                continue

            key = normalize_text(f"{county} - {code}")
            if key not in buckets[contest]:
                buckets[contest][key] = PrecinctAggNode()
            node = buckets[contest][key]

            votes = parse_votes(row.votes)
            party = str(row.party or "").strip().upper()
            cand = str(row.candidate or "").strip()

            if party.startswith("DEM"):
                node.dem += votes
                if cand:
                    node.dem_candidate_votes[cand] += votes
            elif party.startswith("REP"):
                node.rep += votes
                if cand:
                    node.rep_candidate_votes[cand] += votes
            else:
                node.other += votes

    out: Dict[str, List[dict]] = {}
    for contest, precinct_map in buckets.items():
        rows: List[dict] = []
        for precinct_norm, node in sorted(precinct_map.items(), key=lambda x: x[0]):
            total = node.dem + node.rep + node.other
            if total <= 0:
                continue
            margin_votes = node.rep - node.dem
            margin_pct = (margin_votes / total) * 100.0 if total else 0.0
            winner = "REP" if node.rep > node.dem else ("DEM" if node.dem > node.rep else "TIE")
            dem_cand = node.dem_candidate_votes.most_common(1)[0][0] if node.dem_candidate_votes else ""
            rep_cand = node.rep_candidate_votes.most_common(1)[0][0] if node.rep_candidate_votes else ""

            rows.append(
                {
                    "county": precinct_norm,
                    "dem_votes": round(node.dem),
                    "rep_votes": round(node.rep),
                    "other_votes": round(node.other),
                    "total_votes": round(total),
                    "dem_candidate": dem_cand,
                    "rep_candidate": rep_cand,
                    "margin": round(margin_votes),
                    "margin_pct": margin_pct,
                    "winner": winner,
                    "color": "",
                }
            )
        out[contest] = rows
        print(f"[contests] {year} {contest}: {len(rows)} precinct rows")

    return out


def load_county_lookup(counties_geojson: Path) -> Dict[str, str]:
    gdf = gpd.read_file(counties_geojson)
    mapping: Dict[str, str] = {}
    for _, row in gdf.iterrows():
        countyfp = str(row.get("COUNTYFP20") or row.get("COUNTYFP") or "").zfill(3)
        name = str(row.get("NAME20") or row.get("NAME") or "").strip()
        if countyfp and name:
            mapping[countyfp] = name
    return mapping


def enrich_vtd(vtd_zip: Path, county_lookup: Dict[str, str], year_code: str) -> gpd.GeoDataFrame:
    src = f"zip://{vtd_zip.resolve()}"
    gdf = gpd.read_file(src).to_crs("EPSG:4326")

    county_col = f"COUNTYFP{year_code}"
    vtd_col = f"VTDST{year_code}"
    if county_col not in gdf.columns or vtd_col not in gdf.columns:
        raise RuntimeError(f"Missing expected fields in {vtd_zip.name}: {county_col}, {vtd_col}")

    county_names = [county_lookup.get(str(v).zfill(3), str(v).zfill(3)) for v in gdf[county_col]]
    codes = [str(v).strip() for v in gdf[vtd_col]]
    norms = [normalize_text(f"{c} - {p}") for c, p in zip(county_names, codes)]

    gdf["county_nam"] = county_names
    gdf["COUNTYNAME"] = county_names
    gdf["precinct"] = codes
    gdf["PRECINCT"] = codes
    gdf["precinct_name"] = [f"{c} - {p}" for c, p in zip(county_names, codes)]
    gdf["precinct_norm"] = norms
    return gdf


def write_precinct_layers(data_dir: Path, county_lookup: Dict[str, str]) -> None:
    vtd10 = enrich_vtd(data_dir / "tl_2012_48_vtd10.zip", county_lookup, "10")
    vtd20 = enrich_vtd(data_dir / "tl_2020_48_vtd20.zip", county_lookup, "20")

    out10 = data_dir / "tl_2012_48_vtd10.geojson"
    out20 = data_dir / "tl_2020_48_vtd20.geojson"
    vtd10.to_file(out10, driver="GeoJSON")
    vtd20.to_file(out20, driver="GeoJSON")
    print(f"[layers] wrote {out10}")
    print(f"[layers] wrote {out20}")

    # Build centroids for map dot-mode.
    centroid_src = vtd20[["county_nam", "precinct", "precinct_name", "precinct_norm", "geometry"]].copy()
    centroid_src = centroid_src.to_crs("EPSG:3083")
    centroid_src["geometry"] = centroid_src.geometry.centroid
    centroid_src = centroid_src.to_crs("EPSG:4326")
    centroid_src["has_polygon"] = True
    centroids_out = data_dir / "precinct_centroids_tx.geojson"
    centroid_src.to_file(centroids_out, driver="GeoJSON")
    print(f"[layers] wrote {centroids_out}")


def build_alias_index_from_norms(norms: Iterable[str]) -> Dict[str, Dict[str, Set[str]]]:
    index: Dict[str, Dict[str, Set[str]]] = defaultdict(lambda: defaultdict(set))
    for norm in norms:
        s = str(norm)
        if " - " not in s:
            continue
        county, code = s.split(" - ", 1)
        county = county.strip().upper()
        code = code.strip().upper()
        counties = county_key_variants(county)
        for c in counties:
            for v in code_variants(code):
                index[c][v].add(s)
            if code:
                index[c][code].add(s)
    return index


def build_block_assignment_weights(
    data_dir: Path, county_lookup: Dict[str, str]
) -> Tuple[Dict[str, Dict[str, List[Tuple[str, float]]]], Dict[str, Dict[str, Set[str]]]]:
    assign_zip = data_dir / "BlockAssign_ST48_TX.zip"
    if not assign_zip.exists():
        raise FileNotFoundError(f"Missing {assign_zip}")

    with zipfile.ZipFile(assign_zip) as z:
        vtd = pd.read_csv(z.open("BlockAssign_ST48_TX_VTD.txt"), sep="|", dtype=str)
        cd = pd.read_csv(z.open("BlockAssign_ST48_TX_CD.txt"), sep="|", dtype=str)
        sldl = pd.read_csv(z.open("BlockAssign_ST48_TX_SLDL.txt"), sep="|", dtype=str)
        sldu = pd.read_csv(z.open("BlockAssign_ST48_TX_SLDU.txt"), sep="|", dtype=str)

    vtd["BLOCKID"] = vtd["BLOCKID"].astype(str).str.strip()
    vtd["COUNTYFP"] = vtd["COUNTYFP"].astype(str).str.zfill(3)
    vtd["VTD20"] = vtd["DISTRICT"].astype(str).str.strip().str.zfill(6)

    # 2020 block population as interpolation weight.
    blocks = pyogrio.read_dataframe(
        f"zip://{(data_dir / 'tl_2020_48_tabblock20.zip').resolve()}",
        read_geometry=False,
        columns=["GEOID20", "POP20"],
    )
    blocks["BLOCKID"] = blocks["GEOID20"].astype(str).str.strip()
    blocks["w"] = pd.to_numeric(blocks["POP20"], errors="coerce").fillna(0.0)
    # Keep zero-pop blocks in denominator to avoid losing geography.
    blocks.loc[blocks["w"] <= 0, "w"] = 1.0
    blocks = blocks[["BLOCKID", "w"]]

    base = vtd[["BLOCKID", "COUNTYFP", "VTD20"]].merge(blocks, on="BLOCKID", how="left")
    base["w"] = base["w"].fillna(1.0)
    base["county_name"] = base["COUNTYFP"].map(county_lookup).fillna(base["COUNTYFP"])
    base["precinct_norm"] = (base["county_name"] + " - " + base["VTD20"]).map(normalize_text)

    alias_index = build_alias_index_from_norms(base["precinct_norm"].unique().tolist())

    scope_tables = {
        "congressional": cd,
        "state_house": sldl,
        "state_senate": sldu,
    }
    weights_by_scope: Dict[str, Dict[str, List[Tuple[str, float]]]] = {}

    for scope, table in scope_tables.items():
        t = table.copy()
        t["BLOCKID"] = t["BLOCKID"].astype(str).str.strip()
        t["district_id"] = t["DISTRICT"].map(normalize_district_id)
        t = t[t["district_id"] != ""][["BLOCKID", "district_id"]]

        merged = base.merge(t, on="BLOCKID", how="inner")
        grouped = merged.groupby(["precinct_norm", "district_id"], as_index=False)["w"].sum()
        grouped["sum_w"] = grouped.groupby("precinct_norm")["w"].transform("sum")
        grouped["weight"] = grouped["w"] / grouped["sum_w"]

        scope_weights: Dict[str, List[Tuple[str, float]]] = defaultdict(list)
        for _, row in grouped.iterrows():
            wt = float(row["weight"])
            if wt <= 0:
                continue
            scope_weights[str(row["precinct_norm"])].append((str(row["district_id"]), wt))

        for precinct_norm, arr in list(scope_weights.items()):
            s = sum(x[1] for x in arr)
            scope_weights[precinct_norm] = [(d_id, w / s) for d_id, w in arr] if s > 0 else []

        weights_by_scope[scope] = scope_weights
        print(f"[weights] {scope}: {len(scope_weights)} precincts")

    return weights_by_scope, alias_index


def parse_precinct_row_key(row_key: str) -> Tuple[str, str]:
    s = str(row_key or "").strip().upper()
    if " - " in s:
        county, code = s.split(" - ", 1)
        parsed_code = extract_precinct_code(code)
        if parsed_code:
            return normalize_text(county), parsed_code
        return normalize_text(county), re.sub(r"[^A-Z0-9.\-]", "", code.strip().upper())
    return normalize_text(s), ""


def match_precinct_norms(row_key: str, alias_index: Dict[str, Dict[str, Set[str]]]) -> List[str]:
    county, code = parse_precinct_row_key(row_key)
    if not county or not code or is_non_geographic_precinct(code):
        return []
    hits: Set[str] = set()
    for county_key in county_key_variants(county):
        county_map = alias_index.get(county_key, {})
        for v in code_variants(code):
            hits.update(county_map.get(v, set()))
    return sorted(hits)


def build_district_payload(
    rows: List[dict],
    scope: str,
    contest_type: str,
    year: int,
    alias_index: Dict[str, Dict[str, Set[str]]],
    weights_by_precinct: Dict[str, List[Tuple[str, float]]],
    crosswalk_files: List[str],
) -> dict:
    district_votes: Dict[str, Dict[str, float]] = defaultdict(lambda: {"dem": 0.0, "rep": 0.0, "other": 0.0})
    dem_candidate_votes: Dict[str, Counter] = defaultdict(Counter)
    rep_candidate_votes: Dict[str, Counter] = defaultdict(Counter)

    total_rows = 0
    matched_rows = 0
    total_votes = 0.0
    matched_votes = 0.0

    for row in rows:
        total_rows += 1
        dem = float(row["dem_votes"])
        rep = float(row["rep_votes"])
        oth = float(row["other_votes"])
        total = float(row["total_votes"])
        total_votes += total

        precinct_norms = match_precinct_norms(row["county"], alias_index)
        if not precinct_norms:
            continue

        matched_any = False
        split = 1.0 / len(precinct_norms)
        for precinct_norm in precinct_norms:
            d_weights = weights_by_precinct.get(precinct_norm, [])
            if not d_weights:
                continue
            matched_any = True
            for district_id, wt in d_weights:
                share = split * wt
                district_votes[district_id]["dem"] += dem * share
                district_votes[district_id]["rep"] += rep * share
                district_votes[district_id]["other"] += oth * share
                if row.get("dem_candidate"):
                    dem_candidate_votes[district_id][row["dem_candidate"]] += dem * share
                if row.get("rep_candidate"):
                    rep_candidate_votes[district_id][row["rep_candidate"]] += rep * share

        if matched_any:
            matched_rows += 1
            matched_votes += total

    results = {}
    for district_id, v in sorted(district_votes.items(), key=lambda x: int(x[0])):
        dem = float(v["dem"])
        rep = float(v["rep"])
        oth = float(v["other"])
        total = dem + rep + oth
        if total <= 0:
            continue
        margin_votes = rep - dem
        margin_pct = (margin_votes / total) * 100.0 if total else 0.0
        winner = "REP" if rep > dem else ("DEM" if dem > rep else "TIE")
        dem_cand = dem_candidate_votes[district_id].most_common(1)[0][0] if dem_candidate_votes[district_id] else ""
        rep_cand = rep_candidate_votes[district_id].most_common(1)[0][0] if rep_candidate_votes[district_id] else ""

        results[district_id] = {
            "dem_votes": round(dem),
            "rep_votes": round(rep),
            "other_votes": round(oth),
            "total_votes": round(total),
            "dem_candidate": dem_cand,
            "rep_candidate": rep_cand,
            "margin": round(margin_votes),
            "margin_pct": margin_pct,
            "winner": winner,
        }

    coverage = (matched_votes / total_votes * 100.0) if total_votes > 0 else 0.0
    payload = {
        "meta": {
            "scope": scope,
            "contest_type": contest_type,
            "year": year,
            "method": "block_assignment_vtd_population_weighted",
            "crosswalk_files_available": crosswalk_files,
            "match_coverage_pct": coverage,
            "matched_precinct_rows": matched_rows,
            "total_precinct_rows": total_rows,
            "matched_total_votes": round(matched_votes),
            "total_votes": round(total_votes),
            "generated_at": datetime.now(timezone.utc).isoformat(),
        },
        "general": {"results": results},
    }
    return payload


def write_json(path: Path, payload: dict) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", encoding="utf-8") as f:
        json.dump(payload, f, ensure_ascii=True, indent=2)


def build_contest_manifest_entry(year: int, contest_type: str, file_name: str, rows: List[dict]) -> dict:
    dem_total = sum(float(r["dem_votes"]) for r in rows)
    rep_total = sum(float(r["rep_votes"]) for r in rows)
    other_total = sum(float(r["other_votes"]) for r in rows)
    total_votes = sum(float(r["total_votes"]) for r in rows)
    return {
        "year": year,
        "contest_type": contest_type,
        "file": file_name,
        "rows": len(rows),
        "dem_total": round(dem_total),
        "rep_total": round(rep_total),
        "other_total": round(other_total),
        "total_votes": round(total_votes),
        "major_party_contested": bool(dem_total > 0 and rep_total > 0),
    }


def upsert_contest_manifest_entry(entries: List[dict], entry: dict) -> None:
    year = int(entry.get("year", 0))
    contest_type = str(entry.get("contest_type", ""))
    for i, existing in enumerate(entries):
        if int(existing.get("year", 0)) == year and str(existing.get("contest_type", "")) == contest_type:
            entries[i] = entry
            return
    entries.append(entry)


def main() -> None:
    parser = argparse.ArgumentParser(description="Build TX precinct layers and district aggregates from precinct CSVs.")
    parser.add_argument("--data-dir", default="Data", help="Data directory (default: Data)")
    parser.add_argument(
        "--skip-shapefile-contests",
        action="store_true",
        help="Skip overriding 2016/2018/2020 contest slices from tx_YYYY.zip shapefiles.",
    )
    args = parser.parse_args()

    data_dir = Path(args.data_dir).resolve()
    openelections_root = data_dir / "openelections-data-tx"
    contests_dir = data_dir / "contests"
    district_contests_dir = data_dir / "district_contests"

    county_lookup = load_county_lookup(data_dir / "tl_2020_48_county20.geojson")
    write_precinct_layers(data_dir, county_lookup)

    files_by_year = choose_general_file_set(openelections_root)
    contest_manifest_entries: List[dict] = []
    contest_rows_by_key: Dict[Tuple[str, int], List[dict]] = {}

    for year, files in sorted(files_by_year.items()):
        contest_rows_by_type = build_precinct_contest_rows(files, year)
        for contest_type, rows in sorted(contest_rows_by_type.items()):
            if not rows:
                continue
            key = f"{contest_type}_{year}"
            out_path = contests_dir / f"{key}.json"
            write_json(out_path, {"rows": rows})
            contest_rows_by_key[(contest_type, year)] = rows
            upsert_contest_manifest_entry(
                contest_manifest_entries,
                build_contest_manifest_entry(year, contest_type, out_path.name, rows),
            )
            print(f"[write] contests/{out_path.name}")

    if not args.skip_shapefile_contests:
        if aggregate_contest_rows_from_shapefile is None or load_existing_candidate_tokens is None:
            print("[warn] shapefile contest module unavailable; skipping shapefile contest overrides")
        else:
            for shapefile_year in (2016, 2018, 2020):
                shp_zip = data_dir / f"tx_{shapefile_year}.zip"
                if not shp_zip.exists():
                    print(f"[skip] missing {shp_zip.name}; no shapefile overrides for {shapefile_year}")
                    continue

                existing_tokens = load_existing_candidate_tokens(contests_dir, shapefile_year)
                rows_by_type = aggregate_contest_rows_from_shapefile(
                    shp_zip=shp_zip,
                    year=shapefile_year,
                    county_lookup=county_lookup,
                    existing_tokens=existing_tokens,
                )
                if not rows_by_type:
                    print(f"[skip] no shapefile contests parsed for {shapefile_year}")
                    continue

                for contest_type, rows in sorted(rows_by_type.items()):
                    if not rows:
                        continue
                    key = f"{contest_type}_{shapefile_year}"
                    out_path = contests_dir / f"{key}.json"
                    write_json(out_path, {"rows": rows})
                    contest_rows_by_key[(contest_type, shapefile_year)] = rows
                    upsert_contest_manifest_entry(
                        contest_manifest_entries,
                        build_contest_manifest_entry(shapefile_year, contest_type, out_path.name, rows),
                    )
                    print(f"[write] contests/{out_path.name} [shapefile]")

    contest_manifest_entries.sort(key=lambda x: (x["contest_type"], x["year"]))
    write_json(contests_dir / "manifest.json", {"files": contest_manifest_entries})
    print(f"[write] contests/manifest.json ({len(contest_manifest_entries)} entries)")

    weights_by_scope, alias_index = build_block_assignment_weights(data_dir, county_lookup)
    crosswalk_files = sorted([p.name for p in data_dir.glob("nhgis_blk*_48.zip")])
    district_manifest_entries: List[dict] = []

    for (contest_type, year), rows in sorted(contest_rows_by_key.items(), key=lambda x: (x[0][1], x[0][0])):
        for scope, scope_weights in weights_by_scope.items():
            payload = build_district_payload(
                rows=rows,
                scope=scope,
                contest_type=contest_type,
                year=year,
                alias_index=alias_index,
                weights_by_precinct=scope_weights,
                crosswalk_files=crosswalk_files,
            )
            result_count = len(payload.get("general", {}).get("results", {}))
            if result_count == 0:
                continue

            key = f"{scope}_{contest_type}_{year}"
            out_path = district_contests_dir / f"{key}.json"
            write_json(out_path, payload)
            print(
                f"[write] district_contests/{out_path.name} "
                f"(districts={result_count}, coverage={payload['meta']['match_coverage_pct']:.2f}%)"
            )
            district_manifest_entries.append(
                {
                    "scope": scope,
                    "year": year,
                    "contest_type": contest_type,
                    "file": out_path.name,
                    "rows": result_count,
                    "districts": result_count,
                    "match_coverage_pct": payload["meta"]["match_coverage_pct"],
                    "major_party_contested": True,
                }
            )

    district_manifest_entries.sort(key=lambda x: (x["scope"], x["contest_type"], x["year"]))
    write_json(district_contests_dir / "manifest.json", {"files": district_manifest_entries})
    print(f"[write] district_contests/manifest.json ({len(district_manifest_entries)} entries)")


if __name__ == "__main__":
    main()
