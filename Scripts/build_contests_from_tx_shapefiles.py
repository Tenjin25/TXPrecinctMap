from __future__ import annotations

import argparse
import json
import re
from dataclasses import dataclass
from pathlib import Path
from typing import Dict, Iterable, List, Optional, Set, Tuple

import pandas as pd
import pyogrio


FIELD_RE = re.compile(r"^G(?P<yy>\d{2})(?P<office>[A-Z]{3})(?P<party>[A-Z])(?P<cand>[A-Z0-9]{2,})$")
NAME_TOKEN_RE = re.compile(r"[A-Z]+")

BASE_OFFICE_TO_CONTEST = {
    "PRE": "president",
    "USS": "us_senate",
    "GOV": "governor",
    "LTG": "lieutenant_governor",
    "ATG": "attorney_general",
    "COM": "comptroller",
    "LND": "land_commissioner",
    "AGR": "agriculture_commissioner",
    "RRC": "railroad_commissioner",
}

IGNORED_NAME_WORDS = {
    "THE",
    "OF",
    "AND",
    "JR",
    "SR",
    "III",
    "II",
    "IV",
    "DR",
    "MR",
    "MRS",
    "MS",
    "JUDGE",
    "JUSTICE",
}

MANUAL_RACE_FALLBACK: Dict[int, Dict[str, List[str]]] = {
    2016: {
        "SSC": ["supreme_court_place_3", "supreme_court_place_5", "supreme_court_place_9"],
        "SCC": [
            "court_of_criminal_appeals_place_2",
            "court_of_criminal_appeals_place_5",
            "court_of_criminal_appeals_place_6",
        ],
    },
    2018: {
        "SSC": ["supreme_court_place_2", "supreme_court_place_4", "supreme_court_place_6"],
        "SCC": [
            "court_of_criminal_appeals_presiding_judge",
            "court_of_criminal_appeals_place_7",
            "court_of_criminal_appeals_place_8",
        ],
    },
    2020: {
        "SSC": [
            "supreme_court_place_6",
            "supreme_court_place_6_unexpired",
            "supreme_court_place_7",
            "supreme_court_place_8",
        ],
        "SCC": [
            "court_of_criminal_appeals_place_3",
            "court_of_criminal_appeals_place_4",
            "court_of_criminal_appeals_place_9",
        ],
    },
}


@dataclass(frozen=True)
class ParsedField:
    name: str
    office: str
    party: str
    candidate_code: str


def normalize_text(value: str) -> str:
    return re.sub(r"\s+", " ", re.sub(r"[^A-Za-z0-9 .\-]", "", str(value))).strip().upper()


def load_county_lookup(counties_geojson: Path) -> Dict[str, str]:
    info = pyogrio.read_dataframe(str(counties_geojson), read_geometry=False)
    countyfp_col = "COUNTYFP20" if "COUNTYFP20" in info.columns else "COUNTYFP"
    name_col = "NAME20" if "NAME20" in info.columns else "NAME"
    out: Dict[str, str] = {}
    for _, row in info.iterrows():
        countyfp = str(row[countyfp_col]).strip().zfill(3)
        name = normalize_text(str(row[name_col]))
        if countyfp and name:
            out[countyfp] = name
    return out


def parse_vote_fields(columns: Iterable[str], year: int) -> List[ParsedField]:
    out: List[ParsedField] = []
    yy = str(year % 100).zfill(2)
    for col in columns:
        m = FIELD_RE.match(str(col))
        if not m:
            continue
        if m.group("yy") != yy:
            continue
        out.append(
            ParsedField(
                name=str(col),
                office=m.group("office"),
                party=m.group("party"),
                candidate_code=m.group("cand"),
            )
        )
    return out


def group_multirace_fields(parsed_fields: List[ParsedField], office_code: str) -> List[List[ParsedField]]:
    office_fields = [f for f in parsed_fields if f.office == office_code]
    races: List[List[ParsedField]] = []
    current: List[ParsedField] = []
    seen_parties: Set[str] = set()
    for field in office_fields:
        if current and field.party in seen_parties:
            races.append(current)
            current = []
            seen_parties = set()
        current.append(field)
        seen_parties.add(field.party)
    if current:
        races.append(current)
    return races


def name_tokens(name: str) -> Set[str]:
    s = normalize_text(name)
    if not s:
        return set()
    tokens = set()
    for word in NAME_TOKEN_RE.findall(s):
        if word in IGNORED_NAME_WORDS or len(word) < 3:
            continue
        tokens.add(word[:3])
    return tokens


def primary_candidate_names(rows: List[dict]) -> Tuple[str, str]:
    dem = ""
    rep = ""
    for row in rows:
        if not dem:
            dem = str(row.get("dem_candidate") or "").strip()
        if not rep:
            rep = str(row.get("rep_candidate") or "").strip()
        if dem and rep:
            break
    return dem, rep


def load_existing_candidate_tokens(contests_dir: Path, year: int) -> Dict[str, Dict[str, Set[str]]]:
    out: Dict[str, Dict[str, Set[str]]] = {}
    for path in contests_dir.glob(f"*_{year}.json"):
        contest_type = path.stem.rsplit("_", 1)[0]
        try:
            payload = json.loads(path.read_text(encoding="utf-8"))
        except Exception:
            continue
        rows = payload.get("rows", []) if isinstance(payload, dict) else payload
        if not isinstance(rows, list):
            continue
        dem_name, rep_name = primary_candidate_names(rows)
        out[contest_type] = {
            "dem_tokens": name_tokens(dem_name),
            "rep_tokens": name_tokens(rep_name),
            "dem_name": {dem_name} if dem_name else set(),
            "rep_name": {rep_name} if rep_name else set(),
        }
    return out


def map_multirace_contests(
    races: List[List[ParsedField]],
    candidate_tokens_by_contest: Dict[str, Dict[str, Set[str]]],
    contest_candidates: List[str],
) -> Dict[int, str]:
    pair_scores: List[Tuple[int, int, int]] = []
    for race_i, race_fields in enumerate(races):
        race_by_party = {f.party: f for f in race_fields}
        rep_code = race_by_party["R"].candidate_code if "R" in race_by_party else ""
        dem_code = race_by_party["D"].candidate_code if "D" in race_by_party else ""

        for contest_i, contest_type in enumerate(contest_candidates):
            toks = candidate_tokens_by_contest.get(contest_type, {})
            rep_toks = toks.get("rep_tokens", set())
            dem_toks = toks.get("dem_tokens", set())
            score = 0
            if rep_code:
                score += 3 if rep_code in rep_toks else -1
            if dem_code:
                score += 3 if dem_code in dem_toks else -1
            if not rep_code and not rep_toks:
                score += 1
            if not dem_code and not dem_toks:
                score += 1
            pair_scores.append((score, race_i, contest_i))

    pair_scores.sort(key=lambda x: x[0], reverse=True)
    assigned_races: Set[int] = set()
    assigned_contests: Set[int] = set()
    out: Dict[int, str] = {}
    for score, race_i, contest_i in pair_scores:
        if score <= 0:
            continue
        if race_i in assigned_races or contest_i in assigned_contests:
            continue
        assigned_races.add(race_i)
        assigned_contests.add(contest_i)
        out[race_i] = contest_candidates[contest_i]
    return out


def resolve_race_to_contest_map(
    year: int,
    office_code: str,
    races: List[List[ParsedField]],
    candidate_tokens_by_contest: Dict[str, Dict[str, Set[str]]],
) -> Dict[int, str]:
    if office_code not in {"SSC", "SCC"}:
        return {}

    if office_code == "SSC":
        candidates = sorted(
            [k for k in candidate_tokens_by_contest.keys() if k.startswith("supreme_court_place_")]
        )
    else:
        candidates = sorted(
            [k for k in candidate_tokens_by_contest.keys() if k.startswith("court_of_criminal_appeals_")]
        )

    mapped = map_multirace_contests(races, candidate_tokens_by_contest, candidates)
    if len(mapped) == len(races):
        return mapped

    fallback = MANUAL_RACE_FALLBACK.get(year, {}).get(office_code, [])
    if not fallback:
        return mapped

    out = dict(mapped)
    used_contests = set(out.values())
    fallback_candidates = [c for c in fallback if c not in used_contests]
    generic_candidates = [c for c in candidates if c not in used_contests and c not in fallback_candidates]
    fill_pool = fallback_candidates + generic_candidates
    for i in range(min(len(races), len(fallback))):
        if i not in out:
            if not fill_pool:
                break
            out[i] = fill_pool.pop(0)
    return out


def build_contest_field_map(
    parsed_fields: List[ParsedField],
    year: int,
    candidate_tokens_by_contest: Dict[str, Dict[str, Set[str]]],
) -> Dict[str, List[ParsedField]]:
    contest_fields: Dict[str, List[ParsedField]] = {}

    for office_code, contest_type in BASE_OFFICE_TO_CONTEST.items():
        fields = [f for f in parsed_fields if f.office == office_code]
        if fields:
            contest_fields[contest_type] = fields

    for office_code in ("SSC", "SCC"):
        races = group_multirace_fields(parsed_fields, office_code)
        if not races:
            continue
        race_map = resolve_race_to_contest_map(year, office_code, races, candidate_tokens_by_contest)
        for i, race_fields in enumerate(races):
            contest_type = race_map.get(i)
            if not contest_type:
                continue
            contest_fields[contest_type] = race_fields

    return contest_fields


def summarize_candidate_names(
    existing_tokens: Dict[str, Dict[str, Set[str]]],
    contest_type: str,
) -> Tuple[str, str]:
    tok = existing_tokens.get(contest_type, {})
    dem_name = next(iter(tok.get("dem_name", set())), "")
    rep_name = next(iter(tok.get("rep_name", set())), "")
    return dem_name, rep_name


def aggregate_contest_rows_from_shapefile(
    shp_zip: Path,
    year: int,
    county_lookup: Dict[str, str],
    existing_tokens: Dict[str, Dict[str, Set[str]]],
) -> Dict[str, List[dict]]:
    info = pyogrio.read_info(str(shp_zip))
    raw_fields = info["fields"].tolist() if hasattr(info["fields"], "tolist") else list(info["fields"])
    parsed_fields = parse_vote_fields(raw_fields, year)
    if not parsed_fields:
        return {}

    contest_field_map = build_contest_field_map(parsed_fields, year, existing_tokens)
    if not contest_field_map:
        return {}

    required_cols: Set[str] = {"CNTY", "PREC"}
    for fields in contest_field_map.values():
        for f in fields:
            required_cols.add(f.name)

    df = pyogrio.read_dataframe(str(shp_zip), read_geometry=False, columns=sorted(required_cols))
    df["CNTY"] = df["CNTY"].astype(str).str.strip().str.zfill(3)
    df["PREC"] = df["PREC"].astype(str).str.strip().str.upper()
    df["county_name"] = df["CNTY"].map(county_lookup)
    df = df[df["county_name"].notna()].copy()
    df["key"] = df["county_name"].map(normalize_text) + " - " + df["PREC"]

    for c in required_cols:
        if c in {"CNTY", "PREC"}:
            continue
        df[c] = pd.to_numeric(df[c], errors="coerce").fillna(0.0)

    out: Dict[str, List[dict]] = {}
    for contest_type, fields in sorted(contest_field_map.items()):
        dem_cols = [f.name for f in fields if f.party == "D"]
        rep_cols = [f.name for f in fields if f.party == "R"]
        all_cols = [f.name for f in fields]
        if not all_cols:
            continue

        work = pd.DataFrame({"county": df["key"]})
        work["dem_votes"] = df[dem_cols].sum(axis=1) if dem_cols else 0.0
        work["rep_votes"] = df[rep_cols].sum(axis=1) if rep_cols else 0.0
        work["total_votes"] = df[all_cols].sum(axis=1)
        work["other_votes"] = work["total_votes"] - work["dem_votes"] - work["rep_votes"]
        work = work[work["total_votes"] > 0].copy()

        dem_name, rep_name = summarize_candidate_names(existing_tokens, contest_type)

        rows: List[dict] = []
        for _, row in work.iterrows():
            dem = float(row["dem_votes"])
            rep = float(row["rep_votes"])
            other = float(row["other_votes"])
            total = float(row["total_votes"])
            margin = rep - dem
            margin_pct = (margin / total * 100.0) if total else 0.0
            winner = "REP" if rep > dem else ("DEM" if dem > rep else "TIE")

            rows.append(
                {
                    "county": str(row["county"]),
                    "dem_votes": round(dem),
                    "rep_votes": round(rep),
                    "other_votes": round(other),
                    "total_votes": round(total),
                    "dem_candidate": dem_name,
                    "rep_candidate": rep_name,
                    "margin": round(margin),
                    "margin_pct": margin_pct,
                    "winner": winner,
                    "color": "",
                }
            )

        rows.sort(key=lambda r: r["county"])
        out[contest_type] = rows
    return out


def read_manifest(contests_dir: Path) -> List[dict]:
    path = contests_dir / "manifest.json"
    if not path.exists():
        return []
    try:
        obj = json.loads(path.read_text(encoding="utf-8"))
    except Exception:
        return []
    rows = obj.get("files", []) if isinstance(obj, dict) else []
    return rows if isinstance(rows, list) else []


def write_json(path: Path, payload: dict) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", encoding="utf-8") as f:
        json.dump(payload, f, ensure_ascii=True, indent=2)


def upsert_manifest_entry(entries: List[dict], contest_type: str, year: int, rows: List[dict]) -> None:
    file_name = f"{contest_type}_{year}.json"
    dem_total = sum(float(r["dem_votes"]) for r in rows)
    rep_total = sum(float(r["rep_votes"]) for r in rows)
    other_total = sum(float(r["other_votes"]) for r in rows)
    total_votes = sum(float(r["total_votes"]) for r in rows)
    record = {
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

    for i, existing in enumerate(entries):
        if (
            str(existing.get("contest_type")) == contest_type
            and int(existing.get("year", 0)) == int(year)
        ):
            entries[i] = record
            return
    entries.append(record)


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Build statewide precinct contest JSON slices from tx_2016/2018/2020 shapefiles."
    )
    parser.add_argument("--data-dir", default="Data", help="Data directory (default: Data)")
    parser.add_argument("--years", nargs="+", type=int, default=[2016, 2018, 2020], help="Election years to build.")
    parser.add_argument("--dry-run", action="store_true", help="Do not write files; print what would change.")
    args = parser.parse_args()

    data_dir = Path(args.data_dir).resolve()
    contests_dir = data_dir / "contests"
    county_lookup = load_county_lookup(data_dir / "tl_2020_48_county20.geojson")
    manifest_entries = read_manifest(contests_dir)
    generated_count = 0

    for year in args.years:
        shp_zip = data_dir / f"tx_{year}.zip"
        if not shp_zip.exists():
            print(f"[skip] missing {shp_zip.name}")
            continue

        existing_tokens = load_existing_candidate_tokens(contests_dir, year)
        rows_by_contest = aggregate_contest_rows_from_shapefile(
            shp_zip=shp_zip,
            year=year,
            county_lookup=county_lookup,
            existing_tokens=existing_tokens,
        )
        if not rows_by_contest:
            print(f"[skip] no contest fields parsed for {year}")
            continue

        for contest_type, rows in sorted(rows_by_contest.items()):
            if not rows:
                continue
            out_path = contests_dir / f"{contest_type}_{year}.json"
            if args.dry_run:
                print(f"[dry-run] {out_path.name} rows={len(rows)}")
            else:
                write_json(out_path, {"rows": rows})
                print(f"[write] contests/{out_path.name} rows={len(rows)}")
            upsert_manifest_entry(manifest_entries, contest_type, year, rows)
            generated_count += 1

    if args.dry_run:
        print(f"[dry-run] generated contest files: {generated_count}")
        return

    manifest_entries.sort(key=lambda x: (str(x.get("contest_type", "")), int(x.get("year", 0))))
    write_json(contests_dir / "manifest.json", {"files": manifest_entries})
    print(f"[write] contests/manifest.json entries={len(manifest_entries)}")
    print(f"[done] generated contest files: {generated_count}")


if __name__ == "__main__":
    main()
