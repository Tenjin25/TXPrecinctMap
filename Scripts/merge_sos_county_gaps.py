#!/usr/bin/env python3
"""Merge Texas SOS county-level results into contest JSON slices for missing counties.

Fetches official county tables from elections.sos.state.tx.us historical pages and
appends county-only rows for counties that are not already present in each
contest JSON (including counties that only appear via precinct keys like
"HARRIS - 101"). Existing precinct rows are left untouched.

Example (2014 general):
  python Scripts/merge_sos_county_gaps.py --year 2014 --dry-run
  python Scripts/merge_sos_county_gaps.py --year 2014 --write-csv
"""

from __future__ import annotations

import argparse
import csv
import json
import re
import sys
import time
import urllib.error
import urllib.request
from collections import Counter, defaultdict
from dataclasses import dataclass, field
from html import unescape
from pathlib import Path
from typing import Dict, Iterable, List, Optional, Sequence, Tuple


# Historical election index codes used by SOS URL pattern elchist{code}_race{id}.htm
SOS_ELECTION_CODES = {
    2014: 175,
}

# Statewide races present on the 2014 SOS race-select page that map to atlas contest keys.
# Race IDs are the OPTION values on elchist175_raceselect.htm.
SOS_RACE_MAP_2014: Dict[str, int] = {
    "us_senate": 832,
    "governor": 833,
    "lieutenant_governor": 834,
    "attorney_general": 835,
    "comptroller": 836,
    "land_commissioner": 838,
    "agriculture_commissioner": 839,
    "railroad_commissioner": 93,
    "supreme_court_place_6_unexpired": 3011,
    "supreme_court_place_7": 3012,
    "supreme_court_place_8": 3013,
    "court_of_criminal_appeals_place_3": 99,
    "court_of_criminal_appeals_place_4": 3015,
    "court_of_criminal_appeals_place_9": 3020,
}

USER_AGENT = "TXPrecinctMap-sos-merge/1.0 (+local data backfill)"

# SOS historical pages occasionally omit spaces or use alternate spellings.
COUNTY_ALIASES = {
    "LASALLE": "LA SALLE",
    "DE WITT": "DEWITT",
    "DEWITT": "DEWITT",
    "GALVESTONE": "GALVESTON",  # occasional source typo
}


def normalize_county(value: object) -> str:
    text = re.sub(r"\s+", " ", str(value or "").strip().upper())
    # Match atlas / OpenElections county token style.
    text = text.replace(".", "")
    return COUNTY_ALIASES.get(text, text)


def county_token_from_row_key(key: object) -> str:
    raw = str(key or "").strip()
    if not raw:
        return ""
    return normalize_county(raw.split(" - ", 1)[0])


def parse_int_votes(value: object) -> int:
    s = re.sub(r"[^0-9\-]", "", str(value or "").strip())
    if not s or s == "-":
        return 0
    try:
        return int(s)
    except ValueError:
        return 0


def strip_tags(html: str) -> str:
    text = re.sub(r"(?is)<script[^>]*>.*?</script>", " ", html)
    text = re.sub(r"(?is)<style[^>]*>.*?</style>", " ", text)
    text = re.sub(r"(?is)<br\s*/?>", " ", text)
    text = re.sub(r"(?is)<[^>]+>", " ", text)
    return unescape(re.sub(r"\s+", " ", text)).strip()


def extract_table_html(page_html: str) -> str:
    m = re.search(r"(?is)<table\b[^>]*>.*?</table>", page_html)
    if not m:
        raise ValueError("No <table> found in SOS race page")
    return m.group(0)


def parse_table_rows(table_html: str) -> List[List[str]]:
    rows: List[List[str]] = []
    for tr in re.findall(r"(?is)<tr\b[^>]*>(.*?)</tr>", table_html):
        cells = re.findall(r"(?is)<t[hd]\b[^>]*>(.*?)</t[hd]>", tr)
        if not cells:
            continue
        rows.append([strip_tags(c) for c in cells])
    if len(rows) < 4:
        raise ValueError(f"Unexpected SOS table shape: {len(rows)} rows")
    return rows


@dataclass
class PartyColumn:
    index: int
    party: str
    first_name: str = ""
    last_name: str = ""

    @property
    def candidate(self) -> str:
        name = f"{self.first_name} {self.last_name}".strip()
        return re.sub(r"\s+", " ", name)


@dataclass
class CountyResult:
    county: str
    dem: int = 0
    rep: int = 0
    other: int = 0
    dem_candidate: str = ""
    rep_candidate: str = ""
    total_votes: int = 0

    def to_contest_row(self) -> dict:
        total = self.total_votes if self.total_votes > 0 else (self.dem + self.rep + self.other)
        margin = self.rep - self.dem
        margin_pct = (margin / total) * 100.0 if total else 0.0
        if self.rep > self.dem:
            winner = "REP"
        elif self.dem > self.rep:
            winner = "DEM"
        else:
            winner = "TIE"
        return {
            "county": self.county,
            "dem_votes": int(self.dem),
            "rep_votes": int(self.rep),
            "other_votes": int(self.other),
            "total_votes": int(total),
            "dem_candidate": self.dem_candidate,
            "rep_candidate": self.rep_candidate,
            "margin": int(margin),
            "margin_pct": margin_pct,
            "winner": winner,
            "color": "",
            "source": "sos_county",
        }


@dataclass
class RaceParseResult:
    contest_type: str
    race_id: int
    office_label: str
    counties: List[CountyResult] = field(default_factory=list)
    party_columns: List[PartyColumn] = field(default_factory=list)


def classify_header_party(label: str) -> Optional[str]:
    u = (label or "").strip().upper()
    if u in {"REP", "REPUBLICAN"}:
        return "REP"
    if u in {"DEM", "DEMOCRATIC", "DEMOCRAT"}:
        return "DEM"
    if u in {"LIB", "LIBERTARIAN", "GRN", "GREEN", "IND", "INDEPENDENT", "W-I", "WI", "W/I", "WRITE-IN"}:
        return "OTHER"
    return None


def parse_sos_race_table(page_html: str, contest_type: str, race_id: int) -> RaceParseResult:
    office_m = re.search(
        r"(?is)</h3>\s*([^<]+?)\s*<table\b",
        page_html,
    )
    office_label = strip_tags(office_m.group(1)) if office_m else contest_type

    table_html = extract_table_html(page_html)
    rows = parse_table_rows(table_html)
    # Header is typically 3 rows: first names, last names, party codes.
    first_names = rows[0]
    last_names = rows[1]
    party_row = rows[2]
    if not party_row or normalize_county(party_row[0]) != "COUNTY":
        raise ValueError(f"Race {race_id}: expected County header row, got {party_row[:3]!r}")

    party_cols: List[PartyColumn] = []
    for idx, party_label in enumerate(party_row):
        if idx == 0:
            continue
        party = classify_header_party(party_label)
        if not party:
            # Votes / Voters / TurnOut trail columns.
            continue
        first = first_names[idx] if idx < len(first_names) else ""
        last = last_names[idx] if idx < len(last_names) else ""
        if first in {"...", "Total"} or last in {"...", "Total"}:
            first = "" if first in {"...", "Total"} else first
            last = "" if last in {"...", "Total"} else last
        party_cols.append(PartyColumn(index=idx, party=party, first_name=first, last_name=last))

    if not party_cols:
        raise ValueError(f"Race {race_id}: no party columns parsed")

    dem_cand = next((c.candidate for c in party_cols if c.party == "DEM" and c.candidate), "")
    rep_cand = next((c.candidate for c in party_cols if c.party == "REP" and c.candidate), "")

    counties: List[CountyResult] = []
    for row in rows[3:]:
        if not row:
            continue
        county = normalize_county(row[0])
        if not county or county == "ALL COUNTIES":
            continue
        dem = 0
        rep = 0
        other = 0
        for col in party_cols:
            votes = parse_int_votes(row[col.index] if col.index < len(row) else 0)
            if col.party == "DEM":
                dem += votes
            elif col.party == "REP":
                rep += votes
            else:
                other += votes
        # Prefer explicit Votes column when present (after party cols).
        total_idx = party_cols[-1].index + 1
        total_votes = parse_int_votes(row[total_idx] if total_idx < len(row) else 0)
        if total_votes <= 0:
            total_votes = dem + rep + other
        if total_votes <= 0 and dem + rep + other <= 0:
            continue
        counties.append(
            CountyResult(
                county=county,
                dem=dem,
                rep=rep,
                other=other,
                dem_candidate=dem_cand,
                rep_candidate=rep_cand,
                total_votes=total_votes,
            )
        )

    if len(counties) < 200:
        raise ValueError(f"Race {race_id}: expected ~254 counties, got {len(counties)}")

    return RaceParseResult(
        contest_type=contest_type,
        race_id=race_id,
        office_label=office_label,
        counties=counties,
        party_columns=party_cols,
    )


def sos_race_url(election_code: int, race_id: int) -> str:
    return f"https://elections.sos.state.tx.us/elchist{election_code}_race{race_id}.htm"


def fetch_text(url: str, cache_path: Optional[Path], timeout: float = 60.0) -> str:
    if cache_path and cache_path.exists():
        return cache_path.read_text(encoding="utf-8", errors="replace")
    req = urllib.request.Request(url, headers={"User-Agent": USER_AGENT})
    try:
        with urllib.request.urlopen(req, timeout=timeout) as resp:
            raw = resp.read()
    except urllib.error.HTTPError as err:
        raise RuntimeError(f"HTTP {err.code} for {url}") from err
    text = raw.decode("utf-8", errors="replace")
    if cache_path:
        cache_path.parent.mkdir(parents=True, exist_ok=True)
        cache_path.write_text(text, encoding="utf-8")
    return text


def existing_counties_in_payload(payload: dict) -> set[str]:
    rows = payload.get("rows") if isinstance(payload, dict) else None
    if not isinstance(rows, list):
        return set()
    out: set[str] = set()
    for row in rows:
        if not isinstance(row, dict):
            continue
        token = county_token_from_row_key(row.get("county"))
        if token:
            out.add(token)
    return out


def upsert_manifest_entry(entries: List[dict], contest_type: str, year: int, rows: Sequence[dict]) -> None:
    dem_total = sum(float(r.get("dem_votes", 0) or 0) for r in rows)
    rep_total = sum(float(r.get("rep_votes", 0) or 0) for r in rows)
    other_total = sum(float(r.get("other_votes", 0) or 0) for r in rows)
    total_votes = sum(float(r.get("total_votes", 0) or 0) for r in rows)
    record = {
        "year": int(year),
        "contest_type": contest_type,
        "file": f"{contest_type}_{year}.json",
        "rows": len(rows),
        "dem_total": round(dem_total),
        "rep_total": round(rep_total),
        "other_total": round(other_total),
        "total_votes": round(total_votes),
        "major_party_contested": bool(dem_total > 0 and rep_total > 0),
    }
    for i, existing in enumerate(entries):
        if str(existing.get("contest_type")) == contest_type and int(existing.get("year", 0)) == int(year):
            entries[i] = record
            return
    entries.append(record)


def read_manifest(contests_dir: Path) -> List[dict]:
    path = contests_dir / "manifest.json"
    if not path.exists():
        return []
    try:
        payload = json.loads(path.read_text(encoding="utf-8"))
    except Exception:
        return []
    files = payload.get("files", []) if isinstance(payload, dict) else []
    return files if isinstance(files, list) else []


def write_json(path: Path, payload: dict) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, ensure_ascii=True, indent=2) + "\n", encoding="utf-8")


def write_county_csv(path: Path, race: RaceParseResult) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    fieldnames = ["county", "office", "district", "candidate", "party", "votes"]
    with path.open("w", encoding="utf-8", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()
        for county in race.counties:
            for party, cand, votes in (
                ("DEM", county.dem_candidate, county.dem),
                ("REP", county.rep_candidate, county.rep),
            ):
                if votes or cand:
                    writer.writerow(
                        {
                            "county": county.county,
                            "office": race.office_label,
                            "district": "",
                            "candidate": cand,
                            "party": party,
                            "votes": votes,
                        }
                    )
            if county.other:
                writer.writerow(
                    {
                        "county": county.county,
                        "office": race.office_label,
                        "district": "",
                        "candidate": "OTHER",
                        "party": "OTH",
                        "votes": county.other,
                    }
                )


def merge_race_into_contest(
    contests_dir: Path,
    year: int,
    race: RaceParseResult,
    *,
    dry_run: bool,
    attach_county_totals: bool,
) -> Tuple[int, int, List[str]]:
    path = contests_dir / f"{race.contest_type}_{year}.json"
    if path.exists():
        payload = json.loads(path.read_text(encoding="utf-8"))
        if not isinstance(payload, dict):
            payload = {"rows": []}
    else:
        payload = {"rows": []}

    rows = payload.get("rows")
    if not isinstance(rows, list):
        rows = []
        payload["rows"] = rows

    have = existing_counties_in_payload(payload)
    missing = [c for c in race.counties if c.county not in have]
    added_names = [c.county for c in missing]

    if not dry_run:
        for county in missing:
            rows.append(county.to_contest_row())
        rows.sort(key=lambda r: str((r or {}).get("county", "")).upper())
        if attach_county_totals:
            payload["county_totals"] = {
                c.county: {
                    "dem_votes": c.dem,
                    "rep_votes": c.rep,
                    "other_votes": c.other,
                    "total_votes": c.total_votes if c.total_votes else (c.dem + c.rep + c.other),
                    "dem_candidate": c.dem_candidate,
                    "rep_candidate": c.rep_candidate,
                }
                for c in race.counties
            }
        write_json(path, payload)

    return len(have), len(missing), added_names


def resolve_race_map(year: int) -> Dict[str, int]:
    if year == 2014:
        return dict(SOS_RACE_MAP_2014)
    raise SystemExit(f"No SOS race map configured for year {year}. Add mappings in SOS_RACE_MAP_{year}.")


def main(argv: Optional[Sequence[str]] = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--year", type=int, default=2014, help="Election year (default: 2014)")
    parser.add_argument(
        "--data-dir",
        type=Path,
        default=None,
        help="Data directory containing contests/ (default: <repo>/Data next to Scripts)",
    )
    parser.add_argument(
        "--cache-dir",
        type=Path,
        default=None,
        help="Optional directory for cached SOS HTML pages",
    )
    parser.add_argument(
        "--contest",
        action="append",
        default=[],
        help="Limit to one or more contest_type keys (repeatable)",
    )
    parser.add_argument("--dry-run", action="store_true", help="Report gaps without writing JSON")
    parser.add_argument(
        "--write-csv",
        action="store_true",
        help="Also write per-contest county CSVs under Data/sos_county/{year}/",
    )
    parser.add_argument(
        "--attach-county-totals",
        action="store_true",
        default=True,
        help="Attach full SOS county_totals blob on each contest JSON (default: on)",
    )
    parser.add_argument(
        "--no-attach-county-totals",
        action="store_false",
        dest="attach_county_totals",
        help="Do not write county_totals onto contest JSON",
    )
    parser.add_argument("--sleep", type=float, default=0.35, help="Delay between SOS fetches")
    args = parser.parse_args(argv)

    year = int(args.year)
    if year not in SOS_ELECTION_CODES:
        raise SystemExit(f"Unsupported year {year}; known SOS election codes: {sorted(SOS_ELECTION_CODES)}")

    script_dir = Path(__file__).resolve().parent
    data_dir = (args.data_dir or (script_dir.parent / "Data")).resolve()
    contests_dir = data_dir / "contests"
    if not contests_dir.exists():
        raise SystemExit(f"Missing contests dir: {contests_dir}")

    cache_dir = args.cache_dir
    if cache_dir is None:
        cache_dir = data_dir / "sos_cache" / str(year)
    cache_dir = cache_dir.resolve()

    race_map = resolve_race_map(year)
    if args.contest:
        wanted = {c.strip() for c in args.contest if c.strip()}
        unknown = sorted(wanted - set(race_map))
        if unknown:
            raise SystemExit(f"Unknown contest keys for {year}: {unknown}")
        race_map = {k: v for k, v in race_map.items() if k in wanted}

    election_code = SOS_ELECTION_CODES[year]
    manifest_entries = read_manifest(contests_dir)
    summary = []

    print(f"[sos] year={year} election_code={election_code} data_dir={data_dir}")
    print(f"[sos] contests={', '.join(sorted(race_map))} dry_run={args.dry_run}")

    for contest_type, race_id in sorted(race_map.items(), key=lambda kv: kv[0]):
        url = sos_race_url(election_code, race_id)
        cache_path = cache_dir / f"race_{race_id}.html"
        print(f"[fetch] {contest_type} <- {url}")
        html = fetch_text(url, cache_path)
        race = parse_sos_race_table(html, contest_type, race_id)
        print(f"  parsed office={race.office_label!r} counties={len(race.counties)}")

        if args.write_csv and not args.dry_run:
            csv_path = data_dir / "sos_county" / str(year) / f"{contest_type}_{year}.csv"
            write_county_csv(csv_path, race)
            print(f"  [csv] {csv_path.relative_to(data_dir)}")

        have_n, miss_n, added = merge_race_into_contest(
            contests_dir,
            year,
            race,
            dry_run=args.dry_run,
            attach_county_totals=args.attach_county_totals,
        )
        print(f"  have={have_n} missing={miss_n}")
        if miss_n and miss_n <= 80:
            print(f"  add: {', '.join(added)}")
        elif miss_n:
            print(f"  add (first 40): {', '.join(added[:40])} ...")

        if not args.dry_run:
            out_path = contests_dir / f"{contest_type}_{year}.json"
            rows = json.loads(out_path.read_text(encoding="utf-8")).get("rows", [])
            upsert_manifest_entry(manifest_entries, contest_type, year, rows)

        summary.append((contest_type, have_n, miss_n))
        time.sleep(max(0.0, float(args.sleep)))

    if not args.dry_run:
        manifest_entries.sort(key=lambda x: (str(x.get("contest_type", "")), int(x.get("year", 0))))
        write_json(contests_dir / "manifest.json", {"files": manifest_entries})
        print(f"[write] contests/manifest.json entries={len(manifest_entries)}")

    print("[summary]")
    for contest_type, have_n, miss_n in summary:
        print(f"  {contest_type}: had={have_n} merged={miss_n}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
