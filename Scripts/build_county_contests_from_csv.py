from __future__ import annotations

import argparse
import csv
import json
import re
from collections import Counter, defaultdict
from dataclasses import dataclass
from pathlib import Path
from typing import Dict, Iterable, List, Optional, Tuple


def normalize_text(value: object) -> str:
    return re.sub(r"\s+", " ", re.sub(r"[^a-z0-9 .\-]", "", str(value).lower())).strip().upper()


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


OFFICE_PATTERNS: List[Tuple[re.Pattern[str], str]] = [
    (re.compile(r"PRESIDENT", re.I), "president"),
    (re.compile(r"(U\.?\s*S\.?|US|UNITED STATES)\s*SENAT(?:E|OR)", re.I), "us_senate"),
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


@dataclass
class CountyAggNode:
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


def build_county_contest_rows(path: Path) -> Dict[str, List[dict]]:
    buckets: Dict[str, Dict[str, CountyAggNode]] = defaultdict(dict)

    with path.open("r", encoding="utf-8-sig", newline="") as f:
        reader = csv.DictReader(f)
        for row in reader:
            office = row.get("office", "")
            contest = map_office_to_contest(str(office))
            if not contest:
                continue

            county = normalize_text(row.get("county", ""))
            if not county:
                continue

            candidate = str(row.get("candidate", "")).strip()
            if candidate.upper() == "TOTAL":
                continue

            votes = parse_votes(row.get("votes", "0"))
            party = str(row.get("party", "")).strip().upper()
            if county not in buckets[contest]:
                buckets[contest][county] = CountyAggNode()
            node = buckets[contest][county]

            if party.startswith("DEM"):
                node.dem += votes
                if candidate:
                    node.dem_candidate_votes[candidate] += votes
            elif party.startswith("REP"):
                node.rep += votes
                if candidate:
                    node.rep_candidate_votes[candidate] += votes
            else:
                node.other += votes

    out: Dict[str, List[dict]] = {}
    for contest, county_map in sorted(buckets.items()):
        rows: List[dict] = []
        for county, node in sorted(county_map.items(), key=lambda x: x[0]):
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
                    "county": county,
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
    return out


def read_manifest(contests_dir: Path) -> List[dict]:
    manifest_path = contests_dir / "manifest.json"
    if not manifest_path.exists():
        return []
    try:
        payload = json.loads(manifest_path.read_text(encoding="utf-8"))
    except Exception:
        return []
    files = payload.get("files", []) if isinstance(payload, dict) else []
    return files if isinstance(files, list) else []


def upsert_manifest_entry(entries: List[dict], contest_type: str, year: int, rows: Iterable[dict]) -> None:
    rows = list(rows)
    dem_total = sum(float(r.get("dem_votes", 0.0)) for r in rows)
    rep_total = sum(float(r.get("rep_votes", 0.0)) for r in rows)
    other_total = sum(float(r.get("other_votes", 0.0)) for r in rows)
    total_votes = sum(float(r.get("total_votes", 0.0)) for r in rows)
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
        if (
            str(existing.get("contest_type")) == contest_type
            and int(existing.get("year", 0)) == int(year)
        ):
            entries[i] = record
            return
    entries.append(record)


def write_json(path: Path, payload: dict) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", encoding="utf-8") as f:
        json.dump(payload, f, ensure_ascii=True, indent=2)


def main() -> None:
    parser = argparse.ArgumentParser(description="Build statewide county contest JSON files from a county CSV.")
    parser.add_argument("--csv", required=True, help="Path to county-level election CSV")
    parser.add_argument("--year", required=True, type=int, help="Election year")
    parser.add_argument("--data-dir", default="Data", help="Data directory (default: Data)")
    parser.add_argument(
        "--prune-year-missing",
        action="store_true",
        help="Remove manifest entries/files for the target year that are not present in the source CSV.",
    )
    args = parser.parse_args()

    csv_path = Path(args.csv).resolve()
    if not csv_path.exists():
        raise FileNotFoundError(f"Missing CSV: {csv_path}")

    data_dir = Path(args.data_dir).resolve()
    contests_dir = data_dir / "contests"
    rows_by_contest = build_county_contest_rows(csv_path)
    if not rows_by_contest:
        raise RuntimeError(f"No statewide contests found in {csv_path.name}")

    manifest_entries = read_manifest(contests_dir)
    generated = 0
    generated_contests = set()
    for contest_type, rows in sorted(rows_by_contest.items()):
        if not rows:
            continue
        out_path = contests_dir / f"{contest_type}_{args.year}.json"
        write_json(out_path, {"rows": rows})
        upsert_manifest_entry(manifest_entries, contest_type, args.year, rows)
        generated_contests.add(contest_type)
        generated += 1
        print(f"[write] contests/{out_path.name} rows={len(rows)}")

    if args.prune_year_missing:
        keep_entries: List[dict] = []
        pruned_entries: List[dict] = []
        for entry in manifest_entries:
            entry_year = int(entry.get("year", 0))
            entry_type = str(entry.get("contest_type", ""))
            if entry_year == int(args.year) and entry_type not in generated_contests:
                pruned_entries.append(entry)
            else:
                keep_entries.append(entry)
        manifest_entries = keep_entries
        for entry in pruned_entries:
            file_name = str(entry.get("file", "")).strip()
            if not file_name:
                continue
            stale_path = contests_dir / file_name
            if stale_path.exists():
                stale_path.unlink()
                print(f"[prune] removed contests/{file_name}")

    manifest_entries.sort(key=lambda x: (str(x.get("contest_type", "")), int(x.get("year", 0))))
    write_json(contests_dir / "manifest.json", {"files": manifest_entries})
    print(f"[write] contests/manifest.json entries={len(manifest_entries)}")
    print(f"[done] generated contest files: {generated}")


if __name__ == "__main__":
    main()
