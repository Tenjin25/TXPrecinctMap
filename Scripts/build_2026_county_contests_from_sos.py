"""Build 2026 county contest JSONs from the Texas SOS/Civix ENR feed.

The election-night feed is unofficial.  Its compact field names are decoded
here and written in the same schema as the historical files in Data/contests.
"""

from __future__ import annotations

import argparse
import base64
import json
import re
import urllib.request
from pathlib import Path
from typing import Any


ELECTION_ID = 53815
ELECTION_YEAR = 2026
API_ROOT = "https://goelect.txelections.civixapps.com/api-ivis-system/api/s3/enr"

# SOS race ID -> atlas contest key.  The federal Senate race plus the 14
# statewide offices on the November 3, 2026 ballot are included.
RACE_TO_CONTEST = {
    5075: "us_senate",
    3579: "governor",
    4714: "lieutenant_governor",
    3: "attorney_general",
    21: "comptroller",
    20: "land_commissioner",
    19: "agriculture_commissioner",
    4843: "railroad_commissioner",
    18: "supreme_court_chief_justice",
    4706: "supreme_court_place_2_unexpired",
    4711: "supreme_court_place_7",
    4712: "supreme_court_place_8",
    3819: "court_of_criminal_appeals_place_3",
    3820: "court_of_criminal_appeals_place_4",
    3823: "court_of_criminal_appeals_place_9",
}


def fetch_json(url: str) -> dict[str, Any]:
    request = urllib.request.Request(url, headers={"User-Agent": "TXPrecinctMap data builder"})
    with urllib.request.urlopen(request, timeout=60) as response:
        return json.load(response)


def decode_json(value: str) -> Any:
    return json.loads(base64.b64decode(value).decode("utf-8"))


def clean_candidate(name: str) -> str:
    return re.sub(r"\s+\(I\)$", "", str(name or "").strip())


def build_rows(counties: dict[str, Any]) -> dict[str, list[dict[str, Any]]]:
    output = {contest: [] for contest in RACE_TO_CONTEST.values()}
    for county in sorted(counties.values(), key=lambda value: str(value.get("N", ""))):
        county_name = str(county.get("N", "")).strip().upper()
        races = county.get("Races", {})
        for race_id, contest in RACE_TO_CONTEST.items():
            race = races.get(str(race_id), {})
            candidates = list(race.get("C", {}).values())
            dem_candidates = [candidate for candidate in candidates if candidate.get("P") == "DEM"]
            rep_candidates = [candidate for candidate in candidates if candidate.get("P") == "REP"]
            dem_votes = sum(int(candidate.get("V", 0) or 0) for candidate in dem_candidates)
            rep_votes = sum(int(candidate.get("V", 0) or 0) for candidate in rep_candidates)
            other_votes = sum(
                int(candidate.get("V", 0) or 0)
                for candidate in candidates
                if candidate.get("P") not in {"DEM", "REP"}
            )
            total_votes = dem_votes + rep_votes + other_votes
            margin = rep_votes - dem_votes
            output[contest].append(
                {
                    "county": county_name,
                    "dem_votes": dem_votes,
                    "rep_votes": rep_votes,
                    "other_votes": other_votes,
                    "total_votes": total_votes,
                    "dem_candidate": clean_candidate(dem_candidates[0].get("N", "")) if dem_candidates else "",
                    "rep_candidate": clean_candidate(rep_candidates[0].get("N", "")) if rep_candidates else "",
                    "margin": margin,
                    "margin_pct": (margin / total_votes * 100.0) if total_votes else 0.0,
                    "winner": "REP" if margin > 0 else ("DEM" if margin < 0 else "TIE"),
                    "color": "",
                }
            )
    return output


def manifest_entry(contest: str, rows: list[dict[str, Any]]) -> dict[str, Any]:
    dem_total = sum(row["dem_votes"] for row in rows)
    rep_total = sum(row["rep_votes"] for row in rows)
    other_total = sum(row["other_votes"] for row in rows)
    return {
        "year": ELECTION_YEAR,
        "contest_type": contest,
        "file": f"{contest}_{ELECTION_YEAR}.json",
        "rows": len(rows),
        "dem_total": dem_total,
        "rep_total": rep_total,
        "other_total": other_total,
        "total_votes": dem_total + rep_total + other_total,
        "major_party_contested": dem_total > 0 and rep_total > 0,
    }


def write_json(path: Path, value: Any) -> None:
    path.write_text(json.dumps(value, indent=2, ensure_ascii=True) + "\n", encoding="utf-8")


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--data-dir", default="Data", help="Atlas data directory")
    args = parser.parse_args()

    election_envelope = fetch_json(f"{API_ROOT}/election/{ELECTION_ID}")
    county_envelope = fetch_json(f"{API_ROOT}/election/countyInfo/{ELECTION_ID}")
    home = decode_json(election_envelope["Home"])
    counties = decode_json(county_envelope["upload"])
    reporting = home.get("CountiesReporting", {})
    if int(reporting.get("CR", 0)) != 254 or int(reporting.get("CT", 0)) != 254:
        raise RuntimeError(f"Expected 254/254 counties reporting, got {reporting!r}")
    if len(counties) != 254:
        raise RuntimeError(f"Expected 254 county records, got {len(counties)}")

    contests_dir = Path(args.data_dir).resolve() / "contests"
    rows_by_contest = build_rows(counties)
    manifest_path = contests_dir / "manifest.json"
    manifest = json.loads(manifest_path.read_text(encoding="utf-8"))
    entries = [
        entry
        for entry in manifest.get("files", [])
        if not (int(entry.get("year", 0)) == ELECTION_YEAR and entry.get("contest_type") in rows_by_contest)
    ]

    for contest, rows in sorted(rows_by_contest.items()):
        path = contests_dir / f"{contest}_{ELECTION_YEAR}.json"
        write_json(path, {"rows": rows})
        entry = manifest_entry(contest, rows)
        entries.append(entry)
        print(f"[write] {path.name}: {len(rows)} counties, {entry['total_votes']:,} votes")

    entries.sort(key=lambda entry: (str(entry.get("contest_type", "")), int(entry.get("year", 0))))
    write_json(manifest_path, {"files": entries})
    print(
        "[source] Texas SOS/Civix unofficial ENR; "
        f"counties={reporting['CR']}/{reporting['CT']}; updated={home.get('LastUpdatedTime')}"
    )


if __name__ == "__main__":
    main()
