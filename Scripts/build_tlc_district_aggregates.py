#!/usr/bin/env python3
"""Build district contest aggregates from TLC VTD returns + precinct-district crosswalks."""

from __future__ import annotations

import argparse
import json
import math
import re
import zipfile
from collections import Counter, defaultdict
from datetime import datetime, timezone
from pathlib import Path
from typing import Dict, List, Optional, Tuple

import pandas as pd

ROOT = Path(__file__).resolve().parents[1]
DATA = ROOT / "Data"
TLC = DATA / "tlc"
CROSSWALKS = DATA / "crosswalks"

# TLC's historical return archive is normalized to the 2024-general PCTKEY
# universe. These crosswalks bridge those VTDs through 2020 tabblocks to each
# district plan. Only Congress changes for the 2026 lines.
LINES_CONFIG = {
    2022: {
        "crosswalks": {
            "congressional": CROSSWALKS / "congressional_2022.csv",
            "state_house": CROSSWALKS / "state_house_2022.csv",
            "state_senate": CROSSWALKS / "state_senate_2022.csv",
        },
        "out_dir": DATA / "district_contests",
    },
    2024: {
        "crosswalks": {
            "congressional": CROSSWALKS / "congressional_2022.csv",
            "state_house": CROSSWALKS / "state_house_2022.csv",
            "state_senate": CROSSWALKS / "state_senate_2022.csv",
        },
        "out_dir": DATA / "district_contests",
    },
    2026: {
        "crosswalks": {
            "congressional": CROSSWALKS / "congressional_2026.csv",
            "state_house": CROSSWALKS / "state_house_2022.csv",
            "state_senate": CROSSWALKS / "state_senate_2022.csv",
        },
        "out_dir": DATA / "district_contests_2026",
    },
}

ELECTION_ZIP = TLC / "2024-general-vtds-election-data.zip"
CONTESTS_DIR = DATA / "contests"

OFFICE_MAP = {
    "President": "president",
    "U.S. Sen": "us_senate",
    "Governor": "governor",
    "Lt. Governor": "lieutenant_governor",
    "Attorney Gen": "attorney_general",
    "Comptroller": "comptroller",
    "Land Comm": "land_commissioner",
    "Ag Comm": "agriculture_commissioner",
    "RR Comm 1": "railroad_commissioner",
    # The regular Railroad Commissioner contest is numbered differently in
    # the 2014 TLC archive. RR Comm 2 in 2012 is a separate special election
    # and must not be folded into the regular contest.
    "RR Comm 3": "railroad_commissioner",
    "CCA Pres Judge": "court_of_criminal_appeals_presiding_judge",
    "Sup Ct Chief": "supreme_court_chief_justice",
}

CANONICAL_CANDIDATE_OVERRIDES = {
    (2014, "supreme_court_chief_justice"): {
        "dem": "William Moody",
        "rep": "Nathan Hecht",
    },
    (2020, "supreme_court_chief_justice"): {
        "dem": "Amy Clark Meachum",
        "rep": "Nathan Hecht",
    },
}

def map_office(office: str) -> Optional[str]:
    o = str(office or "").strip()
    if o in OFFICE_MAP:
        return OFFICE_MAP[o]
    m = re.fullmatch(r"Sup Ct (\d+)", o)
    if m:
        return f"supreme_court_place_{int(m.group(1))}"
    m = re.fullmatch(r"CCA (\d+)", o)
    if m:
        return f"court_of_criminal_appeals_place_{int(m.group(1))}"
    return None


def party_bucket(party: object) -> str:
    p = str(party or "").strip().upper()
    if p in {"D", "DEM", "DEMOCRATIC"}:
        return "dem"
    if p in {"R", "REP", "REPUBLICAN"}:
        return "rep"
    return "other"


def load_crosswalk(path: Path) -> pd.DataFrame:
    """Load and validate a population-weighted VTD-to-district crosswalk."""
    if not path.exists():
        raise FileNotFoundError(f"Missing tabblock crosswalk: {path}")
    df = pd.read_csv(path, dtype={"precinct_key": str, "district_num": str})
    required = {"precinct_key", "district_num", "area_weight"}
    missing = required.difference(df.columns)
    if missing:
        raise ValueError(f"{path.name} is missing columns: {sorted(missing)}")

    out = df[["precinct_key", "district_num", "area_weight"]].copy()
    out["precinct_key"] = out["precinct_key"].astype(str).str.strip().str.upper()
    out["district_num"] = pd.to_numeric(out["district_num"], errors="coerce").astype("Int64")
    out["area_weight"] = pd.to_numeric(out["area_weight"], errors="coerce")
    out = out.dropna(subset=["precinct_key", "district_num", "area_weight"])
    out = out[(out["precinct_key"] != "") & (out["area_weight"] > 0)]
    out = (
        out.groupby(["precinct_key", "district_num"], as_index=False)["area_weight"]
        .sum()
    )

    weight_sums = out.groupby("precinct_key")["area_weight"].sum()
    bad_sums = weight_sums[(weight_sums - 1.0).abs() > 1e-9]
    if not bad_sums.empty:
        raise ValueError(
            f"{path.name} has {len(bad_sums):,} precinct weight sums != 1; "
            f"examples={bad_sums.head(10).to_dict()}"
        )
    return out


def iter_general_return_files(zf: zipfile.ZipFile) -> List[Tuple[int, str]]:
    out = []
    for name in zf.namelist():
        m = re.fullmatch(r"(\d{4})_General_Election_Returns\.csv", Path(name).name)
        if m:
            out.append((int(m.group(1)), name))
    return sorted(out)


def round_allocations_preserving_total(
    district_votes: Dict[int, Dict[str, float]], bucket: str
) -> Dict[int, int]:
    """Largest-remainder rounding that preserves the statewide bucket total."""
    raw = {district: float(votes[bucket]) for district, votes in district_votes.items()}
    rounded = {district: math.floor(votes) for district, votes in raw.items()}
    target = round(sum(raw.values()))
    remaining = target - sum(rounded.values())
    order = sorted(
        raw,
        key=lambda district: (raw[district] - rounded[district], -district),
        reverse=True,
    )
    for district in order[:remaining]:
        rounded[district] += 1
    return rounded


def candidate_display_name(name: str, contest_type: str) -> str:
    """Return the office candidate name rather than a presidential ticket."""
    value = str(name or "").strip()
    if contest_type == "president":
        value = value.split("/", 1)[0].strip()
    if not value or (any(ch.islower() for ch in value) and any(ch.isupper() for ch in value)):
        return value

    particles = {"da", "de", "del", "der", "di", "la", "le", "van", "von"}
    parts = re.split(r"(\s+|-)", value.lower())
    word_index = 0
    normalized = []
    for part in parts:
        if not part or part.isspace() or part == "-":
            normalized.append(part)
            continue
        if word_index > 0 and part in particles:
            token = part
        elif re.fullmatch(r"(?:[a-z]\.)+", part):
            token = part.upper()
        elif part in {"ii", "iii", "iv", "v", "vi"}:
            token = part.upper()
        elif part.startswith("mc") and len(part) > 2:
            token = "Mc" + part[2].upper() + part[3:]
        elif part.startswith("o'") and len(part) > 2:
            token = "O'" + part[2].upper() + part[3:]
        else:
            token = part[:1].upper() + part[1:]
        normalized.append(token)
        word_index += 1
    return "".join(normalized)


def load_canonical_candidates(year: int, contest_type: str) -> Dict[str, str]:
    """Get full statewide candidate names from the normalized contest payload."""
    override = CANONICAL_CANDIDATE_OVERRIDES.get((year, contest_type))
    if override:
        return dict(override)
    path = CONTESTS_DIR / f"{contest_type}_{year}.json"
    if not path.exists():
        return {}
    payload = json.loads(path.read_text(encoding="utf-8"))
    rows = payload.get("rows") or []
    candidates: Dict[str, str] = {}
    for bucket in ("dem", "rep"):
        counter: Counter = Counter()
        name_field = f"{bucket}_candidate"
        vote_field = f"{bucket}_votes"
        for row in rows:
            name = str(row.get(name_field) or "").strip()
            if not name:
                continue
            votes = float(row.get(vote_field) or 0)
            counter[name] += max(votes, 1.0)
        if counter:
            candidates[bucket] = candidate_display_name(
                counter.most_common(1)[0][0],
                contest_type,
            )
    return candidates


def aggregate_year(
    returns: pd.DataFrame,
    crosswalk: pd.DataFrame,
    crosswalk_path: Path,
    scope: str,
    contest_type: str,
    year: int,
    lines_year: int,
    canonical_candidates: Optional[Dict[str, str]] = None,
    candidate_source: Optional[str] = None,
) -> dict:
    total_votes = float(returns["Votes"].sum())
    matched_source = returns["cntyvtd"].isin(set(crosswalk["precinct_key"]))
    matched_votes = float(returns.loc[matched_source, "Votes"].sum())
    m = returns.merge(
        crosswalk,
        left_on="cntyvtd",
        right_on="precinct_key",
        how="left",
        validate="many_to_many",
    )
    matched = m[m["district_num"].notna() & m["area_weight"].notna()].copy()
    matched["allocated_votes"] = matched["Votes"] * matched["area_weight"]

    # Candidate names by party within each district
    dem_names: Dict[int, Counter] = defaultdict(Counter)
    rep_names: Dict[int, Counter] = defaultdict(Counter)
    district_votes: Dict[int, Dict[str, float]] = defaultdict(lambda: {"dem": 0.0, "rep": 0.0, "other": 0.0})

    for _, row in matched.iterrows():
        d = int(row["district_num"])
        votes = float(row["allocated_votes"] or 0)
        bucket = party_bucket(row["Party"])
        district_votes[d][bucket] += votes
        name = str(row.get("Name") or "").strip()
        if name and bucket == "dem":
            dem_names[d][name] += votes
        elif name and bucket == "rep":
            rep_names[d][name] += votes

    rounded_by_bucket = {
        bucket: round_allocations_preserving_total(district_votes, bucket)
        for bucket in ("dem", "rep", "other")
    }
    canonical_candidates = canonical_candidates or {}

    results = {}
    for d, v in sorted(district_votes.items()):
        dem = rounded_by_bucket["dem"][d]
        rep = rounded_by_bucket["rep"][d]
        oth = rounded_by_bucket["other"][d]
        total = dem + rep + oth
        if total <= 0:
            continue
        margin = rep - dem
        results[str(d)] = {
            "dem_votes": dem,
            "rep_votes": rep,
            "other_votes": oth,
            "total_votes": total,
            "dem_candidate": canonical_candidates.get("dem")
            or (dem_names[d].most_common(1)[0][0] if dem_names[d] else ""),
            "rep_candidate": canonical_candidates.get("rep")
            or (rep_names[d].most_common(1)[0][0] if rep_names[d] else ""),
            "margin": margin,
            "margin_pct": (margin / total) * 100.0 if total else 0.0,
            "winner": "REP" if rep > dem else ("DEM" if dem > rep else "TIE"),
        }

    return {
        "meta": {
            "scope": scope,
            "contest_type": contest_type,
            "year": year,
            "lines_year": lines_year,
            "method": "tlc_returns_vtd_to_2020_tabblocks_to_district_population_weighted",
            "crosswalk_file": crosswalk_path.name,
            "weight_basis": "2020 census block population; zero-pop blocks receive weight 1",
            "candidate_source": (
                candidate_source
                or (f"Data/contests/{contest_type}_{year}.json" if canonical_candidates else ELECTION_ZIP.name)
            ),
            "match_coverage_pct": (matched_votes / total_votes * 100.0) if total_votes else 0.0,
            "matched_total_votes": round(matched_votes),
            "total_votes": round(total_votes),
            "districts": len(results),
            "generated_at": datetime.now(timezone.utc).isoformat(),
        },
        "general": {"results": results},
    }


def write_json(path: Path, payload: dict) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", encoding="utf-8") as f:
        json.dump(payload, f, ensure_ascii=True, indent=2)


def upsert_manifest(entries: List[dict], entry: dict) -> None:
    key = (entry.get("scope"), entry.get("contest_type"), int(entry.get("year", 0)))
    for i, existing in enumerate(entries):
        ek = (existing.get("scope"), existing.get("contest_type"), int(existing.get("year", 0)))
        if ek == key:
            entries[i] = entry
            return
    entries.append(entry)


def build_for_lines(lines_year: int, scopes: List[str], years: Optional[List[int]] = None) -> None:
    cfg = LINES_CONFIG[lines_year]
    crosswalk_paths: Dict[str, Path] = cfg["crosswalks"]
    crosswalks = {scope: load_crosswalk(crosswalk_paths[scope]) for scope in scopes}
    out_dir: Path = cfg["out_dir"]
    out_dir.mkdir(parents=True, exist_ok=True)

    manifest_path = out_dir / "manifest.json"
    if manifest_path.exists():
        manifest = json.loads(manifest_path.read_text(encoding="utf-8"))
        entries = list(manifest.get("files") or [])
    else:
        entries = []
    processed_years = set()
    generated_keys = set()

    with zipfile.ZipFile(ELECTION_ZIP) as zf:
        for year, member in iter_general_return_files(zf):
            if years and year not in years:
                continue
            processed_years.add(year)
            print(f"[lines {lines_year}] reading {member}")
            with zf.open(member) as f:
                ret = pd.read_csv(
                    f,
                    dtype={"cntyvtd": str, "Party": str, "Name": str, "Office": str},
                    low_memory=False,
                )
            ret["cntyvtd"] = ret["cntyvtd"].astype(str).str.strip()
            ret["Votes"] = pd.to_numeric(ret["Votes"], errors="coerce").fillna(0.0)

            # Only statewide / judicial offices we map
            ret["_contest"] = ret["Office"].map(map_office)
            mapped = ret[ret["_contest"].notna()].copy()
            if mapped.empty:
                print(f"  no mapped statewide offices for {year}")
                continue

            for contest_type, contest_rows in mapped.groupby("_contest"):
                canonical_candidates = load_canonical_candidates(year, str(contest_type))
                candidate_source = (
                    "Texas Secretary of State canonical candidate names"
                    if (year, str(contest_type)) in CANONICAL_CANDIDATE_OVERRIDES
                    else (
                        f"Data/contests/{contest_type}_{year}.json"
                        if canonical_candidates
                        else ELECTION_ZIP.name
                    )
                )
                for scope in scopes:
                    payload = aggregate_year(
                        contest_rows,
                        crosswalks[scope],
                        crosswalk_paths[scope],
                        scope,
                        str(contest_type),
                        year,
                        lines_year,
                        canonical_candidates,
                        candidate_source,
                    )
                    file_name = f"{scope}_{contest_type}_{year}.json"
                    write_json(out_dir / file_name, payload)
                    upsert_manifest(
                        entries,
                        {
                            "scope": scope,
                            "year": year,
                            "contest_type": contest_type,
                            "file": file_name,
                            "rows": len(payload["general"]["results"]),
                            "districts": len(payload["general"]["results"]),
                            "match_coverage_pct": payload["meta"]["match_coverage_pct"],
                            "major_party_contested": any(
                                r["dem_votes"] > 0 and r["rep_votes"] > 0
                                for r in payload["general"]["results"].values()
                            ),
                        },
                    )
                    generated_keys.add((scope, str(contest_type), year))
                    print(
                        f"  wrote {file_name} districts={payload['meta']['districts']} "
                        f"coverage={payload['meta']['match_coverage_pct']:.1f}%"
                    )

    kept_entries = []
    for entry in entries:
        key = (
            str(entry.get("scope") or ""),
            str(entry.get("contest_type") or ""),
            int(entry.get("year", 0)),
        )
        if key[0] in scopes and key[2] in processed_years and key not in generated_keys:
            stale_name = Path(str(entry.get("file") or "")).name
            stale_path = out_dir / stale_name
            expected_prefix = f"{key[0]}_"
            if stale_name.startswith(expected_prefix) and stale_path.exists():
                stale_path.unlink()
                print(f"  removed stale {stale_name}")
            continue
        kept_entries.append(entry)
    entries = kept_entries

    entries.sort(key=lambda e: (e.get("scope", ""), e.get("contest_type", ""), int(e.get("year", 0))))
    write_json(manifest_path, {"files": entries, "generated_at": datetime.now(timezone.utc).isoformat()})
    print(f"manifest -> {manifest_path} ({len(entries)} entries)")


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--lines", type=int, nargs="+", default=[2026], choices=sorted(LINES_CONFIG))
    parser.add_argument(
        "--scopes",
        nargs="+",
        default=["congressional"],
        choices=["congressional", "state_house", "state_senate"],
    )
    parser.add_argument("--years", type=int, nargs="*", default=None)
    args = parser.parse_args()

    if not ELECTION_ZIP.exists():
        raise FileNotFoundError(f"Missing {ELECTION_ZIP}")

    for ly in args.lines:
        build_for_lines(ly, args.scopes, args.years)


if __name__ == "__main__":
    main()
