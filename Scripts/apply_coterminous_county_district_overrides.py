#!/usr/bin/env python3
"""Apply exact county returns to districts proven coterminous by tabblocks."""

from __future__ import annotations

import argparse
import json
import re
import zipfile
from collections import Counter
from datetime import datetime, timezone
from pathlib import Path

import pandas as pd

from build_tlc_district_aggregates import (
    ELECTION_ZIP,
    candidate_display_name,
    map_office,
    party_bucket,
)

ROOT = Path(__file__).resolve().parents[1]
DATA = ROOT / "Data"
CROSSWALKS = DATA / "crosswalks"
CONTESTS = DATA / "contests"
DISTRICT_CONTESTS = DATA / "district_contests"


def verify_ellis_hd10() -> dict:
    path = CROSSWALKS / "block2020_to_state_house_2022.csv"
    blocks = pd.read_csv(
        path,
        dtype={"block_geoid20": str, "COUNTYFP20": str, "district_num": str},
    )
    blocks["COUNTYFP20"] = blocks["COUNTYFP20"].str.zfill(3)
    ellis = blocks[blocks["COUNTYFP20"] == "139"]
    hd10 = blocks[blocks["district_num"] == "10"]
    if ellis.empty or hd10.empty:
        raise ValueError("Ellis County or HD-10 has no assigned tabblocks")
    if set(ellis["district_num"]) != {"10"}:
        raise ValueError(
            f"Ellis County is not wholly in HD-10: {sorted(ellis['district_num'].unique())}"
        )
    if set(hd10["COUNTYFP20"]) != {"139"}:
        raise ValueError(
            f"HD-10 is not wholly in Ellis County: {sorted(hd10['COUNTYFP20'].unique())}"
        )
    if set(ellis["block_geoid20"]) != set(hd10["block_geoid20"]):
        raise ValueError("Ellis County and HD-10 tabblock sets differ")
    return {
        "scope": "state_house",
        "district": "10",
        "county": "ELLIS",
        "county_fips": "139",
        "tabblock_year": 2020,
        "tabblocks": len(ellis),
        "population": int(pd.to_numeric(ellis["POP20"], errors="coerce").fillna(0).sum()),
        "crosswalk_file": path.name,
    }


def exact_result(rows: list[dict], contest_type: str) -> dict:
    dem = round(sum(float(row.get("dem_votes") or 0) for row in rows))
    rep = round(sum(float(row.get("rep_votes") or 0) for row in rows))
    other = round(sum(float(row.get("other_votes") or 0) for row in rows))
    total = dem + rep + other
    margin = rep - dem

    candidate_names = {}
    for bucket in ("dem", "rep"):
        counter: Counter = Counter()
        for row in rows:
            name = str(row.get(f"{bucket}_candidate") or "").strip()
            if name:
                counter[name] += max(float(row.get(f"{bucket}_votes") or 0), 1.0)
        candidate_names[bucket] = (
            candidate_display_name(counter.most_common(1)[0][0], contest_type)
            if counter
            else ""
        )

    return {
        "dem_votes": dem,
        "rep_votes": rep,
        "other_votes": other,
        "total_votes": total,
        "dem_candidate": candidate_names["dem"],
        "rep_candidate": candidate_names["rep"],
        "margin": margin,
        "margin_pct": (margin / total) * 100.0 if total else 0.0,
        "winner": "REP" if rep > dem else ("DEM" if dem > rep else "TIE"),
    }


def load_tlc_ellis_rows(year: int) -> tuple[str, dict[str, list[dict]]]:
    """Load Ellis returns from the same TLC source used by the district builder."""
    member_suffix = f"{year}_General_Election_Returns.csv"
    with zipfile.ZipFile(ELECTION_ZIP) as zf:
        members = [name for name in zf.namelist() if Path(name).name == member_suffix]
        if not members:
            return "", {}
        member = members[0]
        with zf.open(member) as source:
            returns = pd.read_csv(
                source,
                dtype={"FIPS": str, "County": str, "Party": str, "Name": str, "Office": str},
                low_memory=False,
            )
    fips = returns["FIPS"].astype(str).str.replace(r"\.0$", "", regex=True).str.zfill(3)
    county = returns["County"].astype(str).str.strip().str.upper()
    ellis = returns[(fips == "139") | (county == "ELLIS")].copy()
    ellis["_contest"] = ellis["Office"].map(map_office)
    ellis = ellis[ellis["_contest"].notna()].copy()
    ellis["Votes"] = pd.to_numeric(ellis["Votes"], errors="coerce").fillna(0.0)
    grouped = {
        str(contest): rows.to_dict("records")
        for contest, rows in ellis.groupby("_contest")
    }
    return member, grouped


def exact_tlc_result(rows: list[dict], contest_type: str, existing: dict) -> dict:
    votes = {"dem": 0.0, "rep": 0.0, "other": 0.0}
    names = {"dem": Counter(), "rep": Counter()}
    for row in rows:
        bucket = party_bucket(row.get("Party"))
        value = float(row.get("Votes") or 0)
        votes[bucket] += value
        name = str(row.get("Name") or "").strip()
        if bucket in names and name:
            names[bucket][name] += value

    dem = round(votes["dem"])
    rep = round(votes["rep"])
    other = round(votes["other"])
    total = dem + rep + other
    margin = rep - dem
    return {
        "dem_votes": dem,
        "rep_votes": rep,
        "other_votes": other,
        "total_votes": total,
        "dem_candidate": existing.get("dem_candidate")
        or (candidate_display_name(names["dem"].most_common(1)[0][0], contest_type) if names["dem"] else ""),
        "rep_candidate": existing.get("rep_candidate")
        or (candidate_display_name(names["rep"].most_common(1)[0][0], contest_type) if names["rep"] else ""),
        "margin": margin,
        "margin_pct": (margin / total) * 100.0 if total else 0.0,
        "winner": "REP" if rep > dem else ("DEM" if dem > rep else "TIE"),
    }


def apply_year(year: int, proof: dict) -> list[str]:
    changed: list[str] = []
    generated_at = datetime.now(timezone.utc).isoformat()
    if year >= 2012:
        member, contests = load_tlc_ellis_rows(year)
        for contest_type, county_rows in sorted(contests.items()):
            output_path = DISTRICT_CONTESTS / f"state_house_{contest_type}_{year}.json"
            if not output_path.exists():
                continue
            payload = json.loads(output_path.read_text(encoding="utf-8"))
            results = payload.setdefault("general", {}).setdefault("results", {})
            existing = results.get(proof["district"]) or {}
            results[proof["district"]] = exact_tlc_result(county_rows, contest_type, existing)
            meta = payload.setdefault("meta", {})
            overrides = [
                item
                for item in meta.get("exact_coterminous_county_overrides") or []
                if str(item.get("district")) != proof["district"]
            ]
            overrides.append(
                {
                    **proof,
                    "source_file": f"{ELECTION_ZIP.name}:{member}",
                    "source_rows": len(county_rows),
                    "applied_at": generated_at,
                }
            )
            meta["exact_coterminous_county_overrides"] = overrides
            meta["districts"] = len(results)
            output_path.write_text(
                json.dumps(payload, ensure_ascii=True, indent=2) + "\n",
                encoding="utf-8",
            )
            changed.append(output_path.name)
        return changed

    for source_path in sorted(CONTESTS.glob(f"*_{year}.json")):
        contest_type = source_path.stem[: -(len(str(year)) + 1)]
        source = json.loads(source_path.read_text(encoding="utf-8"))
        county_rows = [
            row
            for row in source.get("rows") or []
            if str(row.get("county") or "").strip().upper().split(" - ", 1)[0] == proof["county"]
        ]
        if not county_rows:
            continue

        output_path = DISTRICT_CONTESTS / f"state_house_{contest_type}_{year}.json"
        if not output_path.exists():
            continue
        payload = json.loads(output_path.read_text(encoding="utf-8"))
        results = payload.setdefault("general", {}).setdefault("results", {})
        results[proof["district"]] = exact_result(county_rows, contest_type)

        meta = payload.setdefault("meta", {})
        overrides = [
            item
            for item in meta.get("exact_coterminous_county_overrides") or []
            if str(item.get("district")) != proof["district"]
        ]
        overrides.append(
            {
                **proof,
                "source_file": source_path.name,
                "source_rows": len(county_rows),
                "applied_at": generated_at,
            }
        )
        meta["exact_coterminous_county_overrides"] = overrides
        meta["districts"] = len(results)
        output_path.write_text(json.dumps(payload, ensure_ascii=True, indent=2) + "\n", encoding="utf-8")
        changed.append(output_path.name)
    return changed


def update_manifest(changed: list[str]) -> None:
    manifest_path = DISTRICT_CONTESTS / "manifest.json"
    manifest = json.loads(manifest_path.read_text(encoding="utf-8"))
    changed_set = set(changed)
    for entry in manifest.get("files") or []:
        if entry.get("file") not in changed_set:
            continue
        payload = json.loads((DISTRICT_CONTESTS / entry["file"]).read_text(encoding="utf-8"))
        count = len(payload.get("general", {}).get("results", {}))
        entry["rows"] = count
        entry["districts"] = count
        entry["exact_coterminous_county_override"] = True
    manifest["generated_at"] = datetime.now(timezone.utc).isoformat()
    manifest_path.write_text(json.dumps(manifest, ensure_ascii=True, indent=2) + "\n", encoding="utf-8")


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--years", type=int, nargs="+")
    args = parser.parse_args()
    proof = verify_ellis_hd10()
    years = args.years
    if not years:
        years = sorted(
            {
                int(match.group(1))
                for path in CONTESTS.glob("*.json")
                if (match := re.search(r"_(\d{4})$", path.stem))
            }
        )
    changed: list[str] = []
    for year in years:
        changed.extend(apply_year(year, proof))
    update_manifest(changed)
    print(
        f"verified Ellis County == HD-10 using {proof['tabblocks']:,} tabblocks; "
        f"updated {len(changed)} files"
    )
    for name in changed:
        print(f"  {name}")


if __name__ == "__main__":
    main()
