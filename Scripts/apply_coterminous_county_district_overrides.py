#!/usr/bin/env python3
"""Apply exact county returns to districts proven coterminous by tabblocks."""

from __future__ import annotations

import argparse
import json
import re
from collections import Counter
from datetime import datetime, timezone
from pathlib import Path

import pandas as pd

from build_tlc_district_aggregates import candidate_display_name

ROOT = Path(__file__).resolve().parents[1]
DATA = ROOT / "Data"
CROSSWALKS = DATA / "crosswalks"
CONTESTS = DATA / "contests"
DISTRICT_CONTESTS = DATA / "district_contests"


def find_coterminous_county_districts() -> list[dict]:
    path = CROSSWALKS / "block2020_to_state_house_2022.csv"
    counties_path = DATA / "tl_2020_48_county20.geojson"
    blocks = pd.read_csv(
        path,
        dtype={"block_geoid20": str, "COUNTYFP20": str, "district_num": str},
    )
    blocks["COUNTYFP20"] = blocks["COUNTYFP20"].str.zfill(3)
    blocks["district_num"] = blocks["district_num"].astype(str)

    counties = json.loads(counties_path.read_text(encoding="utf-8"))
    county_names = {
        str(feature.get("properties", {}).get("COUNTYFP20") or "").zfill(3):
        str(feature.get("properties", {}).get("NAME20") or "").strip().upper()
        for feature in counties.get("features") or []
    }

    proofs: list[dict] = []
    for district, district_blocks in blocks.groupby("district_num"):
        county_fips_values = set(district_blocks["COUNTYFP20"])
        if len(county_fips_values) != 1:
            continue
        county_fips = next(iter(county_fips_values))
        county_blocks = blocks[blocks["COUNTYFP20"] == county_fips]
        if set(district_blocks["block_geoid20"]) != set(county_blocks["block_geoid20"]):
            continue
        county_name = county_names.get(county_fips, "")
        if not county_name:
            raise ValueError(f"No county name found for FIPS {county_fips}")
        proofs.append(
            {
                "scope": "state_house",
                "district": str(district),
                "county": county_name,
                "county_fips": county_fips,
                "tabblock_year": 2020,
                "tabblocks": len(district_blocks),
                "population": int(
                    pd.to_numeric(district_blocks["POP20"], errors="coerce").fillna(0).sum()
                ),
                "crosswalk_file": path.name,
            }
        )
    return sorted(proofs, key=lambda item: int(item["district"]))


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


def apply_year(year: int, proofs: list[dict]) -> list[str]:
    changed: list[str] = []
    generated_at = datetime.now(timezone.utc).isoformat()
    for source_path in sorted(CONTESTS.glob(f"*_{year}.json")):
        contest_type = source_path.stem[: -(len(str(year)) + 1)]
        source = json.loads(source_path.read_text(encoding="utf-8"))
        output_path = DISTRICT_CONTESTS / f"state_house_{contest_type}_{year}.json"
        if not output_path.exists():
            continue
        payload = json.loads(output_path.read_text(encoding="utf-8"))
        results = payload.setdefault("general", {}).setdefault("results", {})
        meta = payload.setdefault("meta", {})
        proof_districts = {proof["district"] for proof in proofs}
        overrides = [
            item for item in meta.get("exact_coterminous_county_overrides") or []
            if str(item.get("district")) not in proof_districts
        ]
        applied = False
        for proof in proofs:
            county_rows = [
                row
                for row in source.get("rows") or []
                if str(row.get("county") or "").strip().upper().split(" - ", 1)[0]
                == proof["county"]
            ]
            if not county_rows:
                continue
            results[proof["district"]] = exact_result(county_rows, contest_type)
            overrides.append(
                {
                    **proof,
                    "source_file": source_path.name,
                    "source_rows": len(county_rows),
                    "applied_at": generated_at,
                }
            )
            applied = True
        if not applied:
            continue
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
    proofs = find_coterminous_county_districts()
    if not proofs:
        raise ValueError("No coterminous county/State House district pairs found")
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
        changed.extend(apply_year(year, proofs))
    update_manifest(changed)
    print(
        f"verified {len(proofs)} coterminous county/State House district pair(s); "
        f"updated {len(changed)} files"
    )
    for proof in proofs:
        print(
            f"  HD-{proof['district']} == {proof['county'].title()} County "
            f"({proof['tabblocks']:,} tabblocks)"
        )
    for name in changed:
        print(f"  {name}")


if __name__ == "__main__":
    main()
