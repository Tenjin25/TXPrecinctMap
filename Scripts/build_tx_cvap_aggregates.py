"""Build compact Texas atlas CVAP aggregates from RDH 2024-to-2020-block CSV.

Inputs are kept outside the deployed repository by default. Crosswalk w values
are normalized within each block so a split block is not counted twice.
"""
import argparse
import csv
import json
import zipfile
from collections import defaultdict
from pathlib import Path

FIELDS = ("CVAP_TOT24", "CVAP_HSP24", "CVAP_WHT24", "CVAP_BLA24",
          "CVAP_AMI24", "CVAP_ASI24", "CVAP_NHP24", "CVAP_2OM24")
TARGETS = {
    "cd118_2022_lines": ("block2020_to_cd_2022.csv", "district_num", "district"),
    "cd2026_2026_lines": ("block2020_to_cd_2026.csv", "district_num", "district"),
    "state_house_2022_lines": ("block2020_to_state_house_2022.csv", "district_num", "district"),
    "state_senate_2022_lines": ("block2020_to_state_senate_2022.csv", "district_num", "district"),
    "precinct_2024": ("block2020_to_vtd24.csv", "precinct_key", "precinct_id"),
}

def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--source-dir", type=Path, required=True)
    parser.add_argument("--out-dir", type=Path, default=Path("Data/cvap_aggregates"))
    args = parser.parse_args()
    source = args.source_dir
    with zipfile.ZipFile(source / "tx_cvap_2024_2020_b_csv.zip") as z:
        with z.open("tx_cvap_2024_2020_b.csv") as binary:
            import io
            rows = csv.DictReader(io.TextIOWrapper(binary, encoding="utf-8-sig"))
            if not set(FIELDS + ("GEOID20",)).issubset(rows.fieldnames or []):
                raise ValueError("Unexpected RDH CVAP block CSV schema")
            blocks = {}
            counties = defaultdict(lambda: [0] * len(FIELDS))
            for row in rows:
                geoid = row["GEOID20"].strip()
                if len(geoid) != 15 or not geoid.startswith("48"):
                    raise ValueError(f"Unexpected Texas block GEOID: {geoid}")
                values = tuple(int(row[field] or 0) for field in FIELDS)
                blocks[geoid] = values
                for i, value in enumerate(values):
                    counties[geoid[:5]][i] += value
    county_geo = json.loads((source / "tl_2020_48_county20.geojson").read_text(encoding="utf-8"))
    names = {}
    for feature in county_geo["features"]:
        props = feature["properties"]
        geoid = str(props.get("GEOID20") or props.get("GEOID") or "")
        name = str(props.get("NAME20") or props.get("NAME") or "")
        if geoid and name:
            names[geoid] = name
    args.out_dir.mkdir(parents=True, exist_ok=True)
    def write(name, header, data):
        path = args.out_dir / name
        count = 0
        with path.open("w", newline="", encoding="utf-8") as f:
            writer = csv.writer(f)
            writer.writerow(header)
            for row in data:
                writer.writerow(row)
                count += 1
        print(f"{path}: {count} rows")
    write("county_2020__cvap24.csv",
          ("county_geoid20", "county_name", *FIELDS),
          ((key, names.get(key, ""), *values) for key, values in sorted(counties.items())))
    for target, (file, key_field, out_key) in TARGETS.items():
        crosswalk = source / "crosswalks" / file
        with crosswalk.open(newline="", encoding="utf-8-sig") as f:
            rows = list(csv.DictReader(f))
        weights = defaultdict(float)
        for row in rows:
            weights[row["block_geoid20"]] += max(float(row["w"] or 0), 0)
        sums = defaultdict(lambda: [0.0] * len(FIELDS))
        matched = 0
        for row in rows:
            block = row["block_geoid20"]
            values = blocks.get(block)
            if values is None:
                continue
            weight = max(float(row["w"] or 0), 0)
            denom = weights[block]
            if denom <= 0:
                continue
            key = row[key_field].strip()
            if not key:
                continue
            if target == "precinct_2024":
                countyfp = str(int(row["COUNTYFP20"]))
                county_geoid = "48" + row["COUNTYFP20"].zfill(3)
                if not key.startswith(countyfp) or county_geoid not in names:
                    continue
                key = f"{names[county_geoid]} - {key[len(countyfp):]}"
            matched += 1
            for i, value in enumerate(values):
                sums[key][i] += value * weight / denom
        if matched == 0:
            raise ValueError(f"No CVAP blocks joined to {crosswalk}")
        write(target + "__cvap24.csv", (out_key, *FIELDS),
              ((key, *(round(v, 2) for v in sums[key]))
               for key in sorted(sums, key=lambda s: (len(s), s))))
        print(f"  matched crosswalk rows: {matched}/{len(rows)}")
    print(f"Source blocks: {len(blocks)}; counties: {len(counties)}")

if __name__ == "__main__":
    main()
