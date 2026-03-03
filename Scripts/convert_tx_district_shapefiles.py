from pathlib import Path
import geopandas as gpd


ROOT = Path(__file__).resolve().parents[1]
DATA = ROOT / "Data"

INPUT_OUTPUT = [
    (DATA / "tl_2025_48_cd119.zip", DATA / "tx_cd_2025.geojson"),
    (DATA / "tl_2022_48_sldl.zip", DATA / "tx_state_house_2022.geojson"),
    (DATA / "tl_2022_48_sldu.zip", DATA / "tx_state_senate_2022.geojson"),
]


def convert_zip_to_geojson(src_zip: Path, out_geojson: Path) -> None:
    if not src_zip.exists():
        raise FileNotFoundError(f"Missing shapefile ZIP: {src_zip}")

    gdf = gpd.read_file(f"zip://{src_zip.resolve()}")

    # Web maps expect EPSG:4326.
    if gdf.crs is None:
        gdf = gdf.set_crs("EPSG:4269", allow_override=True)
    gdf = gdf.to_crs("EPSG:4326")

    out_geojson.parent.mkdir(parents=True, exist_ok=True)
    gdf.to_file(out_geojson, driver="GeoJSON")
    print(f"Wrote {out_geojson} ({len(gdf)} rows)")


def main() -> None:
    for src_zip, out_geojson in INPUT_OUTPUT:
        convert_zip_to_geojson(src_zip, out_geojson)


if __name__ == "__main__":
    main()
