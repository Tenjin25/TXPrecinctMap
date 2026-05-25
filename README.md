# The Lone Star Atlas

The Lone Star Atlas is an interactive Texas election map that supports county, precinct, and district analysis across modern election cycles. It is designed for fast exploratory analysis, detailed vote inspection, and year-over-year comparison inside a single-page web app.

## Live Deployment

This project is published through GitHub Pages. Deployment is static and asset-based (`index.html` + `Data/`), so this README intentionally does not include local run instructions.

## What The App Does

- Renders multiple geography layers for Texas:
  - Counties
  - U.S. Congressional districts
  - Texas House districts
  - Texas Senate districts
  - Precinct overlays and precinct centroids
- Supports multiple election visualization modes:
  - `Margins`: two-party margin bucket coloring
  - `Winners`: party winner by geography
  - `Shift`: signed change versus prior comparable cycle
  - `Flips`: party change between comparable cycles
- Provides statewide and local context together:
  - Statewide vote split panel
  - Hover and pinned cards for county/district/precinct contexts
  - Trend history cards for selected areas
- Includes navigation tooling:
  - Fly-to search for county, district, and precinct-like tokens
  - Region quick jumps (DFW, Houston, Central Texas, RGV, etc.)
  - Keyboard toggles for labels and precinct overlay

## Technical Architecture

The application is intentionally consolidated in one document:

- `index.html`
  - Layout and UI structure
  - CSS styling and responsive behavior
  - Client-side data loading, transforms, and render logic

Core runtime patterns:

- Path-based static loading via `CONFIG.paths`
- View state machine (`counties`, `districts`, `state_house`, `state_senate`, optional `vtds_2000`)
- Visualization mode state machine (`margins`, `winners`, `shift`, `flips`)
- Shared vote-counter context model (statewide, hover, pinned)
- Contest/year normalization plus fallback loading from JSON and CSV sources

## Data Architecture

The app reads prebuilt assets from `Data/`.

### Geometry assets

- `Data/tl_2020_48_county20.geojson`
- `Data/tl_2020_48_vtd20.geojson`
- `Data/precinct_centroids_tx.geojson`
- `Data/tx_cd_2025.geojson`
- `Data/tx_state_house_2022.geojson`
- `Data/tx_state_senate_2022.geojson`

### Contest payloads

- `Data/contests/*.json`
  - County/statewide contest slices per `{contest_type}_{year}.json`
- `Data/contests/manifest.json`
  - Contest index used to populate selector options and totals

### District payloads

- `Data/district_contests/*.json`
  - Reallocated district-level contest slices by scope
- `Data/district_contests/manifest.json`
  - Scope/year/contest index and coverage metadata

### Raw and intermediate sources

- Root-level county CSV snapshots (for backfilling and contest regeneration)
- `Data/openelections-data-tx/` historical OpenElections files
- TIGER/Line and election shapefile ZIP bundles
- Crosswalk and block assignment inputs (NHGIS/block-based joins)

## Contest Taxonomy And Judicial Normalization

Canonical contest keys include statewide executive, federal, and judicial contests such as:

- `president`
- `us_senate`
- `governor`
- `lieutenant_governor`
- `attorney_general`
- `comptroller`
- `land_commissioner`
- `agriculture_commissioner`
- `railroad_commissioner`
- `supreme_court_place_*`
- `court_of_criminal_appeals_place_*`
- `court_of_criminal_appeals_presiding_judge`

Dropdown grouping is normalized so judicial contests appear under:

- `Supreme Court`
- `Court of Criminal Appeals`

## Data Build And Maintenance Scripts

Scripts are in `Scripts/` and are used to build or refresh map assets.

### `Scripts/convert_tx_district_shapefiles.py`

- Converts district TIGER/Line ZIP inputs into web-ready EPSG:4326 GeoJSON.
- Typical outputs:
  - `Data/tx_cd_2025.geojson`
  - `Data/tx_state_house_2022.geojson`
  - `Data/tx_state_senate_2022.geojson`

### `Scripts/build_contests_from_tx_shapefiles.py`

- Extracts contest fields from shapefile election bundles.
- Maps source office encodings to canonical contest keys.
- Updates county/statewide contest slices and contest manifest.

### `Scripts/build_county_contests_from_csv.py`

- Builds county contest JSON slices from county-level CSV files.
- Updates or inserts manifest entries for each regenerated contest/year.
- Used for backfilling years where source CSVs are the most complete source.

### `Scripts/build_tx_precinct_and_district_aggregates.py`

- Builds precinct and district aggregates from contest sources and crosswalks.
- Computes totals, margins, winners, candidate labels, and coverage stats.
- Writes `Data/contests` and `Data/district_contests` outputs.

### `Scripts/get_tx_2000s_vtds.ps1`

- Fetches and prepares historical VTD boundary resources.
- Supports legacy boundary workflows and 2000s alias handling.

## Repository Layout

- `index.html`: production app UI and runtime logic
- `SCMap.html`: design/reference variant
- `Data/`: geometry, contest slices, manifests, raw bundles, and intermediate artifacts
- `Scripts/`: data prep and normalization scripts

## Interaction Model

The map supports two complementary analyst workflows:

- Breadth-first scanning:
  - jump regions, switch layers, compare statewide balance quickly
- Detail-first drilldown:
  - pin county/district context, inspect vote split, and review trend history

This supports both narrative election analysis and technical diagnostics.

## Known Constraints

- District reallocation quality depends on crosswalk coverage and source key consistency.
- Legacy source formats vary by year (county-level versus precinct-level granularity).
- Precinct naming conventions vary by county and can cause partial match loss in edge cases.
- Candidate labels vary by source quality; modern rendering applies case normalization for readability.

## Credits

- U.S. Census TIGER/Line geography
- OpenElections Texas data
- Mapbox GL JS
- Turf.js
- Papa Parse

## Status

Active project with ongoing data backfills, contest normalization improvements, and UI iteration for county/district/precinct analysis.
