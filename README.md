# The Lone Star Atlas

Interactive Texas election atlas focused on county, precinct, and district-level exploration from 2000 through 2024, with comparative modes for margins, winners, partisan shift, and flips.

This project is a single-page map experience (`index.html`) backed by prebuilt JSON/CSV/GeoJSON assets in `Data/`.

## What This Project Delivers

- Multi-layer Texas map navigation
  - Counties
  - U.S. Congressional districts
  - Texas House districts
  - Texas Senate districts
  - Precinct overlays and precinct centroids
- Multiple analysis modes
  - `Margins` (two-party margin buckets)
  - `Winners` (party winner by geography)
  - `Shift` (signed change vs prior comparable cycle)
  - `Flips` (party change between cycles)
- Rich focus panels and summaries
  - Live statewide vote split and competitiveness call
  - County/district hover cards and pinned detail views
  - Trend history cards across election years
- Search and navigation tooling
  - Fly-to for county/district/precinct-like tokens
  - Region quick-jumps for major Texas areas (DFW, Houston, Central TX, RGV, etc.)
- Accessibility and UX features
  - Colorblind-friendly palette toggle
  - Label toggles
  - Mobile-aware layout, safe-area handling, compact controls

## Current Branding and UI Theme

- Product name: **The Lone Star Atlas**
- Theme direction: Texas-inspired palette
  - Texas blue: `#002868`
  - Texas red: `#bf0a30`
- These colors are applied across controls, badges, map UI accents, and party-forward visual elements.

## Application Architecture

The app is intentionally consolidated into a single document:

- `index.html`
  - HTML structure for map, controls, legends, side panels, and modals
  - CSS for responsive layout, panel systems, and thematic styling
  - JavaScript for map rendering, data loading, contest transforms, interaction state, and UI synchronization

Key architectural patterns:

- Static asset loading through `CONFIG.paths` (GeoJSON, JSON, CSV)
- View-mode state machine (`counties`, `districts`, `state_house`, `state_senate`)
- Viz-mode state machine (`margins`, `winners`, `shift`, `flips`)
- Shared vote-counter and statewide summary rendering logic
- Contest/year normalization and fallback handling for legacy election slices

## Data Model Overview

The project uses three major data groups:

1. Geometry layers (map shapes / points)
2. Election result payloads (contest slices and district aggregates)
3. Reference metadata (district descriptors, manifests, county demos)

### Core geometry assets

- `Data/tl_2020_48_county20.geojson`
- `Data/tl_2020_48_vtd20.geojson`
- `Data/precinct_centroids_tx.geojson`
- `Data/tx_cd_2025.geojson`
- `Data/tx_state_house_2022.geojson`
- `Data/tx_state_senate_2022.geojson`

### Election payload directories

- `Data/contests/`
  - Statewide/precinct-normalized contest files by `{contest_type}_{year}.json`
  - Includes `manifest.json` with totals and row counts
- `Data/district_contests/`
  - District rollups by scope (`congressional`, `state_house`, `state_senate`)
  - Includes `manifest.json` with district counts and match coverage

### Raw and legacy election sources

- `Data/openelections-data-tx/`
  - Historical TX election CSVs (county/precinct depending on year/source)
- Shapefile bundles used by build scripts
  - `Data/tx_2016.zip`, `Data/tx_2018.zip`, `Data/tx_2020.zip`

### Crosswalk and supporting bundles

- `Data/nhgis_blk*.zip`
  - Used for block assignment/crosswalk weighting in district aggregation

## Contest Taxonomy (Examples)

The normalization layer maps source office names/codes into canonical contest keys such as:

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
- `court_of_criminal_appeals_*`

## Build and Data-Prep Scripts

All helper scripts are in `Scripts/` and are oriented around generating or normalizing the `Data/` assets consumed by `index.html`.

### `Scripts/convert_tx_district_shapefiles.py`

Purpose:

- Converts district TIGER/Line ZIP shapefiles into web-ready EPSG:4326 GeoJSON outputs.

Main outputs:

- `Data/tx_cd_2025.geojson`
- `Data/tx_state_house_2022.geojson`
- `Data/tx_state_senate_2022.geojson`

### `Scripts/build_contests_from_tx_shapefiles.py`

Purpose:

- Extracts contest vote fields from TX shapefile election bundles.
- Maps encoded office fields to canonical contest types.
- Produces normalized contest JSON slices and updates contest manifest records.

Main outputs:

- `Data/contests/*.json`
- `Data/contests/manifest.json`

### `Scripts/build_tx_precinct_and_district_aggregates.py`

Purpose:

- Builds precinct-level and district-level aggregates from TX election CSVs and crosswalk weights.
- Computes per-result totals, margins, winners, candidate labels, and metadata.
- Produces district-contest payloads with match coverage metrics.

Main outputs:

- `Data/contests/*.json`
- `Data/contests/manifest.json`
- `Data/district_contests/*.json`
- `Data/district_contests/manifest.json`

### `Scripts/get_tx_2000s_vtds.ps1`

Purpose:

- Downloads and prepares Texas VTD boundary archives for historical/legacy layers.
- Produces extracted and GeoJSON-converted VTD resources (including 2000s alias files when available).

## Interaction Model

The map supports two complementary workflows:

- **Breadth-first exploration**
  - Use region jumps, layer switches, and statewide panel context to scan macro patterns.
- **Detail-first investigation**
  - Hover or click a specific county/district, pin results, inspect vote split and trend cards, then compare in shift/flip modes.

This dual model makes it suitable for both narrative analysis and rapid diagnostics.

## Known Data and Modeling Notes

- District aggregate files include a `match_coverage_pct` metric; some years/scopes are below 100% due to crosswalk and source-join constraints.
- Legacy source formats vary between county-level and precinct-level records, so fallback paths and normalization heuristics are used.
- Precinct key normalization handles many formatting variants, but ambiguous county-local naming conventions can still produce partial match loss.
- Candidate naming may differ by source quality and office encoding, especially in older records.

## Repository Structure

- `index.html` — primary application UI, style, and logic
- `SCMap.html` — source design reference file
- `Data/` — geometry, election slices, manifests, and raw source bundles
- `Scripts/` — conversion and aggregation scripts
- `.venv/` — local Python environment (workspace-local)

## Credits

- U.S. Census TIGER/Line geography products
- OpenElections Texas datasets
- Mapbox GL JS for map rendering
- Turf.js and Papa Parse for geospatial/data utility in the browser

## Project Status

Active working prototype with expanded UI/analytics, Texas-specific branding/theming, and data-pipeline-backed contest assets. The codebase is optimized for iteration speed in a single-file app architecture while preserving advanced map interactions.
