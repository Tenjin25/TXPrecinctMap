param(
  [string]$DataDir = (Join-Path $PSScriptRoot "..\Data"),
  [switch]$SkipExtract,
  [switch]$SkipGeoJson
)

$ErrorActionPreference = "Stop"

function Write-Step([string]$Message) {
  Write-Host "[vtd-fetch] $Message"
}

function Download-FirstAvailable {
  param(
    [string[]]$Urls,
    [string]$OutFile,
    [string]$Label
  )

  foreach ($url in $Urls) {
    try {
      Write-Step "Trying $Label from $url"
      Invoke-WebRequest -Uri $url -OutFile $OutFile
      Write-Step "Downloaded $Label -> $OutFile"
      return $true
    } catch {
      Write-Step "Failed $url ($($_.Exception.Message))"
    }
  }
  return $false
}

$archiveDir = Join-Path $DataDir "archives\vtd"
$extractDir = Join-Path $DataDir "vtd"
New-Item -ItemType Directory -Path $archiveDir -Force | Out-Null
New-Item -ItemType Directory -Path $extractDir -Force | Out-Null

$datasets = @(
  @{
    Name = "tl_2012_48_vtd10"
    ZipName = "tl_2012_48_vtd10.zip"
    Required = $true
    Urls = @(
      "https://www2.census.gov/geo/tiger/TIGER2012/VTD/tl_2012_48_vtd10.zip",
      "https://www2.census.gov/geo/tiger/TIGER2010/VTD/2010/tl_2010_48_vtd10.zip",
      "https://www2.census.gov/geo/tiger/TIGER2010/VTD/tl_2010_48_vtd10.zip"
    )
  },
  @{
    Name = "tl_2020_48_vtd20"
    ZipName = "tl_2020_48_vtd20.zip"
    Required = $true
    Urls = @(
      "https://www2.census.gov/geo/tiger/TIGER2020/VTD/tl_2020_48_vtd20.zip"
    )
  },
  @{
    Name = "tl_2000_48_vtd00"
    ZipName = "tl_2000_48_vtd00.zip"
    Required = $false
    Urls = @(
      "https://www2.census.gov/geo/tiger/TIGER2000/VTD/tl_2000_48_vtd00.zip",
      "https://www2.census.gov/geo/tiger/TIGER2000/PL/tl_2000_48_vtd00.zip"
    )
  }
)

foreach ($dataset in $datasets) {
  $zipPath = Join-Path $archiveDir $dataset.ZipName
  if (-not (Test-Path $zipPath)) {
    $ok = Download-FirstAvailable -Urls $dataset.Urls -OutFile $zipPath -Label $dataset.Name
    if (-not $ok) {
      if ($dataset.Required) {
        throw "Unable to download required dataset: $($dataset.Name)"
      }
      Write-Step "Optional dataset unavailable: $($dataset.Name)"
      continue
    }
  } else {
    Write-Step "Using cached archive $zipPath"
  }

  if (-not $SkipExtract) {
    $outDir = Join-Path $extractDir $dataset.Name
    New-Item -ItemType Directory -Path $outDir -Force | Out-Null
    Expand-Archive -Path $zipPath -DestinationPath $outDir -Force
    Write-Step "Extracted $($dataset.Name) -> $outDir"
  }
}

if ($SkipGeoJson) {
  Write-Step "Skipping GeoJSON conversion by request."
  exit 0
}

$ogr = Get-Command ogr2ogr -ErrorAction SilentlyContinue
if (-not $ogr) {
  Write-Step "ogr2ogr was not found. Archives were downloaded/extracted, but GeoJSON outputs were not created."
  Write-Step "Install GDAL and rerun without -SkipGeoJson."
  exit 0
}

function Convert-FirstShapefile {
  param(
    [string]$SearchDir,
    [string]$OutGeoJson
  )

  if (-not (Test-Path $SearchDir)) { return $false }
  $shp = Get-ChildItem -Path $SearchDir -Recurse -Filter *.shp -File | Select-Object -First 1
  if (-not $shp) { return $false }

  & $ogr.Path -f GeoJSON $OutGeoJson $shp.FullName
  Write-Step "Created $OutGeoJson"
  return $true
}

$vtd10Extract = Join-Path $extractDir "tl_2012_48_vtd10"
$vtd20Extract = Join-Path $extractDir "tl_2020_48_vtd20"
$vtd00Extract = Join-Path $extractDir "tl_2000_48_vtd00"

$vtd10Out = Join-Path $DataDir "tl_2012_48_vtd10.geojson"
$vtd20Out = Join-Path $DataDir "tl_2020_48_vtd20.geojson"
$vtd2000sOut = Join-Path $DataDir "vtd_2000s_tx.geojson"
$vtd00Out = Join-Path $DataDir "tl_2000_48_vtd00.geojson"

if (Convert-FirstShapefile -SearchDir $vtd10Extract -OutGeoJson $vtd10Out) {
  Copy-Item -Path $vtd10Out -Destination $vtd2000sOut -Force
  Write-Step "Created $vtd2000sOut (alias for 2000s-era VTD map layer)"
}

[void](Convert-FirstShapefile -SearchDir $vtd20Extract -OutGeoJson $vtd20Out)
[void](Convert-FirstShapefile -SearchDir $vtd00Extract -OutGeoJson $vtd00Out)

Write-Step "Done."
