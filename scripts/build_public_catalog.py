import icechunk
from basal import IcechunkCatalog
from basal.storage import repo_config_from_virtual_chunks

# ---------------------------------------------------------------------------
# Catalog storage — writers need credentials; readers get anon access via CDN
# ---------------------------------------------------------------------------

catalog_storage = icechunk.s3_storage(
    bucket="carbonplan-share",
    prefix="basal/public_icechunk_catalog",
    region="us-west-2",
    from_env=True,
)

catalog = IcechunkCatalog.open_or_create(catalog_storage)


# ---------------------------------------------------------------------------
# Storage helpers — return icechunk.Storage objects.
# location and storage_config are auto-derived by basal at registration time.
# ---------------------------------------------------------------------------


def _dynamical(bucket: str, prefix: str) -> icechunk.Storage:
    """Dynamical.org stores: us-west-2, anonymous S3."""
    return icechunk.s3_storage(
        bucket=bucket, prefix=prefix, region="us-west-2", anonymous=True
    )


def _icechunk_public(prefix: str) -> icechunk.Storage:
    """icechunk-public-data bucket: us-east-1, anonymous S3."""
    return icechunk.s3_storage(
        bucket="icechunk-public-data", prefix=prefix, region="us-east-1", anonymous=True
    )


def _source_coop(bucket: str, prefix: str) -> icechunk.Storage:
    """Source Cooperative public stores: anonymous S3, us-west-2."""
    return icechunk.s3_storage(
        bucket=bucket, prefix=prefix, region="us-west-2", anonymous=True
    )


def _carbonplan_virtual(prefix: str) -> icechunk.Storage:
    """carbonplan-share virtual icechunk store (from_env write, anon chunk read).

    storage_config is passed explicitly so consumers can reconstruct storage
    with anonymous=True (the auto-derived config would omit credential info).
    """
    return icechunk.s3_storage(
        bucket="carbonplan-share", prefix=prefix, region="us-west-2", from_env=True
    )


_CARBONPLAN_VC_STORAGE_CONFIG = {
    "type": "s3",
    "bucket": "carbonplan-share",
    "prefix": "basal/examples/virtual_icechunk",
    "region": "us-west-2",
    "anonymous": True,
}

_CARBONPLAN_VC_CONFIG = repo_config_from_virtual_chunks(
    [{"url_prefix": "s3://carbonplan-share/", "region": "us-west-2", "anonymous": True}]
)


# ---------------------------------------------------------------------------
# Registry — each entry has an explicit storage object.
# location overrides the auto-derived URL (preserves trailing slash convention).
# owner and other fields are passed as **metadata kwargs.
# ---------------------------------------------------------------------------

# Approximate bboxes [west, south, east, north] in WGS84.
# Passed explicitly so registration never reads coordinate arrays from large stores.
_GLOBAL_BBOX = [-180.0, -90.0, 180.0, 90.0]
_CONUS_BBOX = [-130.0, 20.0, -60.0, 55.0]
_EUROPE_BBOX = [-23.5, 29.5, 62.5, 70.5]

registry = [
    {
        "name": "noaa-gfs-analysis",
        "storage": _dynamical(
            "dynamical-noaa-gfs",
            "noaa-gfs-analysis/v0.1.0.icechunk",
        ),
        "location": "s3://dynamical-noaa-gfs/noaa-gfs-analysis/v0.1.0.icechunk/",
        "owner": "dynamical.org",
        "title": "NOAA GFS Analysis",
        "description": (
            "Best-estimate of past weather from NOAA's Global Forecast System, "
            "produced by concatenating the first 6 hours of each forecast run. "
            "0.25° (~20km) globally from May 2021 to present."
        ),
        "variables": [
            "temperature_2m",
            "temperature_2m_max",
            "temperature_2m_min",
            "u_wind_10m",
            "v_wind_10m",
            "u_wind_100m",
            "v_wind_100m",
            "precipitation_surface",
            "precipitation_categorical_rain",
            "precipitation_categorical_snow",
            "precipitation_categorical_freezing_rain",
            "precipitation_categorical_ice_pellets",
            "relative_humidity_2m",
            "cloud_cover",
            "pressure_msl",
            "geopotential_height_500hPa",
            "precipitable_water",
            "radiation_shortwave_downward",
            "radiation_longwave_downward",
        ],
        "spatial_resolution": "0.25 degrees (~20km)",
        "domain": "Global",
        "bbox": _GLOBAL_BBOX,
        "start_datetime": "2021-05-01",
        "temporal_coverage": "2021-05-01 to present",
        "update_frequency": "Hourly",
        "license": "CC-BY-4.0",
        "source": "NOAA NOMADS and AWS Open Data Registry",
        "tags": ["global", "analysis", "noaa", "gfs", "0.25deg"],
    },
    {
        "name": "noaa-gfs-forecast",
        "storage": _dynamical(
            "dynamical-noaa-gfs",
            "noaa-gfs-forecast/v0.2.7.icechunk",
        ),
        "location": "s3://dynamical-noaa-gfs/noaa-gfs-forecast/v0.2.7.icechunk/",
        "owner": "dynamical.org",
        "title": "NOAA GFS 16-Day Forecast",
        "description": (
            "Global 0.25° weather forecasts from NOAA's Global Forecast System. "
            "Initialized every 6 hours, covers 0-384 hours (16 days)."
        ),
        "variables": [
            "temperature_2m",
            "u_wind_10m",
            "v_wind_10m",
            "u_wind_100m",
            "v_wind_100m",
            "precipitation_surface",
            "relative_humidity_2m",
            "cloud_cover",
            "pressure_msl",
            "geopotential_height_500hPa",
            "precipitable_water",
            "radiation_shortwave_downward",
            "radiation_longwave_downward",
        ],
        "spatial_resolution": "0.25 degrees (~20km)",
        "domain": "Global",
        "bbox": _GLOBAL_BBOX,
        "start_datetime": "2021-05-01",
        "temporal_coverage": "2021-05-01 to present",
        "update_frequency": "Every 6 hours (0-384h horizon)",
        "license": "CC-BY-4.0",
        "source": "NOAA NOMADS and AWS Open Data Registry",
        "tags": ["global", "forecast", "noaa", "gfs", "0.25deg", "16-day"],
    },
    {
        "name": "noaa-gefs-analysis",
        "storage": _dynamical(
            "dynamical-noaa-gefs",
            "noaa-gefs-analysis/v0.1.2.icechunk",
        ),
        "location": "s3://dynamical-noaa-gefs/noaa-gefs-analysis/v0.1.2.icechunk/",
        "owner": "dynamical.org",
        "title": "NOAA GEFS Analysis",
        "description": (
            "Best-estimate of past weather from NOAA's Global Ensemble Forecast System. "
            "Concatenates first few hours of each historical GEFS forecast. "
            "Combines reforecast (2000-2019) and operational (2020-present) archives at 0.25° / 3-hourly."
        ),
        "variables": [
            "temperature_2m",
            "maximum_temperature_2m",
            "minimum_temperature_2m",
            "wind_u_10m",
            "wind_v_10m",
            "wind_u_100m",
            "wind_v_100m",
            "precipitation_surface",
            "categorical_rain_surface",
            "categorical_snow_surface",
            "categorical_freezing_rain_surface",
            "categorical_ice_pellets_surface",
            "percent_frozen_precipitation_surface",
            "relative_humidity_2m",
            "pressure_surface",
            "pressure_reduced_to_mean_sea_level",
            "geopotential_height_500hpa",
            "geopotential_height_cloud_ceiling",
            "precipitable_water_atmosphere",
            "total_cloud_cover_atmosphere",
            "downward_short_wave_radiation_flux_surface",
            "downward_long_wave_radiation_flux_surface",
        ],
        "spatial_resolution": "0.25 degrees (~20km)",
        "domain": "Global",
        "bbox": _GLOBAL_BBOX,
        "start_datetime": "2000-01-01",
        "temporal_coverage": "2000-01-01 to present",
        "update_frequency": "3-hourly",
        "license": "CC-BY-4.0",
        "doi": "10.5281/zenodo.18777399",
        "source": "NOAA Open Data Dissemination via AWS (reforecast + operational archives)",
        "tags": [
            "global",
            "analysis",
            "noaa",
            "gefs",
            "ensemble",
            "0.25deg",
            "2000-present",
        ],
    },
    {
        "name": "gefs-forecast-35d",
        "storage": _dynamical(
            "dynamical-noaa-gefs",
            "noaa-gefs-forecast-35-day/v0.2.0.icechunk",
        ),
        "location": "s3://dynamical-noaa-gefs/noaa-gefs-forecast-35-day/v0.2.0.icechunk/",
        "owner": "dynamical.org",
        "title": "NOAA GEFS 35-Day Extended Forecast",
        "description": (
            "Extended-range Global Ensemble Forecast System: 0.25° for 0-240h, "
            "0.5° for 243-840h. Sub-seasonal range with full ensemble."
        ),
        "variables": [
            "precipitation_surface",
            "temperature_2m",
            "pressure_msl",
            "relative_humidity_2m",
        ],
        "spatial_resolution": "0.25° (0-240h) / 0.5° (240-840h)",
        "domain": "Global",
        "bbox": _GLOBAL_BBOX,
        "update_frequency": "Daily",
        "license": "Public Domain",
        "source": "NOAA via AWS Open Data",
        "tags": ["global", "sub-seasonal", "noaa", "gefs", "ensemble", "35-day"],
    },
    {
        "name": "noaa-hrrr-analysis",
        "storage": _dynamical(
            "dynamical-noaa-hrrr",
            "noaa-hrrr-analysis/v0.2.0.icechunk",
        ),
        "location": "s3://dynamical-noaa-hrrr/noaa-hrrr-analysis/v0.2.0.icechunk/",
        "owner": "dynamical.org",
        "title": "NOAA HRRR Analysis",
        "description": (
            "Best-estimate of past weather from NOAA's High-Resolution Rapid Refresh "
            "at 3km over CONUS. First hour of each hourly run. Assimilates radar data."
        ),
        "variables": [
            "temperature_2m",
            "dewpoint_2m",
            "u_wind_10m",
            "v_wind_10m",
            "u_wind_80m",
            "v_wind_80m",
            "wind_gust_surface",
            "precipitation_surface",
            "convective_available_potential_energy_surface",
            "convective_inhibition_surface",
            "reflectivity_1km",
            "cloud_cover",
            "cloud_cover_low",
            "cloud_cover_mid",
            "cloud_cover_high",
            "visibility_surface",
            "pressure_msl",
            "pressure_surface",
            "relative_humidity_2m",
            "radiation_shortwave_downward",
            "radiation_longwave_downward",
            "soil_temperature_0_10cm",
            "soil_moisture_0_10cm",
            "snow_depth",
            "snowfall_surface",
        ],
        "spatial_resolution": "3 km",
        "domain": "CONUS",
        "bbox": _CONUS_BBOX,
        "start_datetime": "2014-10-01",
        "temporal_coverage": "2014-10-01 to present",
        "update_frequency": "Hourly",
        "license": "CC-BY-4.0",
        "source": "NOAA Open Data Dissemination via AWS",
        "tags": [
            "conus",
            "high-res",
            "3km",
            "hourly",
            "noaa",
            "hrrr",
            "analysis",
            "radar",
        ],
    },
    {
        "name": "noaa-hrrr-forecast",
        "storage": _dynamical(
            "dynamical-noaa-hrrr",
            "noaa-hrrr-forecast-48-hour/v0.1.0.icechunk",
        ),
        "location": "s3://dynamical-noaa-hrrr/noaa-hrrr-forecast-48-hour/v0.1.0.icechunk/",
        "owner": "dynamical.org",
        "title": "NOAA HRRR 48-Hour Forecast",
        "description": (
            "HRRR 48-hour forecasts at 3km over CONUS. Initialized hourly. "
            "Assimilates radar data. Best for storm-scale and short-range convective prediction."
        ),
        "variables": [
            "temperature_2m",
            "dewpoint_2m",
            "u_wind_10m",
            "v_wind_10m",
            "u_wind_80m",
            "v_wind_80m",
            "wind_gust_surface",
            "precipitation_surface",
            "convective_available_potential_energy_surface",
            "convective_inhibition_surface",
            "lifted_index",
            "reflectivity_1km",
            "cloud_cover",
            "cloud_cover_low",
            "cloud_cover_mid",
            "cloud_cover_high",
            "visibility_surface",
            "pressure_msl",
            "pressure_surface",
            "relative_humidity_2m",
            "radiation_shortwave_downward",
            "radiation_longwave_downward",
            "soil_temperature_0_10cm",
            "soil_moisture_0_10cm",
            "snow_depth",
            "snowfall_surface",
        ],
        "spatial_resolution": "3 km",
        "domain": "CONUS",
        "bbox": _CONUS_BBOX,
        "start_datetime": "2018-07-13",
        "temporal_coverage": "2018-07-13 to present",
        "update_frequency": "Hourly (initialized every hour, 48-hour horizon)",
        "license": "CC-BY-4.0",
        "doi": "10.1175/MWR-D-15-0242.1",
        "source": "NOAA Open Data Dissemination via AWS",
        "tags": [
            "conus",
            "high-res",
            "3km",
            "hourly",
            "noaa",
            "hrrr",
            "forecast",
            "radar",
        ],
    },
    {
        "name": "noaa-mrms-hourly",
        "storage": _dynamical(
            "dynamical-noaa-mrms",
            "noaa-mrms-conus-analysis-hourly/v0.3.0.icechunk",
        ),
        "location": "s3://dynamical-noaa-mrms/noaa-mrms-conus-analysis-hourly/v0.3.0.icechunk/",
        "owner": "dynamical.org",
        "title": "NOAA MRMS CONUS Hourly Analysis",
        "description": (
            "Multi-Radar Multi-Sensor (MRMS) hourly precipitation analysis at 1km (0.01°) "
            "over CONUS. Merges radar mosaics with surface observations and model data."
        ),
        "variables": ["precipitation_surface", "radar_quality_index"],
        "spatial_resolution": "0.01 degrees (~1km)",
        "domain": "CONUS",
        "bbox": _CONUS_BBOX,
        "start_datetime": "2014-10-01",
        "temporal_coverage": "2014-10-01 to present",
        "update_frequency": "Hourly",
        "license": "CC-BY-4.0",
        "doi": "10.1175/2009JAMC1893.1",
        "source": "NOAA via AWS Open Data",
        "tags": ["conus", "radar", "precipitation", "mrms", "1km", "analysis"],
    },
    {
        "name": "ecmwf-aifs-single",
        "storage": _dynamical(
            "dynamical-ecmwf-aifs-single",
            "ecmwf-aifs-single-forecast/v0.1.0.icechunk",
        ),
        "location": "s3://dynamical-ecmwf-aifs-single/ecmwf-aifs-single-forecast/v0.1.0.icechunk/",
        "owner": "dynamical.org",
        "title": "ECMWF AIFS Single Forecast",
        "description": (
            "ECMWF Artificial Intelligence Forecasting System (AIFS) deterministic forecasts "
            "at 0.25° globally. AI-based model trained on ERA5. Initialized twice daily, 10-day horizon."
        ),
        "variables": ["t2m", "u10", "v10", "tp", "msl", "z", "q", "u", "v", "t", "r"],
        "spatial_resolution": "0.25 degrees (~20km)",
        "domain": "Global",
        "bbox": _GLOBAL_BBOX,
        "start_datetime": "2024-06-01",
        "temporal_coverage": "2024-06-01 to present",
        "update_frequency": "Twice daily (00Z, 12Z), 10-day horizon",
        "license": "CC-BY-4.0",
        "source": "ECMWF via dynamical.org",
        "tags": ["global", "ai", "ecmwf", "machine-learning", "forecast", "0.25deg"],
    },
    {
        "name": "ecmwf-ifs-ens",
        "storage": _dynamical(
            "dynamical-ecmwf-ifs-ens",
            "ecmwf-ifs-ens-forecast-15-day-0-25-degree/v0.1.0.icechunk",
        ),
        "location": "s3://dynamical-ecmwf-ifs-ens/ecmwf-ifs-ens-forecast-15-day-0-25-degree/v0.1.0.icechunk/",
        "owner": "dynamical.org",
        "title": "ECMWF IFS Ensemble 15-Day Forecast",
        "description": (
            "Global ensemble weather forecasts from ECMWF's Integrated Forecasting System. "
            "51 members (1 control + 50 perturbed) at 0.25° out to 15 days."
        ),
        "variables": [
            "tp",
            "t2m",
            "msl",
            "gh_500hPa",
            "gh_850hPa",
            "u10",
            "v10",
            "u100",
            "v100",
            "t_850hPa",
            "t_500hPa",
            "u_850hPa",
            "v_850hPa",
            "u_500hPa",
            "v_500hPa",
            "q_850hPa",
            "q_500hPa",
            "r_850hPa",
            "r_500hPa",
        ],
        "spatial_resolution": "0.25 degrees (~20km)",
        "domain": "Global",
        "bbox": _GLOBAL_BBOX,
        "start_datetime": "2017-01-01",
        "temporal_coverage": "2017-01-01 to present",
        "update_frequency": "Twice daily (00Z, 12Z), 15-day horizon",
        "license": "CC-BY-4.0",
        "source": "ECMWF via dynamical.org",
        "tags": ["global", "ensemble", "ecmwf", "probabilistic", "15-day", "51-member"],
    },
    {
        "name": "dwd-icon-eu",
        "storage": _dynamical(
            "dynamical-dwd-icon-eu",
            "dwd-icon-eu-forecast-5-day/v0.2.0.icechunk",
        ),
        "location": "s3://dynamical-dwd-icon-eu/dwd-icon-eu-forecast-5-day/v0.2.0.icechunk/",
        "owner": "dynamical.org",
        "title": "DWD ICON-EU 5-Day Forecast",
        "description": (
            "DWD ICON-EU regional model: 5-day forecasts at 0.0625° (~7km) over Europe. "
            "Initialized every 3 hours with 120-hour horizon."
        ),
        "variables": [
            "temperature_2m",
            "dewpoint_2m",
            "u_wind_10m",
            "v_wind_10m",
            "wind_gust_surface",
            "precipitation_surface",
            "convective_available_potential_energy_surface",
            "cloud_cover",
            "cloud_cover_low",
            "cloud_cover_mid",
            "cloud_cover_high",
            "pressure_msl",
            "relative_humidity_2m",
            "radiation_shortwave_downward",
            "radiation_longwave_downward",
            "snow_depth",
            "visibility_surface",
            "geopotential_height_500hPa",
            "temperature_850hPa",
        ],
        "spatial_resolution": "0.0625 degrees (~7km)",
        "domain": "Europe",
        "bbox": _EUROPE_BBOX,
        "start_datetime": "2021-01-01",
        "temporal_coverage": "2021-01-01 to present",
        "update_frequency": "Every 3 hours (0-120h horizon)",
        "license": "CC-BY-4.0",
        "doi": "10.5676/DWD_pub/nwv/icon_011",
        "source": "DWD Open Data via dynamical.org",
        "tags": ["europe", "regional", "high-res", "7km", "dwd", "icon", "forecast"],
    },
    {
        "name": "glad-land-cover",
        "storage": _icechunk_public("v1/glad"),
        "location": "s3://icechunk-public-data/v1/glad",
        "owner": "glad",
        "title": "GLAD Land Cover and Land Use",
        "description": (
            "Global Land Analysis & Discovery (GLAD) annual land cover and land use classification. "
            "Native Icechunk v2 store on AWS."
        ),
        "variables": ["land_cover", "land_use"],
        "domain": "Global",
        "bbox": _GLOBAL_BBOX,
        "license": "CC-BY-4.0",
        "source": "GLAD / University of Maryland via icechunk-public-data (us-east-1)",
        "tags": [
            "global",
            "land-cover",
            "land-use",
            "annual",
            "glad",
            "classification",
        ],
    },
    {
        "name": "era5-weatherbench2",
        "storage": _icechunk_public("v1/era5_weatherbench2"),
        "location": "s3://icechunk-public-data/v1/era5_weatherbench2",
        "owner": "google-research",
        "title": "WeatherBench2 ERA5 (subset)",
        "description": (
            "Subset of the WeatherBench2 copy of the ERA5 reanalysis dataset stored as native "
            "Icechunk v2. Open via group '1x721x1440' for full-resolution data."
        ),
        "variables": [
            "geopotential",
            "temperature",
            "u_component_of_wind",
            "v_component_of_wind",
            "specific_humidity",
            "2m_temperature",
            "10m_u_wind",
            "10m_v_wind",
            "mean_sea_level_pressure",
            "total_precipitation",
        ],
        "spatial_resolution": "0.25 degrees (~20km)",
        "domain": "Global",
        "bbox": _GLOBAL_BBOX,
        "start_datetime": "1959-01-01",
        "end_datetime": "2023-12-31",
        "temporal_coverage": "1959-01-01 to 2023-12-31",
        "license": "CC-BY-4.0",
        "source": "ECMWF ERA5 via WeatherBench2 / Google Research (icechunk-public-data, us-east-1)",
        "tags": [
            "global",
            "reanalysis",
            "era5",
            "weatherbench2",
            "ecmwf",
            "0.25deg",
            "benchmark",
        ],
    },
    {
        "name": "carbonplan-nohrsc-snowfall",
        "storage": _carbonplan_virtual("basal/examples/virtual_icechunk"),
        "storage_config": _CARBONPLAN_VC_STORAGE_CONFIG,
        "config": _CARBONPLAN_VC_CONFIG,
        "location": "s3://carbonplan-share/basal/examples/virtual_icechunk",
        "owner": "carbonplan",
        "title": "NOHRSC National Snowfall Analysis (VirtualZarr example)",
        "description": (
            "Seasonal snowfall accumulation from NOAA's National Snowfall Analysis v2.1 "
            "over CONUS at 0.04° (~4km). Stored as a VirtualZarr icechunk store — "
            "virtual chunks reference the source NetCDF files on S3. Demonstrates "
            "basal's virtual dataset support."
        ),
        "variables": ["snowfall_accumulation"],
        "spatial_resolution": "0.04 degrees (~4km)",
        "domain": "CONUS",
        "bbox": _CONUS_BBOX,
        "license": "Public Domain",
        "source": "NOAA National Snowfall Analysis via carbonplan-share",
        "tags": [
            "conus",
            "snow",
            "virtual",
            "virtualizarr",
            "nohrsc",
            "carbonplan",
            "example",
        ],
    },
    {
        "name": "carbonplan-ocr-fire-risk",
        "storage": _source_coop(
            "us-west-2.opendata.source.coop",
            "carbonplan/carbonplan-ocr/output/fire-risk/tensor/production/v1.1.0/ocr.icechunk",
        ),
        "location": "s3://us-west-2.opendata.source.coop/carbonplan/carbonplan-ocr/output/fire-risk/tensor/production/v1.1.0/ocr.icechunk",
        "owner": "carbonplan",
        "title": "Open Climate Risk: Wildfire Risk (CONUS)",
        "description": (
            "CarbonPlan Open Climate Risk (OCR) wildfire risk tensor output at building-level "
            "resolution across CONUS. Produced by a distributed pipeline integrating USFS fire "
            "risk models (Riley et al. 2025, Scott et al. 2024, Dillon et al. 2023) and "
            "CONUS404 fire weather indices. Stored as Icechunk on Source Cooperative."
        ),
        "variables": ["fire_risk"],
        "spatial_resolution": "Building-level (~30m raster inputs)",
        "domain": "CONUS",
        "bbox": _CONUS_BBOX,
        "license": "CC-BY-4.0",
        "source": "CarbonPlan via Source Cooperative (source.coop/carbonplan/carbonplan-ocr)",
        "tags": [
            "conus",
            "fire",
            "wildfire",
            "climate-risk",
            "building-level",
            "carbonplan",
            "ocr",
        ],
    },
]


# ---------------------------------------------------------------------------
# Registration loop
# ---------------------------------------------------------------------------

for ds in registry:
    name = ds.pop("name")
    storage = ds.pop("storage")
    storage_config = ds.pop("storage_config", None)
    config = ds.pop("config", None)
    derive_extent = ds.pop("derive_extent", False)

    try:
        catalog.register(
            name=name,
            storage=storage,
            storage_config=storage_config,
            config=config,
            derive_extent=derive_extent,
            **ds,
        )
        print(f"registered  {name}")
    except ValueError as e:
        if "already registered" in str(e):
            catalog.update(name, **ds)
            print(f"updated     {name}")
        else:
            raise
