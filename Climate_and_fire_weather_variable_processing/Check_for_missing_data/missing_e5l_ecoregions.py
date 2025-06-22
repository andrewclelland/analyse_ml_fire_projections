"""
Script to check whether any processing steps have been missed and, if so, fill in any gaps for the ERA5-Land variables. This can occur when batch running multiple scripts simultaneously.

Edit as necessary.
"""
import os
import pandas as pd
from calendar import monthrange
import ee

# Initialize Earth Engine
ee.Authenticate()
ee.Initialize(project='spherical-berm-323321') # <-- Edit as necessary

# Set path and band info
csv_folder = '/home/users/clelland/Model/Analysis/CMIP and FWI time series/Ecoregion CSVs' # <-- Edit as necessary
bands = ['relative_humidity', 'total_precipitation_sum', 'surface_thermal_radiation_downwards_sum',
         'surface_solar_radiation_downwards_sum', 'u_component_of_wind_10m', 'temperature_2m',
         'temperature_2m_max', 'temperature_2m_min']
column_rename = {
    "relative_humidity": "rh",
    "total_precipitation_sum": "tp",
    "surface_thermal_radiation_downwards_sum": "rlds",
    "surface_solar_radiation_downwards_sum": "rsds",
    "u_component_of_wind_10m": "wsp",
    "temperature_2m": "t2m",
    "temperature_2m_max": "mx2t",
    "temperature_2m_min": "mn2t"
}

# Load and filter ecoregions
ecoRegions = ee.FeatureCollection('RESOLVE/ECOREGIONS/2017')
biome_filter = ee.Filter.inList('BIOME_NUM', [6, 11])
realm_filter = ee.Filter.inList('REALM', ['Nearctic', 'Palearctic'])
selected_regions = ecoRegions.filter(ee.Filter.And(biome_filter, realm_filter))
region_list = selected_regions.toList(selected_regions.size())

# All expected months
expected_months = pd.date_range(start='2001-01-01', end='2023-11-01', freq='MS').strftime('%Y-%m')

for i in range(region_list.size().getInfo()):
    feature = ee.Feature(region_list.get(i))
    if i == 5:
        eco_name = 'Eastern Canadian Shield taiga'
        short_name = 'eashti'
    elif i == 19:
        eco_name = 'Northeast Siberian taiga'
        short_name = 'nesibta'
    elif i == 45:
        eco_name = 'Kalaallit Nunaat Arctic steppe'
        short_name = 'kalste'
    else:
        eco_name = feature.get('ECO_NAME').getInfo()
        words = eco_name.split()
        short_name = (words[0][:4] + words[1][:3]).lower() if len(words) >= 2 else words[0][:7].lower()
        
    csv_path = os.path.join(csv_folder, f"e5l_2001_2023_{short_name}.csv") # <-- Edit as necessary

    if not os.path.exists(csv_path):
        print(f"CSV not found for {short_name}, skipping...")
        continue

    df_existing = pd.read_csv(csv_path, parse_dates=['date'])
    df_existing['year_month'] = df_existing['date'].dt.strftime('%Y-%m')
    missing_months = sorted(set(expected_months) - set(df_existing['year_month']))

    if not missing_months:
        print(f"{short_name} is complete.")
        continue

    print(f"Filling missing months for {short_name}: {', '.join(missing_months)}")
    new_data = []

    for ym in missing_months:
        year, month = map(int, ym.split('-'))
        _, last_day = monthrange(year, month)
        file_path = f"gs://clelland_fire_ml/training_e5l_cems_mcd/cems_e5l_mcd_{year}_{month}.tif" # <-- Check permission ok

        try:
            image = ee.Image.loadGeoTIFF(file_path).clip(feature.geometry()).updateMask(
                ee.Image.loadGeoTIFF(file_path).clip(feature.geometry()).neq(-9999))
            means = {band: image.select(band).reduceRegion(
                reducer=ee.Reducer.mean(),
                geometry=feature.geometry(),
                scale=4000,
                maxPixels=1e8
            ).get(band).getInfo() for band in bands}

            means['year'] = year
            means['month'] = month

            if means['total_precipitation_sum'] is not None:
                means['total_precipitation_sum'] *= last_day * 24 * 60 * 60
            if means['surface_thermal_radiation_downwards_sum'] is not None:
                means['surface_thermal_radiation_downwards_sum'] /= (last_day * 24 * 60 * 60)
            if means['surface_solar_radiation_downwards_sum'] is not None:
                means['surface_solar_radiation_downwards_sum'] /= (last_day * 24 * 60 * 60)

            new_data.append(means)

        except Exception as e:
            print(f"Error processing {ym} for {short_name}: {e}")
            continue

    if new_data:
        df_new = pd.DataFrame(new_data)
        df_new['date'] = pd.to_datetime(df_new[['year', 'month']].assign(day=1))
        df_new.drop(columns=['year', 'month'], inplace=True)
        df_new.rename(columns=column_rename, inplace=True)
        df_combined = pd.concat([df_existing.drop(columns=['year_month']), df_new], ignore_index=True)
        df_combined.sort_values('date', inplace=True)
        df_combined.to_csv(csv_path, index=False)
        print(f"Updated CSV saved for {short_name}")
    else:
        print(f"No new data could be retrieved for {short_name}")