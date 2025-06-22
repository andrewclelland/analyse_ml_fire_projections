"""
Script to check whether any processing steps have been missed and, if so, fill in any gaps for historical NASA-downscaled fire weather data. This can occur when batch running multiple scripts simultaneously.

Edit as necessary.
"""
import os
import pandas as pd
from calendar import monthrange
import ee

# Initialize Earth Engine
ee.Authenticate()
ee.Initialize(project='spherical-berm-323321') # <-- Edit as necessary

# Load ecoregions and apply filters
ecoRegions = ee.FeatureCollection('RESOLVE/ECOREGIONS/2017')
biome_filter = ee.Filter.inList('BIOME_NUM', [6, 11])
realm_filter = ee.Filter.inList('REALM', ['Nearctic', 'Palearctic'])
selected_regions = ecoRegions.filter(ee.Filter.And(biome_filter, realm_filter))
region_list = selected_regions.toList(selected_regions.size())

# Bands to extract
bands = ['BUI', 'DC', 'DMC', 'FFMC', 'FWI', 'ISI']

# All expected months from 2001 to 2014
expected_months = pd.date_range(start='2001-01-01', end='2014-12-01', freq='MS').strftime('%Y-%m')

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

    for model in ['access', 'mri']:
        output_path = f'/home/users/clelland/Model/Analysis/CMIP and FWI time series/Ecoregion CSVs/{model}_fwi_2001_2014_{short_name}.csv' # <-- Edit as necessary
        
        if os.path.exists(output_path):
            df_existing = pd.read_csv(output_path, parse_dates=['date'])
            df_existing['year_month'] = df_existing['date'].dt.strftime('%Y-%m')
            existing_months = set(df_existing['year_month'])
        else:
            df_existing = pd.DataFrame()
            existing_months = set()

        missing_months = sorted(set(expected_months) - existing_months)
        if not missing_months:
            print(f"{model.upper()} {short_name} is already complete.")
            continue

        print(f"Filling missing months for {model.upper()} {short_name}: {', '.join(missing_months)}")

        new_data = []
        for ym in missing_months:
            year, month = map(int, ym.split('-'))
            print(f"Processing {year}-{month} for {model}")

            _, last_day = monthrange(year, month)
            file_path = f"gs://clelland_fire_ml/training_nasa_{model}_firecci/nasa_{model}_firecci_{year}_{month}.tif" # <-- Check permission

            try:
                image = ee.Image.loadGeoTIFF(file_path).clip(feature.geometry()).updateMask(
                    ee.Image.loadGeoTIFF(file_path).clip(feature.geometry()).neq(-9999)
                )

                means = {band: image.select(band).reduceRegion(
                    reducer=ee.Reducer.mean(),
                    geometry=feature.geometry(),
                    scale=4000,
                    maxPixels=1e8
                ).get(band).getInfo() for band in bands}

                means['year'] = year
                means['month'] = month
                new_data.append(means)

            except Exception as e:
                print(f"Error processing {ym} for {short_name}: {e}")
                continue

        if new_data:
            df_new = pd.DataFrame(new_data)
            df_new['date'] = pd.to_datetime(df_new[['year', 'month']].assign(day=1))
            df_new.drop(columns=['year', 'month'], inplace=True)

            if not df_existing.empty:
                df_combined = pd.concat([df_existing.drop(columns=['year_month']), df_new], ignore_index=True)
            else:
                df_combined = df_new

            df_combined.sort_values('date', inplace=True)
            df_combined.to_csv(output_path, index=False)
            print(f"Updated CSV saved for {model.upper()} {short_name}")
        else:
            print(f"No new data retrieved for {model.upper()} {short_name}")