"""
Script to check whether any processing steps have been missed and, if so, fill in any gaps for the future fire weather indices. This can occur when batch running multiple scripts simultaneously.

Edit as necessary.
"""
import pandas as pd
from calendar import monthrange
import ee
import os

ee.Authenticate()
ee.Initialize(project='spherical-berm-323321') # <-- Edit as necessary

# Load the ecoregions
ecoRegions = ee.FeatureCollection('RESOLVE/ECOREGIONS/2017')

# Apply filters
biome_filter = ee.Filter.inList('BIOME_NUM', [6, 11])
realm_filter = ee.Filter.inList('REALM', ['Nearctic', 'Palearctic'])
combined_filter = ee.Filter.And(biome_filter, realm_filter)
selected_regions = ecoRegions.filter(combined_filter)
region_list = selected_regions.toList(selected_regions.size())

# Bands to extract
bands = ['B0', 'B1', 'B2', 'B3', 'B4', 'B10']
other = ee.Image.loadGeoTIFF('gs://clelland_fire_ml/training_nasa_access_firecci/nasa_access_firecci_2001_1.tif').select('aspect') # <-- Check permission

folders = ['SSP126', 'SSP245', 'SSP370']
scenarios = ['ssp126', 'ssp245', 'ssp370']
models = ['access', 'mri']
models_long = ['ACCESS-CM2', 'MRI-ESM2-0']

# Loop through each region creating a short name
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

    print(f"Processing region: {eco_name} -> {short_name}")

    for model, model_long in zip(models, models_long):
        for folder, scenario in zip(folders, scenarios):
            region_data = []

            output_path = f'/home/users/clelland/Model/Analysis/CMIP and FWI time series/Ecoregion CSVs/{model}_{scenario}_fwi_2015_2100_{short_name}.csv' # <-- Edit as necessary

            # Load existing data if present
            if os.path.exists(output_path):
                existing_df = pd.read_csv(output_path, parse_dates=['date'], index_col='date')
                existing_dates = existing_df.index.strftime('%Y-%m').tolist()
                print(f"Loaded existing data with {len(existing_df)} rows for {model} {scenario}")
            else:
                existing_df = pd.DataFrame()
                existing_dates = []

            for year in range(2015, 2101):
                for month in range(1, 13):
                    date_key = f"{year}-{month:02d}"
                    if date_key in existing_dates:
                        continue

                    print(f"Processing {date_key} for {model} {scenario}")
                    _, last_day = monthrange(year, month)
                    file_path = f"gs://clelland_fire_ml/FWI_files/{model_long}_COG/{folder}/{model_long}_{scenario}_{year}_{month}_cog.tif"

                    # Load the image
                    image = ee.Image.loadGeoTIFF(file_path)

                    # Clip and mask
                    clipped_image = image.unmask(-9999, sameFootprint=True).clip(feature.geometry()).updateMask(other)
                    clipped_image = clipped_image.updateMask(clipped_image.neq(-9999))

                    # Extract band means
                    try:
                        means = {band: clipped_image.select(band).reduceRegion(
                            reducer=ee.Reducer.mean(),
                            geometry=feature.geometry(),
                            scale=4000,
                            maxPixels=1e8
                        ).get(band).getInfo() for band in bands}
                    except Exception as e:
                        print(f"Skipped {year}-{month} for {model} {scenario} {short_name}: {e}")
                        continue

                    means['year'] = year
                    means['month'] = month
                    region_data.append(means)

            # Combine and save
            if region_data:
                df = pd.DataFrame(region_data)
                df['date'] = pd.to_datetime(df[['year', 'month']].assign(day=1))
                df.set_index('date', inplace=True)
                df.drop(columns=['year', 'month'], inplace=True)
                df.rename(columns={
                    "B0": "BUI", "B1": "DC", "B2": "DMC",
                    "B3": "FFMC", "B4": "FWI", "B10": "ISI"
                }, inplace=True)

                if not existing_df.empty:
                    df = pd.concat([existing_df, df])
                    df = df[~df.index.duplicated(keep='last')]
                    df = df.sort_index()

                df.to_csv(output_path)
                print(f"Saved updated data to {output_path}")
            else:
                print(f"No new data extracted for {model} {scenario} in region: {short_name}")