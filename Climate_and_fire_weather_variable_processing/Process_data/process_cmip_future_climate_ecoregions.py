"""
Script to process future climate variables from the NASA-downscaled CMIP6 data, stored in Google Cloud Storage buckets from the model training, to csv files for each ecoregion using Google Earth Engine.

Edit as necessary.
"""
import pandas as pd
from calendar import monthrange
import ee
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
bands = ['B0', 'B2', 'B3', 'B4', 'B5', 'B6', 'B7', 'B8']
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

    # List to store results for this region
    region_data = []

    for model, model_long in zip(models, models_long):
        for folder, scenario in zip(folders, scenarios):
            for year in range(2015, 2101):  # From 2015 to 2101
                for month in range(1, 13):
                    print(f"Iterating over {year}-{month} for {model} {scenario}")
        
                    _, last_day = monthrange(year, month)
                    file_path = f"gs://clelland_fire_ml/CMIP6_files/{model_long}_COG/{folder}/{model_long}_{scenario}_{year}_{month}_all_cog.tif"
        
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
        
                    # Postprocess values
                    if means['B2'] is not None:
                        means['B2'] *= last_day * 24 * 60 * 60 * 0.001
        
                    region_data.append(means)
        
            # Create DataFrame for the region
            df = pd.DataFrame(region_data)
            if not df.empty:
                df['date'] = pd.to_datetime(df[['year', 'month']].assign(day=1))
                df.set_index('date', inplace=True)
                df.drop(columns=['year', 'month'], inplace=True)
                df.rename(columns={"B0": "rh", "B2": "tp", "B3": "rlds", "B4": "rsds", "B5": "wsp", "B6": "t2m", "B7": "mx2t", "B8": "mn2t"}, inplace=True)
        
                # Save to CSV
                output_path = f'/home/users/clelland/Model/Analysis/CMIP and FWI time series/Ecoregion CSVs/{model}_{scenario}_climate_2015_2100_{short_name}.csv' # <-- Edit as necessary
                df.to_csv(output_path)
            else:
                print(f"No data extracted for region: {short_name}")