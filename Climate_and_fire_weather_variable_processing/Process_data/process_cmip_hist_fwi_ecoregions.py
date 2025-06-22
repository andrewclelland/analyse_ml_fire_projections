"""
Script to process historic fire weather variables from the NASA-downscaled CMIP6 data, stored in Google Cloud Storage buckets from the model training, to csv files for each ecoregion using Google Earth Engine.

Edit as necessary.
"""
import pandas as pd
from calendar import monthrange
import ee
ee.Authenticate()
ee.Initialize(project='spherical-berm-323321') # <-- Edit as necessary.

# Load the ecoregions
ecoRegions = ee.FeatureCollection('RESOLVE/ECOREGIONS/2017')

# Apply filters
biome_filter = ee.Filter.inList('BIOME_NUM', [6, 11])
realm_filter = ee.Filter.inList('REALM', ['Nearctic', 'Palearctic'])
combined_filter = ee.Filter.And(biome_filter, realm_filter)
selected_regions = ecoRegions.filter(combined_filter)
region_list = selected_regions.toList(selected_regions.size())

# Bands to extract
bands = ['BUI', 'DC', 'DMC', 'FFMC', 'FWI', 'ISI']

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

    # List to store results for this region
    region_data = []

    for model in ['access', 'mri']:
        for year in range(2001, 2015):  # From 2001 to 2014
            for month in range(1, 13):
                print(f"Iterating over {year}-{month} for {model}")
    
                _, last_day = monthrange(year, month)
                file_path = f"gs://clelland_fire_ml/training_nasa_{model}_firecci/nasa_{model}_firecci_{year}_{month}.tif" # <-- Check permission (ask Trevor)
    
                # Load the image
                image = ee.Image.loadGeoTIFF(file_path)
    
                # Clip and mask
                clipped_image = image.clip(feature.geometry())
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
                    print(f"Skipped {year}-{month} for {short_name}: {e}")
                    continue
    
                means['year'] = year
                means['month'] = month
    
                region_data.append(means)
    
        # Create DataFrame for the region
        df = pd.DataFrame(region_data)
        if not df.empty:
            df['date'] = pd.to_datetime(df[['year', 'month']].assign(day=1))
            df.set_index('date', inplace=True)
            df.drop(columns=['year', 'month'], inplace=True)
    
            # Save to CSV
            output_path = f'/home/users/clelland/Model/Analysis/CMIP and FWI time series/Ecoregion CSVs/{model}_fwi_2001_2014_{short_name}.csv' # <-- Edit as necessary
            df.to_csv(output_path)
        else:
            print(f"No data extracted for region: {short_name}")