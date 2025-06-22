"""
Script to find the total burned area from processed annual netCDF files for each ecoregion, and save the monthly BA in a csv file for each ecoregion.

All ecoregions across the ABZ are processed here, although the code can easily be adapted to focus on certain areas.
"""
import matplotlib.pyplot as plt
import xarray as xr
import geopandas as gpd
import warnings
import os
import time
warnings.filterwarnings("ignore")

start_time = time.time()
os.environ["CPL_LOG"] = "/home/users/clelland/Model/error_files/Processing/ERROR7"

# Load shapefile
shp_path = '/home/users/clelland/Model/Analysis/RESOLVE shapefile from GEE/resolve_shapefile_from_gee.shp'
gdf = gpd.read_file(shp_path)
selected_ecoregions = gdf[gdf['BIOME_NAME'].isin(['Boreal Forests/Taiga', 'Tundra'])]
print("Shapefile loaded")

# Set manual overrides for certain regions
manual_shortnames = {
    'Eastern Canadian Shield taiga': 'eashti',
    'Northeast Siberian taiga': 'nesibta',
    'Kalaallit Nunaat Arctic steppe': 'kalste'
}

# Loop through filtered ecoregions and create short names
regions = {}
for _, row in selected_ecoregions.iterrows():
    eco_name = row['ECO_NAME']
    
    # Use manual shortname if available
    if eco_name in manual_shortnames:
        short_name = manual_shortnames[eco_name]
    else:
        words = eco_name.split()
        short_name = (words[0][:4] + words[1][:3]).lower() if len(words) >= 2 else words[0][:7].lower()

    print(f"Processing region: {eco_name} -> {short_name}")

    # Store the individual GeoDataFrame in the dictionary
    regions[short_name] = gdf[gdf['ECO_NAME'] == eco_name]

years = range(2025, 2101)
models = ['access', 'mri']
scenarios = ['ssp126', 'ssp245', 'ssp370']

# Loop through regions, models, scenarios, years
for short_name, region_gdf in regions.items():
    print(f"Processing region: {short_name}")
    
    # Initialize an empty list to collect area time series
    for model in models:
        for scenario in scenarios:
            area_list = []
            for year in years:
                print(f"Iterating over: {model} {scenario} {year} for {short_name}")
                netcdf_path = f"/gws/nopw/j04/bas_climate/users/clelland/model/output_{model}_north/{scenario}/output_{model}_north_{scenario}_{year}_v2.nc" # <-- Edit as necessary
                ds = xr.open_dataset(netcdf_path)
                ds.rio.set_spatial_dims(x_dim="lon", y_dim="lat", inplace=True)
                ds.rio.write_crs("EPSG:4326", inplace=True)
                ds_clip = ds.rio.clip(region_gdf.geometry.values, region_gdf.crs, drop=True)
                ds_subset = ds_clip.where(ds_clip["predictions"] >= 0.5) # <-- Edit prediction probability as necessary
                ds_subset = ds_subset.dropna(dim="time", how="all")
                ds_subset = ds_subset.dropna(dim="lat", how="all")
                ds_subset = ds_subset.dropna(dim="lon", how="all")
                
                # Load NetCDF file with rioxarray
                da = ds_subset["predictions"]
                
                # Compute area per timestep
                # Each pixel = 4000m x 4000m = 16 km²
                pixel_area_mha = 16 / 10000
                
                # Area over time
                masked_area = (da > 0).sum(dim=["lat", "lon"]) * pixel_area_mha
                full_times = ds["time"]
                area_timeseries_year = masked_area.reindex(time=full_times, fill_value=0)
            
                area_list.append(area_timeseries_year)
                ds.close()
            
            # Concatenate all years along the time dimension
            area_timeseries = xr.concat(area_list, dim="time")
            
            # Convert to pandas Series
            area_series = area_timeseries.to_series()
            
            # Reset index to get a DataFrame with columns: time, value
            area_df = area_series.reset_index()
            area_df.columns = ['time', 'burned_area_Mha']
            
            # Save to CSV
            #output_csv_path = f"/home/users/clelland/Model/Analysis/Ecoregion plots/area_timeseries_eco_{short_name}_{model}_{scenario}.csv" # <-- Edit as necessary
            area_df.to_csv(output_csv_path, index=False)
            
            print(f"Saved area time series to {output_csv_path}")
            
            time_values = area_timeseries["time"].values
            plt.figure(figsize=(12, 5))
            plt.plot(time_values, area_timeseries.values, label=f"Burned Area (Mha for {short_name}")
            plt.xlabel("Time")
            plt.ylabel("Burned Area (Mha)")
            plt.title(f"Burned Area Over Time for {short_name}, {model} {scenario}")
            plt.grid(True)
            plt.legend()
            plt.tight_layout()
            #plt.savefig(f'/home/users/clelland/Model/Analysis/Ecoregion plots/area_timeseries_{model}_{scenario}_eco_{short_name}.png', dpi=300, bbox_inches='tight', transparent=True) # <-- Edit as necessary

# Record time
end_time = time.time()
time_taken = (end_time - start_time) / 3600
print(f"\nTime taken: {time_taken:.2f} hours")