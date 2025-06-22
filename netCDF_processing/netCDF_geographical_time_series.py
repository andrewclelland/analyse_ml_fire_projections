"""
Script to find the total burned area from processed annual netCDF files for each named geographical region, and save the monthly BA in a csv file for each region.

This example processes the data for Eurasia, but can easily be adapted for North America.
"""
import matplotlib.pyplot as plt
import xarray as xr
import geopandas as gpd
import warnings
import os
import time
warnings.filterwarnings("ignore")

start_time = time.time()
os.environ["CPL_LOG"] = "/home/users/clelland/Model/error_files/Processing/ERROR2"

# Load shapefile
shp_path = '/home/users/clelland/Model/Analysis/Countries shapefile/world-administrative-boundaries.shp' # <-- Edit as necessary
gdf = gpd.read_file(shp_path)
shapefile = gdf.to_crs(epsg=4326)
print("Shapefile loaded")

# Edit countries as necessary
scandi_gdf = shapefile[shapefile['name'].isin(['Sweden', 'Finland', 'Norway'])]
russia_gdf = shapefile[shapefile['name'].isin(['Russian Federation'])]

years = range(2025, 2101)
models = ['access', 'mri']
scenarios = ['ssp126', 'ssp245', 'ssp370']

# Combine into dictionary for easy looping
regions = {
    "russia": russia_gdf,
    "scandi": scandi_gdf
}

# Loop through regions, models, scenarios, years
for region_name, region_gdf in regions.items():
    print(f"Processing region: {region_name}")
    
    # Initialize an empty list to collect area time series
    for model in models:
        for scenario in scenarios:       
            area_list = []
            for year in years:
                print(f"Iterating over: {model} {scenario} {year} for {region_name}")
                netcdf_path = f"/gws/nopw/j04/bas_climate/users/clelland/model/output_{model}_north/{scenario}/output_{model}_north_{scenario}_{year}_v2_eurasia.nc" # <-- Edit as necessary
                ds = xr.open_dataset(netcdf_path)
                ds.rio.set_spatial_dims(x_dim="lon", y_dim="lat", inplace=True)
                ds.rio.write_crs("EPSG:4326", inplace=True)
                ds_clip = ds.rio.clip(region_gdf.geometry.values, region_gdf.crs, drop=True)
                ds_subset = ds_clip.where(ds_clip["predictions"] >= 0.5) # <-- Edit probability level as necessary
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
            output_csv_path = f"/home/users/clelland/Model/Analysis/Geo region plots/area_timeseries_geo_{region_name}_{model}_{scenario}.csv" # <-- Edit as necessary
            area_df.to_csv(output_csv_path, index=False)
            
            print(f"Saved area time series to {output_csv_path}")
            
            time_values = area_timeseries["time"].values
            plt.figure(figsize=(12, 5))
            plt.plot(time_values, area_timeseries.values, label=f"Burned Area (Mha for {region_name}")
            plt.xlabel("Time")
            plt.ylabel("Burned Area (Mha)")
            plt.title(f"Burned Area Over Time for {region_name}, {model} {scenario}")
            plt.grid(True)
            plt.legend()
            plt.tight_layout()
            plt.savefig(f'/home/users/clelland/Model/Analysis/Geo region plots/area_timeseries_{model}_{scenario}_geo_{region_name}.png', dpi=300, bbox_inches='tight', transparent=True) # <-- Edit as necessary

# Record time
end_time = time.time()
time_taken = (end_time - start_time) / 3600
print(f"\nTime taken: {time_taken:.2f} hours")