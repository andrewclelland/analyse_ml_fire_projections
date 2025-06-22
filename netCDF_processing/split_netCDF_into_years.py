"""
Script to split the output netCDF file into years/decades and/or regions.
"""
import xarray as xr

models = ['access', 'mri']
scenarios = ['ssp126', 'ssp245', 'ssp370']
years = range(2025, 2101)

for model in models:
    for scenario in scenarios:
        # Open the combined NetCDF file
        ds = xr.open_dataset(f'/gws/nopw/j04/bas_climate/users/clelland/model/output_{model}_south/output_{model}_south_{scenario}_2025_2100_v2.nc') # <-- Edit as necessary

        # Process only for North America/Eurasia (change as appropriate)
        #ds_subset = ds.sel(lon=ds.lon[ds.lon >= 0])
        
        # Split by year
        for year in years:
            #yearly_ds = ds_subset.sel(time=str(year))
            yearly_ds = ds.sel(time=str(year))
            yearly_ds.to_netcdf(f"/gws/nopw/j04/bas_climate/users/clelland/model/output_{model}_south/{scenario}/output_{model}_south_{scenario}_{year}_v2.nc", encoding={"predictions": {"zlib": True, "complevel": 9}}) # <-- Edit as necessary

            print(f"Saved for {model} {scenario} {year} Siberia")

"""
# For 2025-2030 - Split by **6-years**
sixyr_ds = ds.sel(time=slice(str(2025), str(2030)))
    sixyr_ds.to_netcdf("/gws/nopw/j04/bas_climate/users/clelland/model/output_access_combined/ssp126/output_access_combined_ssp126_2025_2030.nc", encoding={"predictions": {"zlib": True, "complevel": 9}})

# Split by **decade**
for start_year in range(2031, 2101, 10):  # Start from 2031, step by 10 years
    end_year = start_year + 9
    decadal_ds = ds_subset.sel(time=slice(str(start_year), str(end_year)))
    decadal_ds.to_netcdf(f"/gws/nopw/j04/bas_climate/users/clelland/model/output_access_north/ssp126/output_access_north_ssp126_{start_year}_{end_year}_v2_eurasia.nc", encoding={"predictions": {"zlib": True, "complevel": 9}})
    print(f'Saved netCDF for {start_year}_{end_year}')

"""