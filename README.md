# analyse_ml_fire_projections
Repository to process the netCDF fire projections [made here](https://github.com/andrewclelland/ml_fire_projections) and analyse them. 

Preliminary:
*  Ensure you have a Google Earth Engine account linked to a project
*  Use of a supercomputer or HPC is encouraged for running the scripts

Order for running scripts:
1.  Before beginning the analysis `process` the netCDF files to csv format for more convenient analysis.
    *  Run `split_netCDF_into_years` first to reduce the netCDF file load - it converts the whole time series into single years for each scenario. You can also separate the whole file into North America/Eurasia (for example) as required.
    *  Then you can choose to find the time series by `ecoregion`, `geographical` region or `land_cover` class, as required. The scripts all perform the same job: the burned area is found for a specific shapefile over the whole time series and each scenario and saved into individual csv files.
