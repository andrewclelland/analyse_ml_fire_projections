# analyse_ml_fire_projections
Repository to process the netCDF fire projections [made here](https://github.com/andrewclelland/ml_fire_projections) and analyse them. Also includes processing and analysis scripts for processing and analysing the climate and fire weather variables.

Preliminary:
*  Ensure you have a Google Earth Engine account linked to a project
*  It is strongly recommended to have access to a Google Cloud Storage bucket
*  Use of a supercomputer or HPC is encouraged for running the scripts

Order for running scripts:
1.  Before beginning the analysis conduct `netCDF_processing` to convert the files to csv format for more convenient analysis.
    *  Run `split_netCDF_into_years` first to reduce the netCDF file load - it converts the whole time series into single years for each scenario. You can also separate the whole file into North America/Eurasia (for example) as required.
    *  Use `Check and test shapefiles.ipynb` to load and check the shapefiles used in the analysis. Land cover per region can also be found here.
    *  Then you can choose to find the time series by `ecoregion`, `geographical` region or `land_cover` class, as required. The scripts all perform the same job: the burned area is found for a specific shapefile over the whole time series and each scenario and saved into individual csv files.
    *  Combine the individual scenario csv files into one master csv file for each ecoregion using `Process ecoregion CSVs.ipynb`.
    *  We conduct our analysis on an ecoregion level, so remaining code files can easily be adapted for land cover or geographical region analysis.
2.  Conduct `Climate_and_fire_weather_variable_processing` to process the climate and fire weather indices before analysing.
    *  `Process_data` in any order or simultaneously for both the historic and future periods.
    *  Then `Check_for_missing_data` using these scripts. Sometimes when batch processing the data simultaneously it causes the Earth Engine system to be overloaded, and as such certain months can be missed.
    *  After the processing use `Shorten CSVs`. When the processing scripts run, it will save all previous iterations in the CSV file. To remove previous iterations, use this Notebook to save just the current scenario.
    *  Add December 2023 and all 2024 historic data for ERA5-Land and CEMS using `Add 2024 E5l and CEMS data.ipynb`.
3.  Process the `ecoregion_mean` values for each variable into individual ecoregion csv files, before combining into a single `master_summary` csv file for all ecoregions.
4.  Analyse the data on an `individual` or `group` level using the Jupyter Notebooks.
5.  Make circumpolar plots across all ecoregions using `Plots from master summary.ipynb`. Bar plots of burned area can also be made here for grouped regions.
