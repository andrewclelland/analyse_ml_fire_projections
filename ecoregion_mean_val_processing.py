"""
For each ecoregion, create grouped means and percentage changes compared to the historical observed period for each variable over the time periods 2025-2050, 2051-2075 and 2076-2100, then save as csv files for analysis.

Edit as necessary, but maintain consistency with other code.
"""
import pandas as pd
import os

# Define your variables and time periods
climate_vars = ['rh', 'tp', 'rlds', 'rsds', 'wsp', 't2m', 'mx2t', 'mn2t']
fwi_vars = ['BUI', 'DC', 'DMC', 'FFMC', 'FWI', 'ISI']
all_vars = climate_vars + fwi_vars

periods = {
    'historical': ('2001-01-01', '2023-12-31'),
    '2025_2050': ('2025-01-01', '2050-12-31'),
    '2051_2075': ('2051-01-01', '2075-12-31'),
    '2076_2100': ('2076-01-01', '2100-12-31')
}

# Model mapping
model_groups = {
    'ssp': ['ACCESS_SSP126', 'ACCESS_SSP245', 'ACCESS_SSP370',
            'MRI_SSP126', 'MRI_SSP245', 'MRI_SSP370']
}

# Provide region list as tuples: (region_code, region_model_code) - consistency with previous scripts
region_pairs = [('alaspen', 'alapen'), ('centcan', 'cancsh'), ('cookinl', 'cookin'), ('copppla', 'copper'), ('eastcan', 'eastcf'), ('eashti', 'eashti'),
               ('inteala', 'intlow'), ('mid-bor', 'midbor'), ('midwcan', 'midwes'), ('musklak', 'muslta'), ('nortcan', 'norths'), ('southud', 'sohudb'),
               ('watshig', 'watson'), ('nortcor', 'norcor'), ('nortter', 'nwterr'), ('eastsib', 'eastsib'), ('icelbor', 'icelnd'), ('kamcmea', 'kamkurm'),
               ('kamctai', 'kamtaig'), ('nesibta', 'nesibta'), ('okhotai', 'okhman'), ('sakhisl', 'sakhtai'), ('trancon', 'trzconf'), ('westsib', 'westsib'),
               ('scanand', 'scrusta'), ('uralmon', 'uralfor'), ('ahkland', 'ahklun'), ('berilow', 'berlow'), ('brooran', 'brookr'), ('kalanun', 'kalhar'),
               ('pacicoa', 'pacice'), ('novoisl', 'novoisl'), ('wranisl', 'wrangel'), ('alaseli', 'aleias'), ('arctcoa', 'arccoa'), ('arctfoo', 'arcfoo'),
               ('beriupl', 'berupl'), ('canalow', 'canlow'), ('davihig', 'davish'), ('canahig', 'canhig'), ('inteyuk', 'intalp'), ('canamid', 'canmid'),
               ('ogilalp', 'ogilvi'), ('tornmou', 'tornga'), ('kalste', 'kalste'), ('russarc', 'rusarc'), ('russber', 'rusbert'), ('chermou', 'cherski'),
               ('chukpen', 'chukchi'), ('kolapen', 'kolapen'), ('nortsib', 'nesibco'), ('nortrus', 'nwrunz'), ('scanmon', 'scambf'), ('taimsib', 'taicens'),
               ('tranbal', 'trzbald'), ('yamatun', 'yamalgy'), ('kamctun', 'kamtund')]

# Where to save output
output_dir = '/home/users/clelland/Model/Analysis/Summary stats' # <-- Edit as necessary
os.makedirs(output_dir, exist_ok=True)

for region, region_model in region_pairs:
    print(f"Processing {region}...")
    root = f'/home/users/clelland/Model/Analysis/CMIP and FWI time series/Ecoregion CSVs/{region}' # <-- Edit as necessary

    # Load CSVs
    csvs = {}
    for var in all_vars:
        def read_df(suffix):  # Helper to build file path
            return pd.read_csv(
                f'{root}/{suffix}_{region}.csv', parse_dates=['date'], index_col='date' # <-- Edit as necessary
            )

        if var in climate_vars:
            csvs[var] = {
                'Observed': read_df('e5l_2001_2023'),
                'ACCESS_SSP126': read_df('access_ssp126_climate_2015_2100'),
                'ACCESS_SSP245': read_df('access_ssp245_climate_2015_2100'),
                'ACCESS_SSP370': read_df('access_ssp370_climate_2015_2100'),
                'MRI_SSP126': read_df('mri_ssp126_climate_2015_2100'),
                'MRI_SSP245': read_df('mri_ssp245_climate_2015_2100'),
                'MRI_SSP370': read_df('mri_ssp370_climate_2015_2100'),
            }
        else:
            csvs[var] = {
                'Observed': read_df('cems_2001_2023'),
                'ACCESS_SSP126': read_df('access_ssp126_fwi_2015_2100'),
                'ACCESS_SSP245': read_df('access_ssp245_fwi_2015_2100'),
                'ACCESS_SSP370': read_df('access_ssp370_fwi_2015_2100'),
                'MRI_SSP126': read_df('mri_ssp126_fwi_2015_2100'),
                'MRI_SSP245': read_df('mri_ssp245_fwi_2015_2100'),
                'MRI_SSP370': read_df('mri_ssp370_fwi_2015_2100'),
            }

    # Output container for plotting
    results = []
    raw_means = []

    for var in all_vars:
        df_all = pd.DataFrame({model: df[var] for model, df in csvs[var].items()})
        df_all.index = pd.to_datetime(df_all.index)
        df_all['month'] = df_all.index.month

        # --- Save raw historical mean (2001–2023) before any bias correction ---
        raw_historical_mask = (df_all.index >= periods['historical'][0]) & (df_all.index <= periods['historical'][1])
        raw_historical_mean = df_all.loc[raw_historical_mask, 'Observed'].mean()

        # Compute monthly bias correction for SSPs
        bias_diffs = {}
        for model in model_groups['ssp']:
            if model in df_all.columns:
                period = ('2015-01-01', '2023-12-31')
                period_mask = (df_all.index >= period[0]) & (df_all.index <= period[1])
                diff = df_all.loc[period_mask, 'Observed'] - df_all.loc[period_mask, model]
                bias_diffs[model] = diff.groupby(df_all.loc[period_mask, 'month']).mean()

        # Apply correction
        corrected = {}
        corrected['Observed'] = df_all['Observed']
        for model in df_all.columns.drop(['Observed', 'month']):
            corrected_series = df_all[model].copy()
            corrected_series.index = pd.to_datetime(corrected_series.index)
            corrected_series = corrected_series + df_all['month'].map(bias_diffs[model])
            corrected[model] = corrected_series

        corrected_df = pd.DataFrame(corrected)

        # Mean values by period
        means = {}
        for label, (start, end) in periods.items():
            mask = (corrected_df.index >= start) & (corrected_df.index <= end)
            means[label] = corrected_df.loc[mask].mean()

        # Percentage change vs historical
        historic_mean = raw_historical_mean
        for future_period in ['2025_2050', '2051_2075', '2076_2100']:
            delta = ((means[future_period] - historic_mean) / historic_mean) * 100
            for model in model_groups['ssp']:
                if model in delta:
                    results.append({
                        'region': region,
                        'variable': var,
                        'model': model,
                        'period': future_period,
                        'percent_change': delta[model]
                    })

        # Save the raw means for each variable, model, and period
        for period, mean_series in means.items():
            for model in mean_series.index:
                raw_means.append({
                    'region': region,
                    'variable': var,
                    'model': model,
                    'period': period,
                    'mean_value': mean_series[model]
                })

        # Add observed percent_change = 0 for all periods
        if 'Observed' in means['historical']:
            results.append({
                'region': region,
                'variable': var,
                'model': 'Observed',
                'period': 'historical',
                'percent_change': 0.0
            })
        
    # Save intermediate results
    df_results = pd.DataFrame(results)
    df_means = pd.DataFrame(raw_means)
    
    # Merge summary stats and raw means
    df_combined = df_results.merge(df_means, on=['region', 'variable', 'model', 'period'], how='left')
    df_combined.to_csv(f'{output_dir}/{region}_summary.csv', index=False) # <-- Edit as necessary