import pandas as pd
import numpy as np

def transform_nigeria_data():
    print("Transforming Nigerian economic data...")

    df = pd.read_csv('data/nigeria_economic_data.csv')

    # Drop rows with no meaningful data
    df = df.dropna(subset=['gdp_growth_pct', 'inflation_rate_pct'], how='all')
    df = df[df['year'] >= 2010].copy()
    df = df.sort_values('year').reset_index(drop=True)

    # Economic health score (simple composite)
    df['gdp_growth_norm'] = df['gdp_growth_pct'].fillna(0)
    df['inflation_norm'] = df['inflation_rate_pct'].fillna(df['inflation_rate_pct'].mean())

    # Classify economic periods
    def classify_economy(row):
        if row['gdp_growth_pct'] > 5:
            return 'High Growth'
        elif row['gdp_growth_pct'] > 2:
            return 'Moderate Growth'
        elif row['gdp_growth_pct'] > 0:
            return 'Slow Growth'
        else:
            return 'Recession'

    df['economic_period'] = df.apply(classify_economy, axis=1)

    # Inflation severity
    def classify_inflation(val):
        if pd.isna(val): return 'Unknown'
        if val < 10: return 'Low'
        elif val < 20: return 'Moderate'
        elif val < 30: return 'High'
        else: return 'Crisis'

    df['inflation_severity'] = df['inflation_rate_pct'].apply(classify_inflation)

    # Year over year GDP change
    df['gdp_yoy_change'] = df['gdp_growth_pct'].diff().round(2)

    # GDP per capita change
    df['gdp_per_capita_change_usd'] = df['gdp_per_capita_usd'].diff().round(2)

    # Real hardship index (inflation minus GDP growth)
    df['hardship_index'] = (df['inflation_rate_pct'] - df['gdp_growth_pct']).round(2)

    # Clean up
    df = df.drop(columns=['gdp_growth_norm', 'inflation_norm'])

    import os
    os.makedirs('data', exist_ok=True)
    df.to_csv('data/nigeria_economic_transformed.csv', index=False)

    print(f"✅ Transformed {len(df)} rows")
    print()
    print("--- Economic Periods ---")
    print(df[['year', 'gdp_growth_pct', 'inflation_rate_pct', 'economic_period', 'inflation_severity', 'hardship_index']].to_string(index=False))
    return df

if __name__ == '__main__':
    transform_nigeria_data()
