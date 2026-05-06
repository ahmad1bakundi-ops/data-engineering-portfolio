import wbgapi as wb
import pandas as pd
import os
from datetime import datetime

def fetch_nigeria_indicators():
    print("Fetching Nigerian economic data from World Bank API...")

    indicators = {
        'NY.GDP.MKTP.KD.ZG': 'gdp_growth_pct',
        'FP.CPI.TOTL.ZG': 'inflation_rate_pct',
        'SL.UEM.TOTL.ZS': 'unemployment_rate_pct',
        'NY.GDP.PCAP.CD': 'gdp_per_capita_usd',
        'BX.KLT.DINV.WD.GD.ZS': 'foreign_direct_investment_pct',
        'EG.USE.ELEC.KH.PC': 'electricity_consumption_kwh',
    }

    dfs = []
    for code, name in indicators.items():
        try:
            df = wb.data.DataFrame(code, 'NGA', mrv=15)
            df = df.T.reset_index()
            df.columns = ['year', name]
            df['year'] = df['year'].str.replace('YR', '').astype(int)
            dfs.append(df.set_index('year'))
            print(f"  ✅ {name}")
        except Exception as e:
            print(f"  ❌ {name}: {e}")

    combined = pd.concat(dfs, axis=1).reset_index()
    combined.columns.name = None
    combined['country'] = 'Nigeria'
    combined['country_code'] = 'NGA'
    combined['ingested_at'] = datetime.now().isoformat()

    os.makedirs('data', exist_ok=True)
    combined.to_csv('data/nigeria_economic_data.csv', index=False)
    print(f"\n✅ Saved {len(combined)} rows to data/nigeria_economic_data.csv")
    print(combined.to_string())
    return combined

if __name__ == '__main__':
    fetch_nigeria_indicators()
