import yfinance as yf
import pandas as pd
from datetime import datetime
import os
 
# ================== CONFIGURATION =====================
start_date = '2025-04-01'
end_date = datetime.today().strftime('%Y-%m-%d')
 
output_dir = r'D:\Data_Mgmt2\Real_Time_Data_Pipeline'
os.makedirs(output_dir, exist_ok=True)
 
# ================== COUNTRY SECTORS =====================
country_sectors = {
    'USA': {
        'IT': ['AAPL', 'MSFT', 'ORCL', 'IBM', 'NVDA'],
        'Electronics': ['INTC', 'AMD', 'QCOM', 'TXN', 'AVGO'],
        'Agriculture': ['DE', 'ADM', 'MOS', 'BG', 'CF'],
        'Pharma': ['PFE', 'JNJ', 'MRK', 'BMY', 'ABBV'],
        'Automotive': ['TSLA', 'F', 'GM', 'RIVN', 'HOG']
    },
    'China': {
        'IT': ['TCEHY', 'BIDU', 'BABA', 'JD', 'NTES'],
        'Electronics': ['SMICY', 'LNVGY', 'ZTEHY', 'TPV.TW', 'BOE.SZ'],
        'Agriculture': ['YONG', 'DFM', 'CMCI', 'COFCO', 'ZHONG'],
        'Pharma': ['SINO', '3SBIO', 'HCM', 'CMS', '603259.SS'],
        'Automotive': ['NIO', 'LI', 'BYDDF', 'XPEV', 'GELYF']
    },
    'Europe': {
        'IT': ['SAP', 'ASML', 'ADYEY', 'CAP.PA', 'ERIC'],
        'Electronics': ['STM', 'NXPI', 'IFX.DE', 'AMS.SW', 'NOK'],
        'Agriculture': ['BASFY', 'BAYRY', 'SYT', 'BN.PA', 'RDSA.AS'],
        'Pharma': ['NVS', 'RHHBY', 'GSK', 'SNY', 'AZN'],
        'Automotive': ['VWAGY', 'BMWYY', 'MBGYY', 'RNLSY', 'STLA']
    },
    'India': {
        'IT': ['INFY.NS', 'TCS.NS', 'WIPRO.NS', 'TECHM.NS', 'HCLTECH.NS'],
        'Electronics': ['DIXON.NS', 'BHEL.NS', 'VGUARD.NS', 'BEL.NS', 'HAVELLS.NS'],
        'Agriculture': ['UPL.NS', 'PIIND.NS', 'COROMANDEL.NS', 'GODREJAGRO.NS', 'BALRAMCHIN.NS'],
        'Pharma': ['SUNPHARMA.NS', 'CIPLA.NS', 'DRREDDY.NS', 'BIOCON.NS', 'LUPIN.NS'],
        'Automotive': ['TATAMOTORS.NS', 'M&M.NS', 'MARUTI.NS', 'ASHOKLEY.NS', 'EICHERMOT.NS']
    }
}
 
# ================== SECTOR DATA =====================
for country, sectors in country_sectors.items():
    all_data = []
    for sector, tickers in sectors.items():
        for ticker in tickers:
            try:
                stock = yf.Ticker(ticker)
                hist = stock.history(start=start_date, end=end_date)
                if not hist.empty:
                    hist.reset_index(inplace=True)
                    hist = hist[['Date', 'Open', 'High', 'Low', 'Close', 'Volume']]
                    hist['Ticker'] = ticker  # Ensuring ticker column per row
                    hist['Sector'] = sector
                    hist['Country'] = country
                    all_data.append(hist)
            except Exception as e:
                print(f"Error fetching stock data for {ticker}: {e}")
 
    if all_data:
        final_df = pd.concat(all_data, ignore_index=True)
        filename = os.path.join(output_dir, f'{country}_sector_stock_data_Apr2025.csv')
        final_df.to_csv(filename, index=False)
        print(f"Sector stock data saved for {country}!")
    else:
        print(f"No sector stock data fetched for {country}.")
 
 
# ================== INDEX DATA =====================
country_indexes = {
    'USA': ['^GSPC', '^IXIC'],
    'China': ['000001.SS', '399001.SZ'],
    'Europe': ['^STOXX50E', '^FTSE'],
    'India': ['^NSEI', '^BSESN']
}
 
# ================== INDEX DATA =====================
for country, indexes in country_indexes.items():
    index_data = []
    for index in indexes:
        try:
            idx = yf.Ticker(index)
            hist = idx.history(start=start_date, end=end_date)
            if not hist.empty:
                hist.reset_index(inplace=True)
                hist = hist[['Date', 'Open', 'High', 'Low', 'Close', 'Volume']]
                hist['Ticker'] = index  # Ensuring ticker column per row
                hist['Sector'] = 'Index'
                hist['Country'] = country
                index_data.append(hist)
        except Exception as e:
            print(f"Error fetching index data for {index}: {e}")
 
    if index_data:
        final_df = pd.concat(index_data, ignore_index=True)
        filename = os.path.join(output_dir, f'{country}_index_data_Apr2025.csv')
        final_df.to_csv(filename, index=False)
        print(f"Index data saved for {country}!")
    else:
        print(f"No index data fetched for {country}.")