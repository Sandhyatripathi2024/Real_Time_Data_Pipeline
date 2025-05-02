import yfinance as yf
import pandas as pd
from google.cloud import bigquery

def fetch_and_load():
    start_date = '2025-04-01'
    end_date = pd.Timestamp.today().strftime('%Y-%m-%d')

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

    client = bigquery.Client()
    dataset_id = 'stock_data'  

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
                        hist['Ticker'] = ticker
                        hist['Sector'] = sector
                        hist['Country'] = country
                        all_data.append(hist)
                except Exception as e:
                    print(f"Error fetching {ticker}: {e}")

        if all_data:
            df = pd.concat(all_data, ignore_index=True)
            table_id = f"{client.project}.{dataset_id}.{country.lower()}_sector_stock_data"
            job = client.load_table_from_dataframe(df, table_id, job_config=bigquery.LoadJobConfig(write_disposition="WRITE_TRUNCATE"))
            job.result()
            print(f"Loaded sector data for {country} → {table_id}")

    # Similarly repeat for indexes
    country_indexes = {
        'USA': ['^GSPC', '^IXIC'],
        'China': ['000001.SS', '399001.SZ'],
        'Europe': ['^STOXX50E', '^FTSE'],
        'India': ['^NSEI', '^BSESN']
    }

    for country, indexes in country_indexes.items():
        index_data = []
        for index in indexes:
            try:
                idx = yf.Ticker(index)
                hist = idx.history(start=start_date, end=end_date)
                if not hist.empty:
                    hist.reset_index(inplace=True)
                    hist = hist[['Date', 'Open', 'High', 'Low', 'Close', 'Volume']]
                    hist['Ticker'] = index
                    hist['Sector'] = 'Index'
                    hist['Country'] = country
                    index_data.append(hist)
            except Exception as e:
                print(f"Error fetching index {index}: {e}")

        if index_data:
            df = pd.concat(index_data, ignore_index=True)
            table_id = f"{client.project}.{dataset_id}.{country.lower()}_index_data"
            job = client.load_table_from_dataframe(df, table_id, job_config=bigquery.LoadJobConfig(write_disposition="WRITE_TRUNCATE"))
            job.result()
            print(f"Loaded index data for {country} → {table_id}")

def main(request):
    fetch_and_load()
    return "Data loaded to BigQuery"

if __name__ == "__main__":
    fetch_and_load()
