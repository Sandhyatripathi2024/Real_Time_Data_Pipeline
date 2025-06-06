Real-Time Stock Market Data Pipeline
This project analyzes stock market data and news sentiment around significant global events like President Trump's tariff announcement on April 2, 2025.
The objective is to evaluate market reactions before and after the announcement across the USA, China, Europe, and India.

🗂️ Directory Structure
bash
CopyEdit
Real_Time_Data_Pipeline/  
├── main.py                 # Cloud Function script for automated daily stock updates (from April onward)  
├── Stock_Data.py           # Script to fetch stock data for March 2025 (before announcement)  
├── News.py                 # Script to fetch news headlines for April 2025 (post announcement)  
├── requirements.txt        # Python dependencies  
├── README.md               # Project documentation  
├── trump_tariffs_news_sentiment_2025-04-26.csv  # Processed news sentiment data  
├── *.csv                   # Exported stock index & sector data per country  
├── stock_market_analysis/   # dbt project folder (models, macros, snapshots, tests, seeds)  
├── logs/                   # Optional logs folder  
└── .gitignore  
📊 Data Sources
-Yahoo Finance → stock prices (indices + sectors) for USA, China, Europe, India (March–April 2025)
-NewsAPI.org → headlines for April 2025 filtered by tariff-related keywords (manual fetch → sentiment scored)

🔄 Pipeline Overview
Stock Data (March) → fetched once using Stock_Data.py, saved as CSV.

News Data (April) → fetched once using News.py, manually processed for sentiment.

Daily Updates (April onward) → automated via main.py deployed as Google Cloud Function; scheduled daily at 11PM Berlin time by Cloud Scheduler.

All data loaded to BigQuery.

Transformations & modeling performed with dbt (Star Schema).

Final insights queried via SQL, available for dashboarding (e.g., Tableau).

(Refer to included Architecture Diagram for flow visualization.)

🛠️ Tech Stack
✔️ Python → ETL scripting
✔️ Google BigQuery → centralized data warehouse
✔️ Google Cloud Functions + Scheduler → automation & orchestration
✔️ dbt → transformation and data modeling
✔️ GitHub → version control
✔️ (Optional) Tableau → dashboard layer

⚙️ Setup Instructions
Clone this repository

bash
CopyEdit
git clone https://github.com/Sandhyatripathi2024/Real_Time_Data_Pipeline.git  
cd Real_Time_Data_Pipeline  
Install dependencies

bash
CopyEdit
pip install -r requirements.txt  
Run local scripts (if needed):

Fetch March stock data → python Stock_Data.py

Fetch April news → python News.py

Deploy daily ETL script as Cloud Function:

(see documentation inside main.py)

🎯 Key Features
-Real-time daily updates (automated with Cloud Functions & Scheduler)
-Star Schema modeling (with dbt)
-Supports pre-post event comparison (March vs April 2025)
-Extendable pipeline (additional countries, sectors, news sources)

🙏 Acknowledgments
Yahoo Finance

NewsAPI.org

Google Cloud Documentation

dbt Documentation

Special thanks to Prof. Dr.-Ing. Binh Vu for guidance

📌 Notes
-This project was developed as part of Data Management 2 course at SRH Hochschule Heidelberg (2025).
-Demo video and presentation slides included separately.