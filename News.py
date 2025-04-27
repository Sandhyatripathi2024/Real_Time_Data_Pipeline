import requests
import json
import pandas as pd
from datetime import datetime, timedelta
from vaderSentiment.vaderSentiment import SentimentIntensityAnalyzer

# -------------------- CONFIGURATION -------------------- #
api_key = '79b5af5f9c3546cb857a816907f56a31'
query = 'Trump tariffs OR trade war OR China tariffs'
from_date = '2025-04-01'
to_date = datetime.today().strftime('%Y-%m-%d')

current_date = datetime.strptime(from_date, '%Y-%m-%d')
end_date = datetime.strptime(to_date, '%Y-%m-%d')
page_size = 100

all_articles = []

# -------------------- FETCH NEWS DATA DAILY -------------------- #
while current_date <= end_date:
    next_date = current_date + timedelta(days=1)
    date_from = current_date.strftime('%Y-%m-%d')
    date_to = next_date.strftime('%Y-%m-%d')
    page = 1

    print(f"Fetching articles from {date_from} to {date_to}")

    while True:
        url = (f'https://newsapi.org/v2/everything?q={query}&from={date_from}&to={date_to}'
               f'&sortBy=publishedAt&pageSize={page_size}&page={page}&language=en&apiKey={api_key}')
        
        response = requests.get(url)
        if response.status_code != 200:
            print(f"Error {response.status_code}: {response.text}")
            break  # Stop if API limit reached or error

        articles = response.json().get('articles', [])
        print(f"Page {page}: Fetched {len(articles)} articles.")

        if not articles:
            break  # No more articles for this date

        all_articles.extend(articles)
        page += 1  # Move to next page

    current_date = next_date  # Move to the next day

print(f"Total articles fetched: {len(all_articles)}")

# -------------------- SENTIMENT ANALYSIS -------------------- #
analyzer = SentimentIntensityAnalyzer()
processed_articles = []

for article in all_articles:
    text = (article.get('title') or '') + ' ' + (article.get('description') or '')
    sentiment = analyzer.polarity_scores(text)['compound']
    processed_articles.append({
        'publishedAt': article.get('publishedAt'),
        'source': article['source'].get('name'),
        'title': article.get('title'),
        'description': article.get('description'),
        'url': article.get('url'),
        'sentiment_score': sentiment
    })

# -------------------- SAVE TO CSV -------------------- #
df = pd.DataFrame(processed_articles)
filename = f'trump_tariffs_news_sentiment_{to_date}.csv'
df.to_csv(filename, index=False)
print(f"Saved news data with sentiment to {filename}")

# Optionally, print a few rows
print(df.head())
