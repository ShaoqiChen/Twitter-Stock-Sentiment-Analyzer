import streamlit as st
import time
import random
import requests
import flair
import plotly.express as px
import pandas as pd
from google.cloud import bigquery

placeholder = st.empty()
start_button = st.empty()

def sentiment_analysis(text):
    sentiment_model = flair.models.TextClassifier.load('en-sentiment')
    sentence = flair.data.Sentence(TEXT)
    sentiment_model.predict(sentence)

    if sentence.labels[0].value == 'NEGATIVE':
        return -1*sentence.labels[0].score
    else: return sentence.labels[0].score




bqclient = bigquery.Client()

# Download query results.
# query_string = """
# SELECT 
# time,
# AVG(mean_sentiment) OVER (ORDER BY time ASC ROWS 6 PRECEDING) AS rolling_mean
# FROM 
# (SELECT 
# timestamp_trunc(posted_at, MINUTE, 'UTC') AS time,
# AVG(sentiment) AS mean_sentiment
# FROM `twitter-stock-sentiment.streaming_tweets.tweets_TSLA`
# GROUP BY time
# LIMIT 100)
# ORDER BY time
# """




def radar_chart():  
    # ACCESS_KEY = '4aff97d989c193993dad530d9a22b9ea'
    # response = requests.get('http://api.marketstack.com/v1/intraday',
    #                         params={
    #                             'access_key':ACCESS_KEY,
    #                             'symbols':'TSLA'
    #                         })
    # total_df = pd.DataFrame(response.json()['data'], index=range(len(response.json()['data'])))

    text_query_string = """
    SELECT 
    posted_at,
    text
    FROM `twitter-stock-sentiment.streaming_tweets.tweets_TSLA`
    """

    dataframe = (
        bqclient.query(text_query_string)
        .result()
        .to_dataframe(
            # Optionally, explicitly request to use the BigQuery Storage API. As of
            # google-cloud-bigquery version 1.26.0 and above, the BigQuery Storage
            # API is used by default.
            create_bqstorage_client=True,
        )
    )
    print(dataframe.head())
    dataframe['sentiment'] = dataframe['text'].apply(sentiment_analysis)
    dataframe['truncted_datetime'] = pd.to_datetime(dataframe['posted_at']).dt.floor('T')

    fig = px.line(dataframe['sentiment'].groupby(dataframe['truncted_datetime']).mean())
    placeholder.write(fig)

    
    
if start_button.button('Start',key='start'):
    start_button.empty()
    if st.button('Stop',key='stop'):
        pass
    while True:
        radar_chart()
        time.sleep(60)