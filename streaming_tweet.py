"""A streaming python pipeline to read in pubsub tweets and perform classification"""

from __future__ import absolute_import

import argparse
import datetime
import json
import logging

import numpy as np

import apache_beam as beam
import apache_beam.transforms.window as window
from apache_beam.io.gcp.bigquery_tools import parse_table_schema_from_json
from apache_beam.options.pipeline_options import StandardOptions, GoogleCloudOptions, SetupOptions, PipelineOptions
from apache_beam.transforms.util import BatchElements

from googleapiclient import discovery


# Instantiate the ML engine api client object as none
#cmle_api = None


# def init_api():
#     global cmle_api

#     # If it hasn't been instantiated yet: do it now
#     if cmle_api is None:
#         cmle_api = discovery.build('ml', 'v1',
#                                    discoveryServiceUrl='https://storage.googleapis.com/cloud-ml/discovery/ml_v1_discovery.json',
#                                    cache_discovery=True)


def aggregate_format(key_values):
    # Aggregate tweets per 10 second window
    (key, values) = key_values

    mean_sentiment = np.mean([x['length'] for x in values])
    mean_timestamp = datetime.datetime.utcfromtimestamp(np.mean([
        (datetime.datetime.strptime(x["posted_at"], '%Y-%m-%d %H:%M:%S') - datetime.datetime.fromtimestamp(
            0)).total_seconds() for x in values
    ]))

    logging.info("mean sentiment")
    logging.info(mean_sentiment)

    logging.info("mean timestamp")
    logging.info(mean_timestamp)

    # Return in correct format, according to BQ schema
    aggregated_length = [{"posted_at": mean_timestamp.strftime('%Y-%m-%d %H:%M:%S'), "sentiment": mean_sentiment}]
    print(aggregated_length)
    return list(map(lambda aggregated_length: json.loads(aggregated_length), aggregated_length))


def estimate_cmle(instances):
    """
    Calls the tweet_sentiment_classifier API on CMLE to get predictions
    Args:
       instances: list of strings
    Returns:
        float: estimated values
    """

    # Init the CMLE calling api
    #init_api()
    APIKEY="AIzaSyCzOT7Kw0H4klK4t03Pi92jj_x6F0Z7nOs"  # Replace with your API key
    from googleapiclient.discovery import build
    lservice = build('language', 'v1beta1', developerKey=APIKEY)

    request_data = {'instances': instances}

    logging.info("making request to the Natural Language api")

    # Call the model
    #model_url = 'projects/twitter-stock-sentiment/models/tweet_sentiment_classifier'
    #response = cmle_api.projects().predict(body=request_data, name=model_url).execute()

    # Read out the scores
    #values = [item["score"] for item in response['predictions']]

    values = []
    for tweet in instances:
        response = lservice.documents().analyzeSentiment(
        body={
          'document': {
             'type': 'PLAIN_TEXT',
             'language': 'EN',
             'content': tweet
          }
        }).execute()

        # extract sentiment prediction
         # numerical score 0-1
        values.append(response['documentSentiment']['polarity']) 

    return values


def estimate(messages):

    # Be able to cope with a single string as well
    if not isinstance(messages, list):
        messages = [messages]

    # Messages from pubsub are JSON strings
    instances = list(map(lambda message: json.loads(message), messages))

    # Estimate the sentiment of the 'text' of each tweet
    #scores = estimate_cmle([instance["text"] for instance in instances])
    # This is for testing
    lengths = [len(instance["text"]) for instance in instances]

    # Join them together
    for i, instance in enumerate(instances):
        #instance['sentiment'] = scores[i]
        instance['length'] = lengths[i]

    logging.info("first message in batch")
    logging.info(instances[0])

    return instances


def run(argv=None):
    # Main pipeline run def

    # Make explicit BQ schema for output tables
    bigqueryschema_json = '{"fields": [' \
                          '{"name":"id","type":"STRING"},' \
                          '{"name":"text","type":"STRING"},' \
                          '{"name":"user_id","type":"STRING"},' \
                          '{"name":"posted_at","type":"TIMESTAMP"},' \
                          '{"name":"length","type":"NUMERIC"}' \
                          ']}'
                          
                          
    bigqueryschema = parse_table_schema_from_json(bigqueryschema_json)

    bigqueryschema_mean_json = '{"fields": [' \
                               '{"name":"posted_at","type":"TIMESTAMP"},' \
                               '{"name":"sentiment","type":"FLOAT"}' \
                               ']}'
    bigqueryschema_mean = parse_table_schema_from_json(bigqueryschema_mean_json)

    """Build and run the pipeline."""
    parser = argparse.ArgumentParser()
    group = parser.add_mutually_exclusive_group(required=False)
    group.add_argument(
        '--input_subscription',
        help=('Input PubSub subscription of the form '
              '"projects/twitter-stock-sentiment/subscriptions/test."'),
        default="projects/twitter-stock-sentiment/subscriptions/test"

    )
    group.add_argument(
        '--input_topic',
        help=('Input PubSub topic of the form '
              '"projects/twitter-stock-sentiment/topics/stock-twitter."'),
        default="projects/twitter-stock-sentiment/topics/stock-twitter"
    )

    known_args, pipeline_args = parser.parse_known_args(argv)

    # We use the save_main_session option because one or more DoFn's in this
    # workflow rely on global context (e.g., a module imported at module level).
    pipeline_options = PipelineOptions(pipeline_args)
    pipeline_options.view_as(SetupOptions).save_main_session = True
    pipeline_options.view_as(StandardOptions).streaming = True
    # Run on Cloud Dataflow by default
    pipeline_options.view_as(StandardOptions).runner = 'DataflowRunner'

    google_cloud_options = pipeline_options.view_as(GoogleCloudOptions)
    google_cloud_options.project = 'twitter-stock-sentiment'
    google_cloud_options.staging_location = 'gs://tweets-bucket-20210923/staging'
    google_cloud_options.temp_location = 'gs://tweets-bucket-20210923/temp'
    google_cloud_options.region = 'europe-west3'

    p = beam.Pipeline(options=pipeline_options)

    # Read from PubSub into a PCollection.
    if known_args.input_subscription:
        lines = p | "read in tweets" >> beam.io.ReadFromPubSub(
            subscription=known_args.input_subscription,
            with_attributes=False,
            id_label="tweet_id"
        )
    else:
        lines = p | "read in tweets" >> beam.io.ReadFromPubSub(
            topic=known_args.input_topic,
            with_attributes=False,
            id_label="tweet_id")

    # Window them, and batch them into batches of 50 (not too large)
    output_tweets = (lines
                     | 'assign window key' >> beam.WindowInto(window.SlidingWindows(120,20))
                     | 'batch into n batches' >> BatchElements(min_batch_size=49, max_batch_size=50)
                     | 'account length' >> beam.FlatMap(lambda messages: estimate(messages))
                     )

    # Write to Bigquery
    output_tweets | 'store twitter posts' >> beam.io.WriteToBigQuery(
        table="tweets_with_length",
        dataset="streaming_tweets",
        schema=bigqueryschema,
        write_disposition=beam.io.BigQueryDisposition.WRITE_APPEND,
        create_disposition=beam.io.BigQueryDisposition.CREATE_IF_NEEDED,
        project="twitter-stock-sentiment"
    )

    # Average out and log the mean value
    (output_tweets
     | 'pair with key' >> beam.Map(lambda x: (1, x))
     | 'group by key' >> beam.GroupByKey()
     | 'aggregate and format' >> beam.Map(aggregate_format)
     | 'store aggregated sentiment' >> beam.io.WriteToBigQuery(
                table="aggregated_tweets",
                dataset="streaming_tweets",
                schema=bigqueryschema_mean,
                write_disposition=beam.io.BigQueryDisposition.WRITE_APPEND,
                create_disposition=beam.io.BigQueryDisposition.CREATE_IF_NEEDED,
                project="twitter-stock-sentiment"))

    result = p.run()
    result.wait_until_finish()


if __name__ == '__main__':
    logging.getLogger().setLevel(logging.INFO)
    run()
