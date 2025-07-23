#!/usr/bin/env python3
"""
main.py – Dataflow streaming pipeline:
  Pub/Sub JSON  ➜  dict  ➜  BigQuery row
"""

import json
import apache_beam as beam
from apache_beam.options.pipeline_options import PipelineOptions, StandardOptions

PROJECT_ID   = "discovery-projs"
SUB_ID       = "alpha-stream-sub"          # our subscription
BQ_TABLE     = "discovery-projs:alpha_ds.requests"  # dataset.table
TEMP_LOC     = "gs://nyc_buckets/temp"
REGION       = "us-central1"

class JsonToRow(beam.DoFn):
    """Parse Pub/Sub message -> dict matching BigQuery schema."""
    def process(self, element):
        data = json.loads(element.decode("utf-8"))
        yield {
            "request_id":  data["request_id"],
            "timestamp":   data["timestamp"],
            "type":        data["type"],
            "descriptor":  data["descriptor"],
            "zip":         data["zip"],
            "borough":     data["borough"],
            "status":      data["status"],
            "ingested_at": data["ingested_at"],
        }

def run():
    opts = PipelineOptions(
        streaming=True,
        project=PROJECT_ID,
        temp_location=TEMP_LOC,
        region=REGION,
    )
    with beam.Pipeline(options=opts) as p:
        (
            p
            | "ReadFromPubSub" >> beam.io.ReadFromPubSub(
                  subscription=f"projects/{PROJECT_ID}/subscriptions/{SUB_ID}",
                  timestamp_attribute=None,
              )
            | "ParseJSON"      >> beam.ParDo(JsonToRow())
            | "WriteToBQ" >> beam.io.WriteToBigQuery(
                 BQ_TABLE,
                 write_disposition=beam.io.BigQueryDisposition.WRITE_APPEND,
                 create_disposition=beam.io.BigQueryDisposition.CREATE_NEVER,
              )
        )

if __name__ == "__main__":
    run()
