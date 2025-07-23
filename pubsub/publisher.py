#!/usr/bin/env python3
"""
publisher.py  – This script simulates NYC 311 events and publishes them to Pub/Sub.
"""

import csv
import json
import time
from google.cloud import pubsub_v1

PROJECT_ID = "discovery-projs"          
TOPIC_ID   = "streaming-311-events"    
CSV_PATH   = "pubsub/sample_311.csv"    # this is a small test file
DELAY_SEC  = 2                          # the pause between messages

def main() -> None:
    publisher = pubsub_v1.PublisherClient()
    topic_path = publisher.topic_path(PROJECT_ID, TOPIC_ID)

    with open(CSV_PATH, newline="") as f:
        reader = csv.DictReader(f)
        for row in reader:
            message_json = json.dumps({
                "request_id":   row["unique_key"],
                "timestamp":    row["created_date"],
                "type":         row["complaint_type"],
                "descriptor":   row["descriptor"],
                "zip":          row["incident_zip"],
                "borough":      row["borough"],
                "status":       row["status"],
                "ingested_at":  time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime())
            })
            future = publisher.publish(topic_path, message_json.encode("utf-8"))
            msg_id = future.result()           # blocks until server returns ID
            print(f"Published {msg_id}: {message_json}")

            time.sleep(DELAY_SEC)

    print("We done publishing them sample events bruv.")

if __name__ == "__main__":
    main()
