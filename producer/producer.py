import time
import json
import requests
from kafka import KafkaProducer

from utils.constants import (
    FEDCREDIT_BASE_URL,
    FEDCREDIT_ENDPOINT,
    FEDCREDIT_FIELDS,
    FEDCREDIT_PAGE_SIZE,
)
from config.config_loader import (
    KAFKA_BOOTSTRAP,
    KAFKA_TOPIC,
    FETCH_INTERVAL_SEC,
)

producer = KafkaProducer(
    bootstrap_servers=[KAFKA_BOOTSTRAP],
    value_serializer=lambda v: json.dumps(v).encode("utf-8"),
)

def build_initial_url():
    base = FEDCREDIT_BASE_URL.rstrip("/") + FEDCREDIT_ENDPOINT
    params = {
        "format": "json",
        "page[size]": str(FEDCREDIT_PAGE_SIZE),
    }
    fields = FEDCREDIT_FIELDS.strip()
    if fields:
        params["fields"] = fields
    query = "&".join(f"{k}={v}" for k, v in params.items())
    return f"{base}?{query}"

def fetch_all_records():
    all_rows = []
    next_url = build_initial_url()

    while next_url:
        try:
            resp = requests.get(next_url, timeout=60)
            resp.raise_for_status()
            payload = resp.json()
            data = payload.get("data", [])
            if not data:
                break
            all_rows.extend(data)
            links = payload.get("links", {})
            next_url = links.get("next")
        except Exception as e:
            print(f"Error fetching Federal Credit data: {e}")
            break

    return all_rows

def main():
    while True:
        rows = fetch_all_records()
        for row in rows:
            print(f"Producing → {row}")
            producer.send(KAFKA_TOPIC, value=row)
        time.sleep(FETCH_INTERVAL_SEC)

if __name__ == "__main__":
    main()
