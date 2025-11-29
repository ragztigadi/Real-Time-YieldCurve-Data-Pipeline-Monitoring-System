import json
import requests
from datetime import datetime

from kafka import KafkaProducer
from config.config_loader import (
    KAFKA_BOOTSTRAP,
    KAFKA_TOPIC,
    FETCH_INTERVAL_SEC,  
)
from utils.constants import (
    FEDCREDIT_BASE_URL,
    FEDCREDIT_ENDPOINT,
    FEDCREDIT_FIELDS,
    FEDCREDIT_PAGE_SIZE,
)


def _build_initial_url() -> str:
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


def _fetch_all_records():
    all_rows = []
    next_url = _build_initial_url()

    while next_url:
        resp = requests.get(next_url, timeout=60)
        resp.raise_for_status()
        payload = resp.json()

        data = payload.get("data", [])
        if not data:
            break

        all_rows.extend(data)

        links = payload.get("links", {})
        next_url = links.get("next")

    return all_rows


def federalcredit_to_kafka_pipeline():
    """
    Fetch Federal Credit Similar Maturity Rates from Treasury API
    and push all records to Kafka topic.
    Intended to be called as an Airflow task.
    """
    producer = KafkaProducer(
        bootstrap_servers=[KAFKA_BOOTSTRAP],
        value_serializer=lambda v: json.dumps(v).encode("utf-8"),
    )

    records = _fetch_all_records()
    fetched_at_iso = datetime.utcnow().isoformat()

    for rec in records:
        rec["fetched_at_iso"] = fetched_at_iso
        producer.send(KAFKA_TOPIC, value=rec)

    producer.flush()
    producer.close()
