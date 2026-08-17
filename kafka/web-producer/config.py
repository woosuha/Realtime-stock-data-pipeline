import os
import requests
from datetime import datetime, timedelta

# 환경 변수 및 인프라 설정
API_TOKEN = os.environ.get("API_TOKEN")
SLACK_WEBHOOK_URL = os.environ.get("SLACK_WEBHOOK_URL")
KAFKA_BROKER = "my-cluster-kafka-bootstrap.kafka.svc:9092"
MASSIVE_TOKEN = os.environ.get("MASSIVE_TOKEN")

# Apicurio Schema Registry 설정
SCHEMA_REGISTRY_URL = "http://apicurio-registry.kafka.svc:8080/apis/ccompat/v7"
SCHEMA_SUBJECT = "web-producer"
SCHEMA_VERSION = "latest"

# 전일 기준 거래 상위50 종목
date = datetime.now() - timedelta(days=1)
date = date.strftime("%Y-%m-%d")

url = f"https://api.polygon.io/v2/aggs/grouped/locale/us/market/stocks/{date}"

data = requests.get(
    url,
    params={"apiKey": MASSIVE_TOKEN}
).json()

top50 = sorted(
    data["results"],
    key=lambda x: x["v"],
    reverse=True
)[:50]

SYMBOLS = [x["T"] for x in top50]