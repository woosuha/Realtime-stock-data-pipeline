import requests
import avro.schema
import avro.io
from io import BytesIO
import hashlib
import json
from config import SCHEMA_REGISTRY_URL, SCHEMA_SUBJECT, SCHEMA_VERSION

def read_avro_schema_from_registry():
    url = f"{SCHEMA_REGISTRY_URL}/subjects/{SCHEMA_SUBJECT}/versions/{SCHEMA_VERSION}"
    response = requests.get(url)

    if response.status_code != 200:
        raise Exception(f"Schema Registry 조회 실패: {response.status_code}, {response.text}")

    schema_str = response.json()["schema"]
    return avro.schema.parse(schema_str)

SCHEMA = read_avro_schema_from_registry()

def to_avro(data):
    bytes_writer = BytesIO()
    writer = avro.io.DatumWriter(SCHEMA)
    encoder = avro.io.BinaryEncoder(bytes_writer)
    writer.write(data, encoder)
    return bytes_writer.getvalue()

def create_tick_hash(tick):
    hash_source = (
        f"{tick.get('s')}|"
        f"{tick.get('t')}|"
        f"{tick.get('p'):.10f}|"
        f"{tick.get('v'):.10f}|"
        f"{json.dumps(tick.get('c'), separators=(',', ':'))}"
    )
    return hashlib.sha256(hash_source.encode("utf-8")).hexdigest()