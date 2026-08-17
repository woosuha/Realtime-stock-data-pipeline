from kafka import KafkaConsumer, TopicPartition
import avro.schema
import avro.io
from io import BytesIO
import requests
import json


# Apicurio Schema Registry 설정

SCHEMA_REGISTRY_URL = (
    "http://apicurio-registry.kafka.svc:8080/apis/ccompat/v7"
)

SCHEMA_SUBJECT = "web-producer"
SCHEMA_VERSION = "latest"


KAFKA_BOOTSTRAP_SERVERS = (
    "my-cluster-kafka-bootstrap.kafka.svc:9092"
)
KAFKA_TOPIC = "stock_data"


# 파티션/오프셋 번호
TARGET_PARTITION = 1
TARGET_OFFSET = 46170

# Apicurio에서 Avro Schema 읽기
def read_avro_schema_from_registry():

    url = (
        f"{SCHEMA_REGISTRY_URL}"
        f"/subjects/{SCHEMA_SUBJECT}"
        f"/versions/{SCHEMA_VERSION}"
    )

    response = requests.get(url)

    if response.status_code != 200:
        raise Exception(
            f"Schema Registry 조회 실패: "
            f"{response.status_code}, "
            f"{response.text}"
        )

    schema_str = response.json()["schema"]

    return avro.schema.parse(schema_str)

# Kafka 메시지 Avro Decode
def decode_avro_message(message_value, schema):
    bytes_reader = BytesIO(message_value)
    decoder = avro.io.BinaryDecoder(bytes_reader)
    reader = avro.io.DatumReader(schema)
    data = reader.read(decoder)
    return data

def main():
    print("=" * 80)
    print("Kafka Offset Debug Consumer")
    print("=" * 80)
    print(f"Topic     : {KAFKA_TOPIC}")
    print(f"Partition : {TARGET_PARTITION}")
    print(f"Offset    : {TARGET_OFFSET}")
    print("=" * 80)

    schema = read_avro_schema_from_registry()

    # 2. Kafka Consumer 생성
    # 디버깅용이므로 자동 커밋 사용 안함
    consumer = KafkaConsumer(
        bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
        enable_auto_commit=False,
        auto_offset_reset="earliest"
    )


    try:
        topic_partition = TopicPartition(
            KAFKA_TOPIC,
            TARGET_PARTITION
        )
        consumer.assign([topic_partition])
        consumer.seek(
            topic_partition,
            TARGET_OFFSET
        )
        message = next(consumer)

        print("=" * 80)
        print("Kafka Message")
        print("=" * 80)
        print(f"topic     : {message.topic}")
        print(f"partition : {message.partition}")
        print(f"offset    : {message.offset}")
        print(f"timestamp : {message.timestamp}")
        print(f"key       : {message.key}")
        print(f"value size: {len(message.value)} bytes")

    # 디코더
        data = decode_avro_message(
            message.value,
            schema
        )



        print("=" * 80)
        print("Avro Data")
        print("=" * 80)

        print(
            json.dumps(
                data,
                indent=2,
                ensure_ascii=False
            )
        )

        print("=" * 80)
        print("Tick Data")
        print("=" * 80)

        if "data" in data:

            print(
                f"Tick 개수 : {len(data['data'])}"
            )

            for index, tick in enumerate(data["data"]):

                print(
                    f"[{index}] "
                    f"Symbol={tick.get('s')} "
                    f"Price={tick.get('p')} "
                    f"Volume={tick.get('v')} "
                    f"Seq={tick.get('seq')} "
                    f"Hash={tick.get('hash')}"
                )

        else:

            print(
                "Avro 데이터에 'data' 필드가 없습니다."
            )


    except StopIteration:

        print("=" * 80)
        print("해당 Offset의 메시지를 찾지 못했습니다.")
        print("=" * 80)


    except Exception as e:

        print("=" * 80)
        print("오류 발생")
        print("=" * 80)

        print(type(e).__name__)
        print(str(e))

        raise


    finally:

        consumer.close()

        print("=" * 80)
        print("Consumer 종료")
        print("=" * 80)


if __name__ == "__main__":
    main()