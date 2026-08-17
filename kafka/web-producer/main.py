import asyncio
import websockets
import json
import logging
from datetime import datetime
import pytz
from kafka import KafkaProducer

from config import SYMBOLS, API_TOKEN, KAFKA_BROKER
from utils.slack import send_slack_alert
from utils.registry import create_tick_hash, to_avro


logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")
KST = pytz.timezone("Asia/Seoul")

# Kafka producer
producer = KafkaProducer(
    bootstrap_servers=KAFKA_BROKER,
    value_serializer=lambda v: v,
    acks="all")

def send_to_kafka(topic, data):
    producer.send(topic, value=data)

def get_kst_timestamp():
    return datetime.now(KST).isoformat(timespec='milliseconds')

async def handle_websocket_data(uri, symbols):
    max_retries = 10
    retry_count = 0
    backoff_time = 1
    all_cnt = 0

    while retry_count < max_retries:
        try:
            async with websockets.connect(uri) as websocket:
                logging.info("✅ WebSocket 연결 성공")
                retry_count = 0
                backoff_time = 1

                logging.info(f"⏱️ 구독 요청 시간: {get_kst_timestamp()}")
                for symbol in symbols:
                    ws_message = json.dumps({"type": "subscribe", "symbol": symbol})
                    await websocket.send(ws_message)
                    logging.info(f"📩 구독 요청 전송: {ws_message}")

                while True:
                    try:
                        message = await asyncio.wait_for(websocket.recv(), timeout=60)
                        logging.info(f"📨 수신 시간: {get_kst_timestamp()}")
                    except asyncio.TimeoutError:
                        err_msg = "❗ 1분 동안 WebSocket 데이터가 수신되지 않아 재연결합니다."
                        logging.warning(err_msg)
                        send_slack_alert(
                            title="❗ WebSocket 수신 타임아웃",
                            summary="1분 동안 데이터가 수신되지 않았습니다",
                            details=err_msg,
                            level="warning"
                        )
                        await websocket.close()
                        raise

                    try:
                        message_data = json.loads(message)
                        message_data["timestamp_web"] = get_kst_timestamp()
                    except json.JSONDecodeError:
                        err_msg = f"❗ JSON 디코딩 실패 | 원본 메시지: {message}"
                        logging.warning(err_msg)
                        send_slack_alert(
                            title="❗ JSON 파싱 오류",
                            summary="WebSocket 메시지를 JSON으로 변환할 수 없습니다",
                            details=err_msg,
                            level="warning"
                        )
                        continue

                    # {"type":"trade"}가 아닌 메세지는 무시(ping메세지 무시)
                    if message_data.get("type") != "trade":
                        continue
                    
                    row_count = len(message_data["data"])

                    # tick별 hash 추가
                    for tick in message_data["data"]:
                        all_cnt += 1
                        tick["Hash"] = create_tick_hash(tick)
                        tick["Seq"] = all_cnt                       

                    # Avro 직렬화
                    try:
                        avro_data = to_avro([{
                            "timestamp_web": message_data["timestamp_web"],
                            "data": message_data["data"]
                        }])
                    except Exception as e:
                        err_msg = f"❗ Avro 직렬화 실패 | 오류: {e}\nmessage_data : {message_data}"
                        logging.error(err_msg)
                        send_slack_alert(
                            title="❗ Avro 직렬화 오류",
                            summary="수신 데이터를 Avro 포맷으로 변환하지 못했습니다",
                            details=err_msg,
                            level="warning"
                        )
                        continue

                    # Kafka 전송
                    try:
                        send_to_kafka('stock_data', avro_data)
                        logging.info(f"✅ Kafka 전송 완료 | 행수 : {row_count}개, 전체 보낸 행수 : {all_cnt}")
                    except Exception as e:
                        err_msg = f"❗ Kafka 전송 실패 | 오류: {e}"
                        logging.error(err_msg)
                        send_slack_alert(
                            title="❗ Kafka 전송 오류",
                            summary="Avro 데이터를 Kafka에 전송하지 못했습니다",
                            details=err_msg,
                            level="warning"
                        )
                        continue

        except websockets.exceptions.ConnectionClosed as e:
            err_msg = f"❌ WebSocket 연결 종료: {e}. {backoff_time}초 후 재연결..."
            logging.error(err_msg)
            send_slack_alert("❌ WebSocket 연결 끊김", "WebSocket 연결 종료 후 재시도", err_msg, "danger")
        except Exception as e:
            err_msg = f"❌ WebSocket 처리 오류: {e}. {backoff_time}초 후 재연결..."
            logging.error(err_msg)
            send_slack_alert("❌ WebSocket 처리 오류", "WebSocket 처리 예외 발생", err_msg, "danger")

        retry_count += 1
        await asyncio.sleep(backoff_time)
        backoff_time = min(backoff_time * 2, 60)

    # 10회 재시도 실패 시 최종 알림
    send_slack_alert(
        title="🚨🚨🚨 최대 재연결 초과",
        summary="kafka-producer 프로세스 종료",
        details="WebSocket 10회 이상 재연결 실패",
        level="danger"
    )



websocket_uri = f"wss://ws.finnhub.io?token={API_TOKEN}"

def main():
    # 1. 앱 시작 알림
    send_slack_alert(
        title="🚀 Kafka Producer 시작",
        summary="실시간 주식 데이터 수집 애플리케이션이 시작되었습니다.",
        details=(
            f"WebSocket 연결 및 Kafka 전송을 시작합니다.\n"
            f"구독 종목 수: {len(SYMBOLS)}개\n"
            f"Kafka Broker: {KAFKA_BROKER}"
        ),
        level="info"
    )

    # 2. WebSocket 시작
    asyncio.run(
        handle_websocket_data(
            websocket_uri,
            SYMBOLS))


if __name__ == "__main__":
    main()