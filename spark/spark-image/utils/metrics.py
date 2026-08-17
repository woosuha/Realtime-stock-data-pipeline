import json
import threading
from prometheus_client import Gauge, start_http_server
from pyspark.sql.streaming import StreamingQueryListener
import time as t_module
from .slack import send_slack_alert

spark_kafka_processed_offset = Gauge(
    "spark_kafka_processed_offset",
    "Kafka end offset processed by Spark raw streaming query",
    ["query", "topic", "partition"]
)

class KafkaOffsetListener(StreamingQueryListener):
    def __init__(self):
        super().__init__()
        self.last_progress_time = t_module.time()
        self.timeout_seconds = 60
        self.alert_sent = False
        
        # 동시성 제어를 위한 락(Lock)
        self.lock = threading.Lock()

        # 백그라운드 타임아웃 체커 스레드 시작 (1분 데이터 유입 부재 감지용)
        self.checker_thread = threading.Thread(target=self._timeout_checker, daemon=True)
        self.checker_thread.start()

    def _timeout_checker(self):
        """백그라운드에서 주기적으로 데이터 유입 부재 여부를 확인하는 스레드"""
        while True:
            t_module.sleep(5) # 5초마다 체크
            with self.lock:
                if not self.alert_sent and (t_module.time() - self.last_progress_time > self.timeout_seconds):
                    err_msg = f"마지막 수신 시각 이후 {self.timeout_seconds}초 동안 'raw' 스트리밍 쿼리에 새 데이터가 유입되지 않았습니다."
                    send_slack_alert(
                        title="❗ Kafka 데이터 수신 타임아웃",
                        summary="1분 동안 데이터가 수신되지 않았습니다",
                        details=err_msg,
                        level="warning"
                    )
                    self.alert_sent = True

    def onQueryStarted(self, event):
        print(f"[Prometheus] Query started: name={event.name}, id={event.id}")

    def onQueryProgress(self, event):
        progress = event.progress

        if progress.get("name") == "raw":
            with self.lock:
                self.last_progress_time = t_module.time()
                self.alert_sent = False

        sources = progress.get("sources", [])
        for source in sources:
            description = source.get("description", "")
            if "KafkaV2" not in description:
                continue

            end_offset = source.get("endOffset")
            if not end_offset:
                continue

            if isinstance(end_offset, str):
                end_offset = json.loads(end_offset)

            for topic, partitions in end_offset.items():
                for partition, offset in partitions.items():
                    spark_kafka_processed_offset.labels(
                        query="raw",
                        topic=topic,
                        partition=str(partition)
                    ).set(float(offset))

                    print(f"[Prometheus] query=raw topic={topic} partition={partition} offset={offset}")

    def onQueryTerminated(self, event):
            pass

def init_prometheus():
    start_http_server(8000)